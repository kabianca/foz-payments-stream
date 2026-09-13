"""One micro-batch, from parsed events to four Delta tables.

``process_batch`` is a plain function of (DataFrame, batch_id, what is already on
disk). It knows nothing about Kafka or streaming, which is what makes every
decision below testable without a broker. Structured Streaming hands it the
same batch again after a crash; every write is a MERGE by key, so the second
time changes nothing.

The watermark is owned here rather than delegated to ``withWatermark``. Spark's
built-in watermark drops a late row and increments a counter; a payments ledger
needs the row. The rule is the same one Spark applies to windowed aggregations:

    watermark(N)   = max(event_time seen through batch N) - allowed_lateness
    late in N+1    = window_end <= watermark(N)

A window closes when the watermark passes its end. A record whose window is
closed goes to ``late_events`` with the watermark it lost to; a record whose
window is still open goes in, even if its event_time is older than the
watermark itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from foz.config import Settings
from foz.schema import AMOUNT, EVENTS, STREAM_STATE
from foz.tables import Tables

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MICRO = timedelta(microseconds=1)


class MalformedBatchError(RuntimeError):
    """Rows without a transaction_id or event_time. Refused loudly, never dropped."""


@dataclass(frozen=True)
class BatchReport:
    batch_id: int
    watermark_applied: datetime | None
    max_event_time: datetime | None
    watermark: datetime | None
    rows_in: int
    rows_on_time: int
    rows_late: int
    rows_duplicate: int


# --- timestamps cross the Python/JVM border as epoch microseconds, never as ---
# --- naive datetimes: PySpark converts those through the local timezone.    ---


def _dt(micros: int | None) -> datetime | None:
    return None if micros is None else _EPOCH + micros * _MICRO


def _micros(moment: datetime) -> int:
    return (moment - _EPOCH) // _MICRO


def _ts(moment: datetime) -> Column:
    return F.timestamp_micros(F.lit(_micros(moment)))


def process_batch(
    spark: SparkSession,
    batch_df: DataFrame,
    batch_id: int,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> BatchReport:
    """Route one batch into events / late_events, refresh the touched windows,
    advance the watermark and record the batch in stream_state."""
    tables = Tables(spark, settings)
    processing_time = now or datetime.now(timezone.utc)

    previous = _previous_state(tables, batch_id)
    watermark_applied = _dt(previous["watermark"]) if previous else None
    previous_max = _dt(previous["max_event_time"]) if previous else None

    _refuse_malformed(batch_df, batch_id)

    enriched = _enrich(batch_df, batch_id, processing_time, settings).persist()
    try:
        rows_in = batch_df.count()

        if watermark_applied is None:
            late, on_time = enriched.limit(0), enriched
        else:
            is_late = F.col("window_end") <= _ts(watermark_applied)
            late, on_time = enriched.filter(is_late), enriched.filter(~is_late)

        # Every Delta operation is a commit; skip the ones with nothing to say.
        any_late = watermark_applied is not None and not late.isEmpty()
        any_on_time = not on_time.isEmpty()
        if any_late:
            _quarantine(tables, late, watermark_applied)
        if any_on_time:
            _merge_insert_only(tables.delta("events"), on_time, "transaction_id")
            _refresh_windows(tables, on_time, batch_id, processing_time)

        # Counts come from rows stamped with this batch_id, not from MERGE
        # metrics: on a replay the MERGE inserts nothing, but the rows the first
        # attempt inserted still carry this batch_id, so the ledger stays true.
        rows_on_time = _stamped(tables, "events", batch_id) if any_on_time else 0
        rows_late = _stamped(tables, "late_events", batch_id) if any_late else 0

        batch_max = _dt(enriched.agg(F.max(F.unix_micros("event_time"))).first()[0])
        max_event_time = max(filter(None, (previous_max, batch_max)), default=None)
        watermark = (
            max_event_time - settings.allowed_lateness if max_event_time else None
        )
        if watermark is not None:
            _close_windows(tables, watermark, batch_id, processing_time)
    finally:
        enriched.unpersist()

    report = BatchReport(
        batch_id=batch_id,
        watermark_applied=watermark_applied,
        max_event_time=max_event_time,
        watermark=watermark,
        rows_in=rows_in,
        rows_on_time=rows_on_time,
        rows_late=rows_late,
        rows_duplicate=rows_in - rows_on_time - rows_late,
    )
    _record(tables, report, processing_time)
    return report


def _previous_state(tables: Tables, batch_id: int):
    """The newest stream_state row before this batch. On a replay, the row for
    this very batch may already exist; it is ignored so the replay is judged
    against exactly the same threshold as the first attempt."""
    rows = (
        tables.df("stream_state")
        .filter(F.col("batch_id") < batch_id)
        .orderBy(F.col("batch_id").desc())
        .select(
            F.unix_micros("watermark").alias("watermark"),
            F.unix_micros("max_event_time").alias("max_event_time"),
        )
        .limit(1)
        .collect()
    )
    return rows[0] if rows else None


def _refuse_malformed(batch_df: DataFrame, batch_id: int) -> None:
    bad = batch_df.filter(
        F.col("transaction_id").isNull() | F.col("event_time").isNull()
    ).count()
    if bad:
        raise MalformedBatchError(
            f"batch {batch_id}: {bad} row(s) without transaction_id or event_time; "
            "refusing to drop them silently"
        )


def _enrich(
    batch_df: DataFrame, batch_id: int, processing_time: datetime, settings: Settings
) -> DataFrame:
    """Deduplicate within the batch (first arrival wins), assign the window,
    stamp the processing time and select the exact EVENTS column order."""
    first_arrival = Window.partitionBy("transaction_id").orderBy(
        "kafka_timestamp", "kafka_partition", "kafka_offset"
    )
    window = F.window("event_time", f"{settings.window_seconds} seconds")
    return (
        batch_df.withColumn("_arrival", F.row_number().over(first_arrival))
        .filter(F.col("_arrival") == 1)
        .withColumn("_window", window)
        .withColumn("window_start", F.col("_window.start"))
        .withColumn("window_end", F.col("_window.end"))
        .withColumn("amount", F.col("amount").cast(AMOUNT))
        .withColumn("processing_time", _ts(processing_time))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .select(*EVENTS.fieldNames())
    )


def _stamped(tables: Tables, name: str, batch_id: int) -> int:
    return tables.df(name).filter(F.col("batch_id") == batch_id).count()


def _quarantine(
    tables: Tables, late: DataFrame, watermark_applied: datetime | None
) -> None:
    """Late rows go to late_events with the evidence attached. A late copy of a
    transaction already in events is a duplicate, not a late event, and is
    absorbed like any other duplicate."""
    if watermark_applied is None:
        return
    known = tables.df("events").select("transaction_id")
    evidence = (
        late.join(known, "transaction_id", "left_anti")
        .withColumn("watermark_at_arrival", _ts(watermark_applied))
        .withColumn(
            "lateness_seconds",
            (F.lit(_micros(watermark_applied)) - F.unix_micros("event_time"))
            / F.lit(1_000_000.0),
        )
    )
    _merge_insert_only(tables.delta("late_events"), evidence, "transaction_id")


def _merge_insert_only(target: DeltaTable, source: DataFrame, key: str) -> None:
    """Insert rows whose key is unseen; leave existing rows untouched, so the
    first arrival wins and a replay is a no-op."""
    (
        target.alias("t")
        .merge(source.alias("s"), f"t.{key} = s.{key}")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _refresh_windows(
    tables: Tables, on_time: DataFrame, batch_id: int, processing_time: datetime
) -> None:
    """Recompute every (window, merchant) this batch touched from the events
    table and upsert the totals. Totals are derived, never accumulated, so a
    replayed batch rewrites the same numbers instead of adding them twice."""
    touched = on_time.select("window_start", "window_end", "merchant_id").distinct()
    totals = (
        tables.df("events")
        .join(F.broadcast(touched), ["window_start", "window_end", "merchant_id"])
        .groupBy("window_start", "window_end", "merchant_id")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").cast(AMOUNT).alias("total_amount"),
        )
        .withColumn("is_final", F.lit(False))
        .withColumn("updated_batch_id", F.lit(batch_id).cast("long"))
        .withColumn("updated_at", _ts(processing_time))
    )
    (
        tables.delta("merchant_windows")
        .alias("t")
        .merge(
            totals.alias("s"),
            "t.window_start = s.window_start AND t.merchant_id = s.merchant_id",
        )
        .whenMatchedUpdate(
            set={
                "tx_count": "s.tx_count",
                "total_amount": "s.total_amount",
                "updated_batch_id": "s.updated_batch_id",
                "updated_at": "s.updated_at",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def _close_windows(
    tables: Tables, watermark: datetime, batch_id: int, processing_time: datetime
) -> None:
    """Every open window the watermark has passed becomes final. Idempotent:
    the predicate excludes rows that are already final, and nothing is written
    when no window is due."""
    due = (~F.col("is_final")) & (F.col("window_end") <= _ts(watermark))
    if tables.df("merchant_windows").filter(due).isEmpty():
        return
    tables.delta("merchant_windows").update(
        condition=due,
        set={
            "is_final": F.lit(True),
            "updated_batch_id": F.lit(batch_id).cast("long"),
            "updated_at": _ts(processing_time),
        },
    )


def _record(tables: Tables, report: BatchReport, processing_time: datetime) -> None:
    """Upsert the batch ledger row. Timestamps travel as microseconds and are
    converted on the JVM side so no local timezone can leak in."""

    def micros(moment: datetime | None) -> int | None:
        return None if moment is None else _micros(moment)

    raw = tables.spark.createDataFrame(
        [
            (
                report.batch_id,
                micros(report.watermark_applied),
                micros(report.max_event_time),
                micros(report.watermark),
                report.rows_in,
                report.rows_on_time,
                report.rows_late,
                report.rows_duplicate,
                micros(processing_time),
            )
        ],
        "batch_id long, watermark_applied long, max_event_time long, watermark long, "
        "rows_in long, rows_on_time long, rows_late long, rows_duplicate long, "
        "processing_time long",
    )
    row = raw.select(
        *[
            F.timestamp_micros(F.col(field.name)).alias(field.name)
            if field.dataType.typeName() == "timestamp"
            else F.col(field.name)
            for field in STREAM_STATE.fields
        ]
    )
    (
        tables.delta("stream_state")
        .alias("t")
        .merge(row.alias("s"), "t.batch_id = s.batch_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
