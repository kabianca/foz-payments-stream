"""Invariants over the four Delta tables. This is the proof, not the dashboard.

Every check is a property that must hold after any sequence of batches,
restarts and replays. Run ``python -m foz.check`` against a live ``data/``
directory; exit status is non-zero if any invariant is violated.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from foz.config import Settings
from foz.spark import build_session
from foz.tables import Tables


@dataclass(frozen=True)
class Result:
    name: str
    ok: bool
    detail: str


def _count_pairs(events: DataFrame) -> DataFrame:
    return events.groupBy("window_start", "merchant_id").agg(
        F.count("*").alias("n"), F.sum("amount").alias("total")
    )


def check_ledger_matches_tables(t: Tables) -> Result:
    ledger = (
        t.df("stream_state")
        .agg(
            F.coalesce(F.sum("rows_in"), F.lit(0)).alias("rows_in"),
            F.coalesce(F.sum("rows_on_time"), F.lit(0)).alias("on_time"),
            F.coalesce(F.sum("rows_late"), F.lit(0)).alias("late"),
            F.coalesce(F.sum("rows_duplicate"), F.lit(0)).alias("dup"),
        )
        .first()
    )
    events, late = t.df("events").count(), t.df("late_events").count()
    ok = ledger["on_time"] == events and ledger["late"] == late
    ok = ok and ledger["rows_in"] == ledger["on_time"] + ledger["late"] + ledger["dup"]
    return Result(
        "ledger matches tables",
        ok,
        f"rows_in={ledger['rows_in']} = events {events} + late {late} + duplicates {ledger['dup']}",
    )


def check_no_duplicates(t: Tables) -> Result:
    events, late = t.df("events"), t.df("late_events")
    dup_events = events.count() - events.select("transaction_id").distinct().count()
    dup_late = late.count() - late.select("transaction_id").distinct().count()
    both = (
        events.select("transaction_id")
        .join(late.select("transaction_id"), "transaction_id")
        .count()
    )
    ok = dup_events == 0 and dup_late == 0 and both == 0
    return Result(
        "one row per transaction",
        ok,
        f"duplicates in events={dup_events}, in late_events={dup_late}, in both={both}",
    )


def check_aggregates_reconcile(t: Tables) -> Result:
    expected = _count_pairs(t.df("events"))
    actual = t.df("merchant_windows").select(
        "window_start", "merchant_id", "tx_count", "total_amount"
    )
    joined = expected.join(actual, ["window_start", "merchant_id"], "full")
    mismatched = joined.filter(
        F.col("n").isNull()
        | F.col("tx_count").isNull()
        | (F.col("n") != F.col("tx_count"))
        | (F.col("total") != F.col("total_amount"))
    ).count()
    return Result(
        "windows equal a fresh recount of events",
        mismatched == 0,
        f"{actual.count()} windows, {mismatched} disagree with events",
    )


def check_late_has_evidence(t: Tables) -> Result:
    late = t.df("late_events")
    bad = late.filter(
        (F.col("window_end") > F.col("watermark_at_arrival"))
        | (F.col("lateness_seconds") <= 0)
    ).count()
    return Result(
        "every late row arrived after its window closed",
        bad == 0,
        f"{late.count()} late rows, {bad} without a closed window at arrival",
    )


def check_watermark_monotonic(t: Tables) -> Result:
    rows = (
        t.df("stream_state")
        .orderBy("batch_id")
        .select("batch_id", "watermark_applied", "watermark")
        .collect()
    )
    previous = None
    breaks = 0
    for row in rows:
        if previous is not None and row["watermark_applied"] != previous["watermark"]:
            breaks += 1
        if (
            previous is not None
            and previous["watermark"] is not None
            and row["watermark"] is not None
            and row["watermark"] < previous["watermark"]
        ):
            breaks += 1
        previous = row
    return Result(
        "watermark never moves backwards",
        breaks == 0,
        f"{len(rows)} batches, {breaks} break(s) in the chain",
    )


def check_final_windows_frozen(t: Tables) -> Result:
    windows = t.df("merchant_windows")
    last = t.df("stream_state").agg(F.max("watermark")).first()[0]
    final = windows.filter("is_final")
    touched_after = (
        t.df("events")
        .join(
            final.select("window_start", "merchant_id", "updated_batch_id"),
            ["window_start", "merchant_id"],
        )
        .filter(F.col("batch_id") > F.col("updated_batch_id"))
        .count()
    )
    still_open = 0
    if last is not None:
        still_open = windows.filter(
            ~F.col("is_final") & (F.col("window_end") <= F.lit(last))
        ).count()
    ok = touched_after == 0 and still_open == 0
    return Result(
        "final windows never changed again",
        ok,
        f"{final.count()} final, {touched_after} received events after closing, {still_open} should be closed",
    )


def check_two_clocks(t: Tables) -> Result:
    events = t.df("events")
    total = events.count()
    same = events.filter(F.col("processing_time") == F.col("event_time")).count()
    before_broker = events.filter(
        F.col("processing_time") < F.col("kafka_timestamp")
    ).count()
    ok = same == 0 and before_broker == 0
    return Result(
        "event time and processing time are different clocks",
        ok,
        f"{total} rows, {same} with equal clocks, {before_broker} processed before the broker saw them",
    )


CHECKS: list[Callable[[Tables], Result]] = [
    check_ledger_matches_tables,
    check_no_duplicates,
    check_aggregates_reconcile,
    check_late_has_evidence,
    check_watermark_monotonic,
    check_final_windows_frozen,
    check_two_clocks,
]


def run_checks(t: Tables) -> list[Result]:
    return [check(t) for check in CHECKS]


def status(t: Tables) -> None:
    print("tables:")
    for name in ("events", "late_events", "merchant_windows", "stream_state"):
        print(f"  {name:17s} {t.df(name).count():>8d} rows")
    print("\nlast batches:")
    (
        t.df("stream_state")
        .orderBy(F.col("batch_id").desc())
        .limit(10)
        .orderBy("batch_id")
        .select(
            "batch_id",
            "rows_in",
            "rows_on_time",
            "rows_late",
            "rows_duplicate",
            F.date_format("watermark", "HH:mm:ss").alias("watermark"),
            F.date_format("processing_time", "HH:mm:ss").alias("processed_at"),
        )
        .show(truncate=False)
    )


def ingested(t: Tables) -> int:
    return int(
        t.df("stream_state").agg(F.coalesce(F.sum("rows_in"), F.lit(0))).first()[0]
    )


def wait_for_rows(t: Tables, at_least: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        seen = ingested(t)
        if seen >= at_least:
            return
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out: {seen} rows ingested, waiting for {at_least}")
        time.sleep(2)


def session() -> SparkSession:
    return build_session(
        "foz-check",
        master="local[2]",
        with_pip_jars=os.environ.get("FOZ_PIP_JARS", "1") == "1",
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--status", action="store_true", help="print table sizes and the last batches"
    )
    parser.add_argument(
        "--ingested",
        action="store_true",
        help="print only the number of rows ingested so far",
    )
    parser.add_argument(
        "--wait-for-rows",
        type=int,
        default=None,
        help="block until this many rows were ingested",
    )
    parser.add_argument("--timeout", type=float, default=180.0)
    args = parser.parse_args(argv)

    spark = session()
    spark.sparkContext.setLogLevel("ERROR")
    tables = Tables(spark, Settings.from_env())
    tables.ensure()

    if args.ingested:
        print(ingested(tables))
        return
    if args.wait_for_rows is not None:
        wait_for_rows(tables, args.wait_for_rows, args.timeout)
    if args.status:
        status(tables)
        return

    results = run_checks(tables)
    for result in results:
        mark = "✔" if result.ok else "✘"
        print(f"{mark} {result.name:52s} {result.detail}")
    failed = [r for r in results if not r.ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} invariants hold")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
