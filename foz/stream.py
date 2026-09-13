"""The streaming job: Kafka -> parse -> foreachBatch(process_batch).

Only this module knows about Kafka. ``parse_events`` and ``start_query`` take a
DataFrame with the Kafka source's columns (value, partition, offset, timestamp)
so the tests can drive the very same query from a file source and exercise the
checkpoint without a broker.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from foz.batch import process_batch
from foz.config import Settings
from foz.schema import AMOUNT, EVENT_SCHEMA
from foz.spark import build_session
from foz.tables import Tables

log = logging.getLogger("foz.stream")


def kafka_source(spark: SparkSession, settings: Settings) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        # Losing offsets (topic recreated, retention) is an incident, not a
        # condition to skip past. The job stops and says so.
        .option("failOnDataLoss", "true")
        .load()
    )


def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka record -> one row per event with the wire schema plus provenance.

    ``event_time`` comes from the payload and is the only clock used for
    windowing. ``kafka_timestamp`` is when the broker appended the record.
    ``processing_time`` is stamped later, per batch, by ``process_batch``.
    """
    event = F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event")
    return raw.select(
        event,
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select(
        "event.transaction_id",
        "event.merchant_id",
        "event.account_id",
        F.col("event.amount").cast(AMOUNT).alias("amount"),
        "event.currency",
        "event.method",
        "event.event_time",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )


def start_query(
    spark: SparkSession,
    events: DataFrame,
    settings: Settings,
    *,
    available_now: bool = False,
) -> StreamingQuery:
    """Attach process_batch to a parsed streaming DataFrame and start it.

    The checkpoint holds the Kafka offsets of every batch before it runs, so a
    restart replays exactly the batch that was interrupted - and process_batch
    is built so that replaying it is harmless.
    """

    def sink(batch_df: DataFrame, batch_id: int) -> None:
        report = process_batch(spark, batch_df, batch_id, settings)
        log.info(
            "batch %d: in=%d on_time=%d late=%d duplicate=%d watermark=%s",
            report.batch_id,
            report.rows_in,
            report.rows_on_time,
            report.rows_late,
            report.rows_duplicate,
            report.watermark.isoformat() if report.watermark else None,
        )

    writer = (
        events.writeStream.queryName("foz")
        .foreachBatch(sink)
        .option("checkpointLocation", settings.checkpoint_path)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{settings.trigger_seconds} seconds")
    return writer.start()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    settings = Settings.from_env()
    spark = build_session("foz-stream")
    spark.sparkContext.setLogLevel("WARN")
    Tables(spark, settings).ensure()
    log.info(
        "consuming %s from %s; window=%ss lateness=%ss checkpoint=%s",
        settings.topic,
        settings.bootstrap_servers,
        settings.window_seconds,
        settings.allowed_lateness_seconds,
        settings.checkpoint_path,
    )
    query = start_query(spark, parse_events(kafka_source(spark, settings)), settings)
    query.awaitTermination()


if __name__ == "__main__":
    main()
