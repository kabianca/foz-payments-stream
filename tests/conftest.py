"""Shared fixtures: one local Spark session per test run, a fresh set of Delta
tables per test, and helpers to build batches with explicit clocks.

No Kafka, no Docker. ``process_batch`` is a function of a DataFrame and what is
on disk, so every decision is exercised with hand-built batches whose event
times are chosen to the second.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from foz.config import Settings
from foz.tables import Tables

# PySpark converts naive timestamps through the process timezone. Pin it so a
# laptop in Sao Paulo and a runner in UTC read the same clocks.
os.environ["TZ"] = "UTC"
time.tzset()

T0 = datetime(2026, 9, 13, 12, 0, 0, tzinfo=timezone.utc)

# What parse_events hands to process_batch: the wire fields plus provenance.
INPUT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("account_id", StringType()),
        StructField("amount", StringType()),
        StructField("currency", StringType()),
        StructField("method", StringType()),
        StructField("event_time", TimestampType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
    ]
)


def at(seconds: float) -> datetime:
    """A moment ``seconds`` after T0, timezone-aware."""
    return T0 + timedelta(seconds=seconds)


def naive(moment: datetime) -> datetime:
    """Spark hands timestamps back as naive datetimes in the session timezone."""
    return moment.astimezone(timezone.utc).replace(tzinfo=None)


def event(
    transaction_id: str,
    merchant_id: str,
    amount: str,
    event_time: datetime,
    *,
    offset: int | None = None,
    partition: int = 0,
    kafka_timestamp: datetime | None = None,
    account_id: str = "A00001",
) -> dict:
    return {
        "transaction_id": transaction_id,
        "merchant_id": merchant_id,
        "account_id": account_id,
        "amount": amount,
        "currency": "BRL",
        "method": "pix",
        "event_time": event_time,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "kafka_timestamp": kafka_timestamp or event_time + timedelta(seconds=1),
    }


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> SparkSession:
    from foz.spark import build_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = build_session(
        "foz-tests",
        master="local[2]",
        shuffle_partitions=2,
        delta_snapshot_partitions=2,
        with_pip_jars=True,
        extra_conf={
            "spark.ui.enabled": "false",
            "spark.ui.showConsoleProgress": "false",
            "spark.sql.warehouse.dir": str(warehouse),
            "spark.driver.host": "127.0.0.1",
        },
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        delta_root=str(tmp_path / "delta"),
        checkpoint_root=str(tmp_path / "checkpoints"),
        window_seconds=60,
        allowed_lateness_seconds=120,
    )


@pytest.fixture
def tables(spark, settings) -> Tables:
    t = Tables(spark, settings)
    t.ensure()
    return t


@pytest.fixture
def make_batch(spark):
    def _make(events: list[dict]) -> DataFrame:
        rows = []
        for index, e in enumerate(events):
            row = dict(e)
            if row.get("kafka_offset") is None:
                row["kafka_offset"] = index
            rows.append(row)
        return spark.createDataFrame(
            [tuple(r[f.name] for f in INPUT_SCHEMA.fields) for r in rows], INPUT_SCHEMA
        )

    return _make


def rows(df: DataFrame, *order_by: str) -> list[dict]:
    ordered = df.orderBy(*order_by) if order_by else df
    return [r.asDict() for r in ordered.collect()]
