"""The streaming query itself: parsing the wire format and resuming from the
checkpoint. Driven from a file source with the Kafka source's columns, so the
same start_query runs here without a broker."""

import json
from datetime import timedelta
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from foz.producer import Event
from foz.stream import parse_events, start_query
from tests.conftest import at, naive, rows

KAFKA_LIKE = StructType(
    [
        StructField("value", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def wire(
    transaction_id: str, moment, amount: str = "12.34", merchant_id: str = "M1"
) -> Event:
    return Event(
        transaction_id=transaction_id,
        merchant_id=merchant_id,
        account_id="A00001",
        amount=amount,
        currency="BRL",
        method="pix",
        event_time=moment,
    )


def test_parse_events_reads_the_wire_format(spark):
    raw = spark.createDataFrame(
        [
            (wire("t1", at(30)).to_json(), 2, 17, at(31)),
            # a producer that writes Zulu instead of +00:00 must parse the same
            (wire("t2", at(45)).to_json().replace("+00:00", "Z"), 0, 3, at(46)),
        ],
        KAFKA_LIKE,
    ).withColumn("value", F.col("value").cast("binary"))

    parsed = rows(parse_events(raw), "transaction_id")

    assert [p["transaction_id"] for p in parsed] == ["t1", "t2"]
    assert parsed[0]["event_time"] == naive(at(30))
    assert parsed[1]["event_time"] == naive(at(45))
    assert str(parsed[0]["amount"]) == "12.34"
    assert parsed[0]["kafka_partition"] == 2 and parsed[0]["kafka_offset"] == 17
    assert parsed[0]["kafka_timestamp"] == naive(at(31))
    assert dict(parsed[0].items()).keys() >= {
        "merchant_id",
        "account_id",
        "currency",
        "method",
    }


def _write(source: Path, name: str, events: list[Event], first_offset: int) -> None:
    lines = [
        json.dumps(
            {
                "value": e.to_json(),
                "partition": 0,
                "offset": first_offset + i,
                "timestamp": (e.event_time + timedelta(seconds=2)).isoformat(),
            }
        )
        for i, e in enumerate(events)
    ]
    (source / name).write_text("\n".join(lines) + "\n")


def _run_until_drained(spark, settings, source: Path) -> None:
    stream = (
        spark.readStream.format("json")
        .schema(KAFKA_LIKE)
        .option("maxFilesPerTrigger", 1)
        .load(str(source))
    )
    query = start_query(spark, parse_events(stream), settings, available_now=True)
    query.awaitTermination()


def test_restart_resumes_from_the_checkpoint_without_reprocessing_or_skipping(
    spark, settings, tables, tmp_path
):
    source = tmp_path / "source"
    source.mkdir()
    _write(
        source, "f1.json", [wire(f"t{i}", at(10 + i)) for i in range(3)], first_offset=0
    )
    _write(
        source,
        "f2.json",
        [wire(f"t{i}", at(20 + i)) for i in range(3, 6)],
        first_offset=3,
    )

    _run_until_drained(spark, settings, source)

    state = rows(tables.df("stream_state"), "batch_id")
    assert [s["batch_id"] for s in state] == [0, 1]
    assert sum(s["rows_in"] for s in state) == 6
    assert tables.df("events").count() == 6

    # The job "restarts": new file, same checkpoint. Only the new file runs.
    _write(source, "f3.json", [wire("t6", at(30)), wire("t0", at(10))], first_offset=6)
    _run_until_drained(spark, settings, source)

    state = rows(tables.df("stream_state"), "batch_id")
    assert [s["batch_id"] for s in state] == [0, 1, 2]
    assert (
        state[2]["rows_in"] == 2
        and state[2]["rows_on_time"] == 1
        and state[2]["rows_duplicate"] == 1
    )
    assert sum(s["rows_in"] for s in state) == 8
    assert tables.df("events").count() == 7


def test_restart_with_nothing_new_writes_no_batch(spark, settings, tables, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    _write(source, "f1.json", [wire("t1", at(10))], first_offset=0)
    _run_until_drained(spark, settings, source)
    _run_until_drained(spark, settings, source)

    assert [s["batch_id"] for s in rows(tables.df("stream_state"), "batch_id")] == [0]
    assert tables.df("events").count() == 1
