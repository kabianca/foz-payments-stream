"""Schemas: the event as it travels on the wire, and the four Delta tables.

Two timestamps are deliberately distinct columns everywhere:

* ``event_time``      - when the transaction happened, set by the producer. This is
                        the only clock that decides which window a record belongs to.
* ``processing_time`` - when this job saw the record. Never used for windowing;
                        persisted so that lateness can be audited after the fact.

``kafka_timestamp`` (when the broker appended the record) sits between the two.
"""

from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# JSON payload produced to Kafka. ``amount`` is a decimal string on the wire:
# money never goes through a float.
EVENT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("merchant_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("amount", StringType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("method", StringType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
    ]
)

AMOUNT = DecimalType(18, 2)

_EVENT_COLUMNS = [
    StructField("transaction_id", StringType(), nullable=False),
    StructField("merchant_id", StringType(), nullable=False),
    StructField("account_id", StringType()),
    StructField("amount", AMOUNT, nullable=False),
    StructField("currency", StringType()),
    StructField("method", StringType()),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end", TimestampType(), nullable=False),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", TimestampType()),
    StructField("processing_time", TimestampType(), nullable=False),
    StructField("batch_id", LongType(), nullable=False),
]

# Every record that arrived while its window was still open. One row per
# transaction_id, first arrival wins.
EVENTS = StructType(_EVENT_COLUMNS)

# Every record that arrived after its window had closed. Same columns plus the
# evidence: what the watermark was when it arrived and how far behind it was.
LATE_EVENTS = StructType(
    _EVENT_COLUMNS
    + [
        StructField("watermark_at_arrival", TimestampType(), nullable=False),
        StructField("lateness_seconds", DoubleType(), nullable=False),
    ]
)

# Per (window, merchant) totals, recomputed from EVENTS whenever a batch touches
# the window. ``is_final`` flips once the watermark passes ``window_end`` and the
# row never changes again.
MERCHANT_WINDOWS = StructType(
    [
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("merchant_id", StringType(), nullable=False),
        StructField("tx_count", LongType(), nullable=False),
        StructField("total_amount", AMOUNT, nullable=False),
        StructField("is_final", BooleanType(), nullable=False),
        StructField("updated_batch_id", LongType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

# One row per micro-batch: the ledger of what the job decided and why.
# ``watermark_applied`` is the threshold this batch was judged against (the
# watermark produced by the previous batch); ``watermark`` is the one it
# produced for the next.
STREAM_STATE = StructType(
    [
        StructField("batch_id", LongType(), nullable=False),
        StructField("watermark_applied", TimestampType()),
        StructField("max_event_time", TimestampType()),
        StructField("watermark", TimestampType()),
        StructField("rows_in", LongType(), nullable=False),
        StructField("rows_on_time", LongType(), nullable=False),
        StructField("rows_late", LongType(), nullable=False),
        StructField("rows_duplicate", LongType(), nullable=False),
        StructField("processing_time", TimestampType(), nullable=False),
    ]
)

TABLES = {
    "events": EVENTS,
    "late_events": LATE_EVENTS,
    "merchant_windows": MERCHANT_WINDOWS,
    "stream_state": STREAM_STATE,
}
