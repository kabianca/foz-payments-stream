"""The watermark: what enters the window, what is quarantined, and the proof
that nothing is dropped in silence."""

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from foz.batch import MalformedBatchError, process_batch
from tests.conftest import at, event, naive, rows


def test_first_batch_has_no_watermark_so_nothing_is_late(
    spark, settings, tables, make_batch
):
    # Same as Spark: until the job has seen an event there is no threshold.
    batch = make_batch(
        [
            event("t1", "M1", "10.00", at(-3600)),
            event("t2", "M1", "20.00", at(600)),
        ]
    )
    report = process_batch(spark, batch, 0, settings)

    assert report.watermark_applied is None
    assert report.rows_on_time == 2 and report.rows_late == 0
    assert report.max_event_time == at(600)
    assert report.watermark == at(600 - 120)
    assert tables.df("late_events").count() == 0


def test_record_within_lateness_lands_in_its_window(
    spark, settings, tables, make_batch
):
    process_batch(spark, make_batch([event("t1", "M1", "10.00", at(600))]), 0, settings)
    # watermark is now 480. An event at 500 belongs to [480, 540), still open.
    process_batch(spark, make_batch([event("t2", "M1", "5.50", at(500))]), 1, settings)

    [row] = rows(tables.df("events").filter("transaction_id = 't2'"))
    assert row["window_start"] == naive(at(480))
    assert row["window_end"] == naive(at(540))
    assert row["batch_id"] == 1

    [window] = rows(
        tables.df("merchant_windows").filter(
            "window_start = timestamp'2026-09-13 12:08:00'"
        )
    )
    assert window["tx_count"] == 1
    assert window["total_amount"] == Decimal("5.50")
    assert window["is_final"] is False


def test_record_past_closed_window_is_quarantined_with_evidence(
    spark, settings, tables, make_batch
):
    process_batch(spark, make_batch([event("t1", "M1", "10.00", at(600))]), 0, settings)
    # watermark 480; window [300, 360) closed long ago.
    report = process_batch(
        spark, make_batch([event("t2", "M1", "5.50", at(300))]), 1, settings
    )

    assert report.rows_late == 1 and report.rows_on_time == 0
    assert tables.df("events").filter("transaction_id = 't2'").count() == 0
    assert (
        tables.df("merchant_windows")
        .filter("window_start = timestamp'2026-09-13 12:05:00'")
        .count()
        == 0
    )

    [late] = rows(tables.df("late_events"))
    assert late["transaction_id"] == "t2"
    assert late["event_time"] == naive(at(300))
    assert late["window_end"] == naive(at(360))
    assert late["watermark_at_arrival"] == naive(at(480))
    assert late["lateness_seconds"] == pytest.approx(180.0)
    assert late["batch_id"] == 1
    assert late["amount"] == Decimal("5.50")

    [state] = rows(tables.df("stream_state").filter("batch_id = 1"))
    assert (
        state["rows_in"] == 1 and state["rows_late"] == 1 and state["rows_on_time"] == 0
    )
    assert state["watermark_applied"] == naive(at(480))


def test_event_older_than_watermark_but_in_an_open_window_is_on_time(
    spark, settings, tables, make_batch
):
    # max 610 -> watermark 490. An event at 485 is older than the watermark, but
    # its window [480, 540) has not closed. Spark's aggregation keeps it; so do we.
    process_batch(spark, make_batch([event("t1", "M1", "10.00", at(610))]), 0, settings)
    report = process_batch(
        spark, make_batch([event("t2", "M1", "1.00", at(485))]), 1, settings
    )

    assert report.rows_on_time == 1 and report.rows_late == 0
    [row] = rows(tables.df("events").filter("transaction_id = 't2'"))
    assert row["window_start"] == naive(at(480))


def test_window_end_equal_to_watermark_is_closed(spark, settings, tables, make_batch):
    # max 600 -> watermark 480. Window [420, 480) ends exactly at the watermark:
    # closed, by the same <= Spark uses.
    process_batch(spark, make_batch([event("t1", "M1", "10.00", at(600))]), 0, settings)
    report = process_batch(
        spark, make_batch([event("t2", "M1", "1.00", at(479))]), 1, settings
    )

    assert report.rows_late == 1


def test_watermark_never_regresses_when_a_batch_is_entirely_old(
    spark, settings, tables, make_batch
):
    process_batch(spark, make_batch([event("t1", "M1", "10.00", at(600))]), 0, settings)
    report = process_batch(
        spark,
        make_batch(
            [event("t2", "M1", "1.00", at(100)), event("t3", "M2", "1.00", at(50))]
        ),
        1,
        settings,
    )

    assert report.rows_late == 2
    assert report.max_event_time == at(600)
    assert report.watermark == at(480)


def test_window_closes_when_watermark_passes_its_end_and_stays_frozen(
    spark, settings, tables, make_batch
):
    process_batch(
        spark,
        make_batch(
            [event("t1", "M1", "10.00", at(10)), event("t2", "M1", "20.00", at(50))]
        ),
        0,
        settings,
    )
    [before] = rows(tables.df("merchant_windows"))
    assert before["is_final"] is False and before["tx_count"] == 2

    # max 300 -> watermark 180 >= window_end 60: the window closes in this batch.
    process_batch(spark, make_batch([event("t3", "M2", "1.00", at(300))]), 1, settings)
    [closed] = rows(tables.df("merchant_windows").filter("merchant_id = 'M1'"))
    assert closed["is_final"] is True
    assert closed["updated_batch_id"] == 1
    assert closed["tx_count"] == 2 and closed["total_amount"] == Decimal("30.00")

    # A straggler for the closed window is quarantined; the window does not move.
    process_batch(spark, make_batch([event("t4", "M1", "99.00", at(30))]), 2, settings)
    [after] = rows(tables.df("merchant_windows").filter("merchant_id = 'M1'"))
    assert after == closed
    assert tables.df("late_events").filter("transaction_id = 't4'").count() == 1


def test_empty_batch_carries_the_watermark_forward(spark, settings, tables, make_batch):
    process_batch(spark, make_batch([event("t1", "M1", "10.00", at(600))]), 0, settings)
    report = process_batch(spark, make_batch([]), 1, settings)

    assert report.rows_in == 0
    assert report.watermark_applied == at(480)
    assert report.watermark == at(480)
    assert tables.df("stream_state").count() == 2


def test_stream_state_chains_each_batch_to_the_previous_watermark(
    spark, settings, tables, make_batch
):
    process_batch(spark, make_batch([event("t1", "M1", "10.00", at(600))]), 0, settings)
    process_batch(spark, make_batch([event("t2", "M1", "10.00", at(900))]), 1, settings)
    process_batch(spark, make_batch([event("t3", "M1", "10.00", at(700))]), 2, settings)

    state = rows(tables.df("stream_state"), "batch_id")
    assert [s["watermark_applied"] for s in state] == [
        None,
        naive(at(480)),
        naive(at(780)),
    ]
    assert [s["watermark"] for s in state] == [
        naive(at(480)),
        naive(at(780)),
        naive(at(780)),
    ]


def test_malformed_rows_stop_the_batch_instead_of_vanishing(
    spark, settings, tables, make_batch
):
    batch = make_batch(
        [
            event("t1", "M1", "10.00", at(10)),
            event(None, "M1", "10.00", at(20)),
        ]
    )
    with pytest.raises(MalformedBatchError):
        process_batch(spark, batch, 0, settings)

    assert tables.df("events").count() == 0
    assert tables.df("stream_state").count() == 0


def test_late_rows_keep_every_column_of_the_event(spark, settings, tables, make_batch):
    process_batch(spark, make_batch([event("t1", "M1", "10.00", at(600))]), 0, settings)
    process_batch(spark, make_batch([event("t2", "M1", "10.00", at(100))]), 1, settings)

    late_columns = set(tables.df("late_events").columns)
    event_columns = set(tables.df("events").columns)
    assert event_columns <= late_columns
    assert late_columns - event_columns == {"watermark_at_arrival", "lateness_seconds"}
    assert (
        tables.df("late_events").filter(F.col("processing_time").isNull()).count() == 0
    )
