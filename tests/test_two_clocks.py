"""Event time and processing time are different columns, both persisted, and
only one of them decides the window."""

from foz.batch import process_batch
from tests.conftest import at, event, naive, rows


def test_both_clocks_are_persisted_on_every_row(spark, settings, tables, make_batch):
    process_batch(
        spark,
        make_batch([event("t1", "M1", "10.00", at(600))]),
        0,
        settings,
        now=at(5000),
    )
    process_batch(
        spark,
        make_batch([event("t2", "M1", "10.00", at(100))]),
        1,
        settings,
        now=at(5010),
    )

    [on_time] = rows(tables.df("events"))
    assert on_time["event_time"] == naive(at(600))
    assert on_time["processing_time"] == naive(at(5000))

    [late] = rows(tables.df("late_events"))
    assert late["event_time"] == naive(at(100))
    assert late["processing_time"] == naive(at(5010))

    for name in ("events", "late_events"):
        assert {"event_time", "processing_time", "kafka_timestamp"} <= set(
            tables.df(name).columns
        )


def test_the_window_comes_from_event_time_not_processing_time(
    spark, settings, tables, make_batch
):
    process_batch(
        spark,
        make_batch([event("t1", "M1", "10.00", at(30))]),
        0,
        settings,
        now=at(86400),
    )

    [row] = rows(tables.df("events"))
    assert row["window_start"] == naive(at(0))
    assert row["window_end"] == naive(at(60))
    [window] = rows(tables.df("merchant_windows"))
    assert window["window_start"] == naive(at(0))
    assert window["updated_at"] == naive(at(86400))


def test_the_watermark_comes_from_event_time_not_processing_time(
    spark, settings, tables, make_batch
):
    # Processing a very old event much later must not push the watermark to "now".
    report = process_batch(
        spark,
        make_batch([event("t1", "M1", "10.00", at(600))]),
        0,
        settings,
        now=at(999_999),
    )

    assert report.max_event_time == at(600)
    assert report.watermark == at(480)


def test_processing_time_is_one_value_per_batch(spark, settings, tables, make_batch):
    batch = make_batch([event(f"t{i}", "M1", "1.00", at(i)) for i in range(5)])
    process_batch(spark, batch, 0, settings, now=at(1000))

    stamps = {r["processing_time"] for r in rows(tables.df("events"))}
    assert stamps == {naive(at(1000))}
