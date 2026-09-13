"""Exactly-once is a claim about the destination. These tests replay batches,
crash between writes and re-send events; the tables must not notice."""

import random
from decimal import Decimal

import pytest

import foz.batch as batch_module
from foz.batch import process_batch
from foz.check import run_checks
from tests.conftest import at, event, rows


def snapshot(tables) -> dict:
    return {
        "events": rows(tables.df("events"), "transaction_id"),
        "late_events": rows(tables.df("late_events"), "transaction_id"),
        "merchant_windows": rows(
            tables.df("merchant_windows"), "window_start", "merchant_id"
        ),
        "stream_state": rows(tables.df("stream_state"), "batch_id"),
    }


def assert_invariants(tables) -> None:
    failed = [r for r in run_checks(tables) if not r.ok]
    assert not failed, [f"{r.name}: {r.detail}" for r in failed]


def test_replaying_a_batch_changes_nothing(spark, settings, tables, make_batch):
    first = make_batch(
        [event("t1", "M1", "10.00", at(600)), event("t2", "M2", "20.00", at(30))]
    )
    second = make_batch(
        [
            event("t3", "M1", "5.00", at(500)),  # on time
            event("t4", "M2", "7.00", at(100)),  # late
            event("t1", "M1", "10.00", at(600)),  # duplicate across batches
        ]
    )
    process_batch(spark, first, 0, settings, now=at(1000))
    report = process_batch(spark, second, 1, settings, now=at(1005))
    before = snapshot(tables)

    replay = process_batch(spark, second, 1, settings, now=at(1005))

    assert snapshot(tables) == before
    assert replay == report
    assert (
        report.rows_on_time == 1
        and report.rows_late == 1
        and report.rows_duplicate == 1
    )
    assert_invariants(tables)


def test_replay_with_a_different_clock_only_moves_processing_time(
    spark, settings, tables, make_batch
):
    process_batch(
        spark,
        make_batch([event("t1", "M1", "10.00", at(600))]),
        0,
        settings,
        now=at(1000),
    )
    second = make_batch([event("t2", "M1", "5.00", at(500))])
    process_batch(spark, second, 1, settings, now=at(1005))
    before = snapshot(tables)

    process_batch(spark, second, 1, settings, now=at(1060))
    after = snapshot(tables)

    assert after["events"] == before["events"]
    assert after["late_events"] == before["late_events"]
    for old, new in zip(before["merchant_windows"], after["merchant_windows"]):
        assert {k: v for k, v in old.items() if k != "updated_at"} == {
            k: v for k, v in new.items() if k != "updated_at"
        }
    for old, new in zip(before["stream_state"], after["stream_state"]):
        assert {k: v for k, v in old.items() if k != "processing_time"} == {
            k: v for k, v in new.items() if k != "processing_time"
        }


@pytest.mark.parametrize(
    "crash_in",
    ["_merge_insert_only", "_refresh_windows", "_close_windows", "_record"],
)
def test_crash_between_writes_then_replay_converges(
    spark, settings, tables, make_batch, monkeypatch, crash_in
):
    """Kill the batch after some of its writes landed, then run it again."""
    process_batch(
        spark,
        make_batch(
            [event("t1", "M1", "10.00", at(600)), event("t2", "M1", "1.00", at(30))]
        ),
        0,
        settings,
    )
    second = make_batch(
        [
            event("t3", "M1", "5.00", at(500)),
            event(
                "t4", "M2", "8.00", at(700)
            ),  # moves the watermark to 580: closes [480,540)
            event("t5", "M2", "7.00", at(100)),  # late
        ]
    )

    original = getattr(batch_module, crash_in)
    calls = {"n": 0}

    def crash_after_first_write(*args, **kwargs):
        calls["n"] += 1
        result = original(*args, **kwargs)
        if calls["n"] == 1:
            raise RuntimeError("simulated crash mid-batch")
        return result

    monkeypatch.setattr(batch_module, crash_in, crash_after_first_write)
    with pytest.raises(RuntimeError, match="simulated crash"):
        process_batch(spark, second, 1, settings)
    monkeypatch.setattr(batch_module, crash_in, original)

    report = process_batch(spark, second, 1, settings)

    assert (
        report.rows_on_time == 2
        and report.rows_late == 1
        and report.rows_duplicate == 0
    )
    assert tables.df("events").count() == 4
    assert tables.df("late_events").count() == 1
    assert tables.df("stream_state").count() == 2
    assert_invariants(tables)


def test_duplicate_within_a_batch_first_arrival_wins(
    spark, settings, tables, make_batch
):
    batch = make_batch(
        [
            event("t1", "M1", "10.00", at(10), offset=7),
            event("t1", "M1", "99.00", at(10), offset=8),
        ]
    )
    report = process_batch(spark, batch, 0, settings)

    assert (
        report.rows_in == 2 and report.rows_on_time == 1 and report.rows_duplicate == 1
    )
    [row] = rows(tables.df("events"))
    assert row["amount"] == Decimal("10.00") and row["kafka_offset"] == 7
    [window] = rows(tables.df("merchant_windows"))
    assert window["tx_count"] == 1 and window["total_amount"] == Decimal("10.00")


def test_duplicate_across_batches_is_absorbed(spark, settings, tables, make_batch):
    process_batch(spark, make_batch([event("t1", "M1", "10.00", at(10))]), 0, settings)
    report = process_batch(
        spark, make_batch([event("t1", "M1", "10.00", at(10))]), 1, settings
    )

    assert report.rows_duplicate == 1 and report.rows_on_time == 0
    [row] = rows(tables.df("events"))
    assert row["batch_id"] == 0
    [window] = rows(tables.df("merchant_windows"))
    assert window["tx_count"] == 1
    assert_invariants(tables)


def test_late_copy_of_a_known_transaction_is_a_duplicate_not_a_late_event(
    spark, settings, tables, make_batch
):
    process_batch(
        spark,
        make_batch(
            [event("t1", "M1", "10.00", at(100)), event("t2", "M1", "1.00", at(600))]
        ),
        0,
        settings,
    )
    # watermark 480; t1's window [60,120) is closed, but t1 is already counted.
    report = process_batch(
        spark, make_batch([event("t1", "M1", "10.00", at(100))]), 1, settings
    )

    assert report.rows_late == 0 and report.rows_duplicate == 1
    assert tables.df("late_events").count() == 0
    assert tables.df("events").count() == 2


def test_invariants_hold_under_random_replays(spark, settings, tables, make_batch):
    """A short synthetic stream, batched, with batches replayed at random."""
    rng = random.Random(7)
    events = []
    for i in range(60):
        moment = at(rng.uniform(0, 900))
        events.append(
            event(f"t{i}", f"M{rng.randrange(3)}", f"{rng.uniform(1, 100):.2f}", moment)
        )
    # inject duplicates
    for i in range(6):
        events.append(dict(rng.choice(events[:60])))

    batches = [events[i : i + 11] for i in range(0, len(events), 11)]
    for batch_id, batch_events in enumerate(batches):
        df = make_batch(batch_events)
        process_batch(spark, df, batch_id, settings)
        if rng.random() < 0.5:
            process_batch(spark, df, batch_id, settings)

    assert_invariants(tables)
    total_in = sum(s["rows_in"] for s in rows(tables.df("stream_state")))
    assert total_in == len(events)
