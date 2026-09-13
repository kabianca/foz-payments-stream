"""The producer must misbehave predictably: same seed, same stream; late means
late enough; a duplicate is byte-for-byte the same event."""

import itertools
import json
from datetime import timedelta

from foz.producer import Event, Knobs, generate, main
from tests.conftest import T0, at


def stepping_clock(step_seconds: float = 1.0):
    ticks = itertools.count()
    return lambda: at(next(ticks) * step_seconds)


def frozen_clock():
    return lambda: T0


def test_generate_is_deterministic_under_a_seed():
    a = [
        (e.to_json(), kind)
        for e, kind in generate(Knobs(count=50, seed=9), frozen_clock())
    ]
    b = [
        (e.to_json(), kind)
        for e, kind in generate(Knobs(count=50, seed=9), frozen_clock())
    ]
    c = [
        (e.to_json(), kind)
        for e, kind in generate(Knobs(count=50, seed=10), frozen_clock())
    ]

    assert a == b
    assert a != c


def test_late_events_are_stamped_past_the_closed_window():
    knobs = Knobs(
        count=200,
        late_fraction=0.3,
        duplicate_fraction=0,
        window_seconds=60,
        allowed_lateness_seconds=120,
        jitter_seconds=5,
    )
    threshold = T0 - timedelta(seconds=60 + 120 + 5)
    kinds = {"fresh": 0, "late": 0}
    for e, kind in generate(knobs, frozen_clock()):
        kinds[kind] += 1
        if kind == "late":
            assert e.event_time <= threshold
        else:
            assert T0 - timedelta(seconds=5) <= e.event_time <= T0
    assert kinds["late"] > 0 and kinds["fresh"] > 0


def test_duplicates_are_exact_copies_of_events_already_sent():
    seen: list[Event] = []
    duplicates = 0
    for e, kind in generate(Knobs(count=200, duplicate_fraction=0.2), frozen_clock()):
        if kind == "duplicate":
            duplicates += 1
            assert e in seen
        seen.append(e)
    assert duplicates > 0


def test_shuffle_disorders_arrival_and_shuffle_one_keeps_it():
    ordered = Knobs(
        count=30, shuffle=1, jitter_seconds=0, late_fraction=0, duplicate_fraction=0
    )
    times = [e.event_time for e, _ in generate(ordered, stepping_clock())]
    assert times == sorted(times)

    shuffled = Knobs(
        count=30, shuffle=10, jitter_seconds=0, late_fraction=0, duplicate_fraction=0
    )
    times = [e.event_time for e, _ in generate(shuffled, stepping_clock())]
    assert times != sorted(times)
    assert sorted(times) == sorted(
        e.event_time for e, _ in generate(ordered, stepping_clock())
    )


def test_count_is_respected_and_kinds_add_up():
    stream = list(generate(Knobs(count=123), frozen_clock()))
    assert len(stream) == 123
    assert {kind for _, kind in stream} <= {"fresh", "late", "duplicate"}


def test_amount_is_a_decimal_string_and_event_time_is_iso_utc():
    e, _ = next(generate(Knobs(count=1), frozen_clock()))
    payload = json.loads(e.to_json())
    assert isinstance(payload["amount"], str)
    assert (
        payload["amount"].count(".") == 1 and len(payload["amount"].split(".")[1]) == 2
    )
    assert payload["event_time"].endswith("+00:00")
    assert set(payload) == {
        "transaction_id",
        "merchant_id",
        "account_id",
        "amount",
        "currency",
        "method",
        "event_time",
    }


def test_dry_run_prints_one_json_line_per_event(capsys):
    main(["--dry-run", "--count", "7", "--seed", "1"])
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 7
    assert all(json.loads(line)["currency"] == "BRL" for line in lines)
