"""Order of arrival must not change the final aggregate: only event_time does."""

import random

from foz.batch import process_batch
from foz.config import Settings
from foz.tables import Tables
from tests.conftest import at, event, rows


def _events(seed: int = 3, n: int = 40, spread: float = 300.0) -> list[dict]:
    rng = random.Random(seed)
    return [
        event(
            f"t{i}",
            f"M{rng.randrange(3)}",
            f"{rng.uniform(1, 100):.2f}",
            at(rng.uniform(0, spread)),
        )
        for i in range(n)
    ]


def _run(
    spark, tmp_path, name: str, events: list[dict], batch_size: int, make_batch
) -> list[dict]:
    settings = Settings(
        delta_root=str(tmp_path / name / "delta"),
        checkpoint_root=str(tmp_path / name / "ckpt"),
    )
    tables = Tables(spark, settings)
    tables.ensure()
    for batch_id, start in enumerate(range(0, len(events), batch_size)):
        process_batch(
            spark, make_batch(events[start : start + batch_size]), batch_id, settings
        )
    return [
        {k: r[k] for k in ("window_start", "merchant_id", "tx_count", "total_amount")}
        for r in rows(tables.df("merchant_windows"), "window_start", "merchant_id")
    ]


def test_out_of_order_within_the_window_gives_the_same_aggregate(
    spark, tmp_path, make_batch
):
    # Disorder inside the lateness budget: the newest event may arrive first
    # and set the watermark, but no window closes before the rest arrive.
    events = _events(spread=100.0)
    in_order = sorted(events, key=lambda e: e["event_time"])
    shuffled = list(events)
    random.Random(11).shuffle(shuffled)

    assert _run(spark, tmp_path, "ordered", in_order, 8, make_batch) == _run(
        spark, tmp_path, "shuffled", shuffled, 8, make_batch
    )


def test_batch_boundaries_do_not_change_the_aggregate(spark, tmp_path, make_batch):
    events = sorted(_events(), key=lambda e: e["event_time"])

    assert _run(spark, tmp_path, "one", events, len(events), make_batch) == _run(
        spark, tmp_path, "many", events, 7, make_batch
    )


def test_reverse_order_gives_the_same_aggregate(spark, tmp_path, make_batch):
    # Arrival order fully reversed: the newest event arrives first and sets the
    # watermark; every other event is within the lateness budget, so all land.
    events = sorted(_events(n=20, spread=100.0), key=lambda e: e["event_time"])
    reverse = list(reversed(events))

    assert _run(spark, tmp_path, "fwd", events, 5, make_batch) == _run(
        spark, tmp_path, "rev", reverse, 5, make_batch
    )
