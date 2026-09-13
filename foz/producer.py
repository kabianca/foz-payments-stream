"""A synthetic payments producer that misbehaves on purpose.

A producer that only emits well-behaved events proves nothing about the job
downstream. This one is deterministic under a seed and injects, each behind its
own knob:

* jitter      - every event is a few seconds behind the wall clock, so event
                order and arrival order already disagree a little
* shuffle     - events leave in shuffled groups, so out-of-order inside a window
                is the norm rather than the exception
* late        - a fraction is stamped far enough in the past that its window
                has already closed when it arrives
* duplicates  - a fraction re-sends an event already sent, byte for byte, the
                way a retried producer would

Run it as ``python -m foz.producer --help``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

log = logging.getLogger("foz.producer")

METHODS = ("pix", "credit", "debit")


@dataclass(frozen=True)
class Knobs:
    count: int = 200
    rate: float = 20.0
    seed: int = 42
    merchants: int = 5
    accounts: int = 50
    jitter_seconds: float = 5.0
    shuffle: int = 10
    late_fraction: float = 0.10
    late_spread_seconds: float = 60.0
    duplicate_fraction: float = 0.05
    window_seconds: int = 60
    allowed_lateness_seconds: int = 120


@dataclass(frozen=True)
class Event:
    transaction_id: str
    merchant_id: str
    account_id: str
    amount: str
    currency: str
    method: str
    event_time: datetime

    @property
    def key(self) -> str:
        return self.merchant_id

    def to_json(self) -> str:
        payload = asdict(self)
        payload["event_time"] = self.event_time.isoformat(timespec="milliseconds")
        return json.dumps(payload, separators=(",", ":"))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def generate(
    knobs: Knobs, clock: Callable[[], datetime] = utc_now
) -> Iterator[tuple[Event, str]]:
    """Yield (event, kind) pairs; kind is 'fresh', 'late' or 'duplicate'.

    Deterministic for a given seed and clock. A 'late' event is stamped at least
    window + lateness + jitter behind the clock, which guarantees its window is
    closed as soon as the watermark has seen one fresh event.
    """
    rng = random.Random(knobs.seed)
    sent: list[Event] = []
    buffer: list[tuple[Event, str]] = []
    produced = 0

    def fresh() -> tuple[Event, str]:
        if rng.random() < knobs.late_fraction:
            kind = "late"
            delay = (
                knobs.window_seconds
                + knobs.allowed_lateness_seconds
                + knobs.jitter_seconds
                + rng.uniform(0, knobs.late_spread_seconds)
            )
        else:
            kind = "fresh"
            delay = rng.uniform(0, knobs.jitter_seconds)
        event = Event(
            transaction_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            merchant_id=f"M{rng.randrange(knobs.merchants):03d}",
            account_id=f"A{rng.randrange(knobs.accounts):05d}",
            amount=f"{rng.uniform(1.0, 500.0):.2f}",
            currency="BRL",
            method=rng.choice(METHODS),
            event_time=clock() - timedelta(seconds=delay),
        )
        return event, kind

    def drain() -> Iterator[tuple[Event, str]]:
        rng.shuffle(buffer)
        while buffer:
            item = buffer.pop()
            sent.append(item[0])
            yield item

    while produced < knobs.count:
        if sent and rng.random() < knobs.duplicate_fraction:
            buffer.append((rng.choice(sent[-50:]), "duplicate"))
        else:
            buffer.append(fresh())
        produced += 1
        if len(buffer) >= max(knobs.shuffle, 1):
            yield from drain()
    yield from drain()


def send(
    stream: Iterable[tuple[Event, str]],
    *,
    bootstrap_servers: str,
    topic: str,
    rate: float,
) -> Counter:
    """Produce to Kafka at roughly ``rate`` events per second. Returns a count
    of what was injected so the numbers can be compared with stream_state."""
    from confluent_kafka import Producer

    failures: list[str] = []

    def on_delivery(err, msg) -> None:
        if err is not None:
            failures.append(str(err))

    producer = Producer({"bootstrap.servers": bootstrap_servers, "acks": "all"})
    counts: Counter = Counter()
    started = time.monotonic()
    for index, (event, kind) in enumerate(stream):
        producer.produce(
            topic, key=event.key, value=event.to_json(), on_delivery=on_delivery
        )
        producer.poll(0)
        counts[kind] += 1
        pause = started + (index + 1) / rate - time.monotonic()
        if pause > 0:
            time.sleep(pause)
    producer.flush()
    if failures:
        raise RuntimeError(f"{len(failures)} delivery failure(s); first: {failures[0]}")
    return counts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    defaults = Knobs()
    parser.add_argument(
        "--count", type=int, default=defaults.count, help="events to send"
    )
    parser.add_argument(
        "--rate", type=float, default=defaults.rate, help="events per second"
    )
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--merchants", type=int, default=defaults.merchants)
    parser.add_argument(
        "--jitter",
        type=float,
        default=defaults.jitter_seconds,
        help="max normal delay, seconds",
    )
    parser.add_argument(
        "--shuffle",
        type=int,
        default=defaults.shuffle,
        help="out-of-order group size (1 = in order)",
    )
    parser.add_argument(
        "--late",
        type=float,
        default=defaults.late_fraction,
        help="fraction of events past the closed window",
    )
    parser.add_argument(
        "--late-spread",
        type=float,
        default=defaults.late_spread_seconds,
        help="extra lateness range, seconds",
    )
    parser.add_argument(
        "--dup",
        type=float,
        default=defaults.duplicate_fraction,
        help="fraction of re-sent events",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=int(os.environ.get("FOZ_WINDOW_SECONDS", defaults.window_seconds)),
    )
    parser.add_argument(
        "--lateness",
        type=int,
        default=int(
            os.environ.get(
                "FOZ_ALLOWED_LATENESS_SECONDS", defaults.allowed_lateness_seconds
            )
        ),
    )
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("FOZ_BOOTSTRAP_SERVERS", "localhost:29092"),
    )
    parser.add_argument("--topic", default=os.environ.get("FOZ_TOPIC", "payments"))
    parser.add_argument(
        "--dry-run", action="store_true", help="print JSON lines instead of producing"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    args = parse_args(argv)
    knobs = Knobs(
        count=args.count,
        rate=args.rate,
        seed=args.seed,
        merchants=args.merchants,
        jitter_seconds=args.jitter,
        shuffle=args.shuffle,
        late_fraction=args.late,
        late_spread_seconds=args.late_spread,
        duplicate_fraction=args.dup,
        window_seconds=args.window,
        allowed_lateness_seconds=args.lateness,
    )
    stream = generate(knobs)
    if args.dry_run:
        counts: Counter = Counter()
        for event, kind in stream:
            counts[kind] += 1
            print(event.to_json())
    else:
        counts = send(
            stream, bootstrap_servers=args.bootstrap, topic=args.topic, rate=args.rate
        )
    log.info(
        "sent %d events to %s: fresh=%d late=%d duplicate=%d (seed=%d)",
        sum(counts.values()),
        args.topic,
        counts["fresh"],
        counts["late"],
        counts["duplicate"],
        knobs.seed,
    )


if __name__ == "__main__":
    main()
