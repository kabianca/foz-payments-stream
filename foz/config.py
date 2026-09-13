"""Runtime settings. Everything comes from the environment with a sane default,
so the same code runs inside Docker Compose, on a laptop and under pytest."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = "kafka:9092"
    topic: str = "payments"

    delta_root: str = "data/delta"
    checkpoint_root: str = "data/checkpoints"

    # Tumbling window size and how far behind the newest event we still accept
    # a record for an open window. Both in seconds so there is nothing to parse.
    window_seconds: int = 60
    allowed_lateness_seconds: int = 120

    trigger_seconds: int = 5
    max_offsets_per_trigger: int = 2000
    starting_offsets: str = "earliest"

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.window_seconds)

    @property
    def allowed_lateness(self) -> timedelta:
        return timedelta(seconds=self.allowed_lateness_seconds)

    # Paths are absolute: Delta reads a relative location as a table name.
    @property
    def checkpoint_path(self) -> str:
        return os.path.abspath(os.path.join(self.checkpoint_root, self.topic))

    def table_path(self, name: str) -> str:
        return os.path.abspath(os.path.join(self.delta_root, name))

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            bootstrap_servers=os.environ.get(
                "FOZ_BOOTSTRAP_SERVERS", cls.bootstrap_servers
            ),
            topic=os.environ.get("FOZ_TOPIC", cls.topic),
            delta_root=os.environ.get("FOZ_DELTA_ROOT", cls.delta_root),
            checkpoint_root=os.environ.get("FOZ_CHECKPOINT_ROOT", cls.checkpoint_root),
            window_seconds=_env_int("FOZ_WINDOW_SECONDS", cls.window_seconds),
            allowed_lateness_seconds=_env_int(
                "FOZ_ALLOWED_LATENESS_SECONDS", cls.allowed_lateness_seconds
            ),
            trigger_seconds=_env_int("FOZ_TRIGGER_SECONDS", cls.trigger_seconds),
            max_offsets_per_trigger=_env_int(
                "FOZ_MAX_OFFSETS_PER_TRIGGER", cls.max_offsets_per_trigger
            ),
            starting_offsets=os.environ.get(
                "FOZ_STARTING_OFFSETS", cls.starting_offsets
            ),
        )
