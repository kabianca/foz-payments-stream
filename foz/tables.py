"""Delta tables by path. No metastore: a path is a name."""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from foz.config import Settings
from foz.schema import TABLES


@dataclass(frozen=True)
class Tables:
    spark: SparkSession
    settings: Settings

    def path(self, name: str) -> str:
        return self.settings.table_path(name)

    def delta(self, name: str) -> DeltaTable:
        return DeltaTable.forPath(self.spark, self.path(name))

    def df(self, name: str) -> DataFrame:
        return self.spark.read.format("delta").load(self.path(name))

    def ensure(self) -> None:
        """Create the four tables with their declared schema if they do not exist.

        Idempotent; safe to call on every job start and before every test.
        """
        for name, schema in TABLES.items():
            (
                DeltaTable.createIfNotExists(self.spark)
                .location(self.path(name))
                .addColumns(schema)
                .execute()
            )
