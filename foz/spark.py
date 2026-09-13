"""SparkSession factory shared by the job, the checks and the tests."""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_session(
    app_name: str = "foz",
    *,
    master: str | None = None,
    shuffle_partitions: int = 4,
    delta_snapshot_partitions: int = 4,
    with_pip_jars: bool = False,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Delta-enabled session pinned to UTC.

    ``with_pip_jars=True`` resolves the Delta jars through the ``delta-spark``
    pip package (laptop and CI). Inside the Docker image the jars are already
    on the classpath and resolving them again would need network at start-up.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Delta rebuilds table state with 50 partitions by default, sized for
        # logs with millions of files. Four small tables and a laptop want a
        # handful: this alone makes every MERGE three times faster here.
        .config(
            "spark.databricks.delta.snapshotPartitions", str(delta_snapshot_partitions)
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )
    if master:
        builder = builder.master(master)
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    if with_pip_jars:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()
