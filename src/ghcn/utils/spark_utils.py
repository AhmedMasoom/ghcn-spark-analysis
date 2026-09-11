"""Spark session helpers (notebook-friendly wrappers)."""

from __future__ import annotations

import random
from typing import Any

from ghcn.config.config import (
    DEFAULT_EXECUTOR_CORES,
    DEFAULT_EXECUTOR_INSTANCES,
    DEFAULT_EXECUTOR_MEMORY_GB,
    DEFAULT_MASTER_MEMORY_GB,
    azure_sas_config_key,
    azure_sas_token,
    spark_app_name,
)
from ghcn.config.paths import get_username


def build_spark_session(
    executor_instances: int = DEFAULT_EXECUTOR_INSTANCES,
    executor_cores: int = DEFAULT_EXECUTOR_CORES,
    worker_memory: float = DEFAULT_EXECUTOR_MEMORY_GB,
    master_memory: float = DEFAULT_MASTER_MEMORY_GB,
    app_name: str | None = None,
):
    """Create a SparkSession configured for the UC MADS Kubernetes cluster.

    Mirrors the course starter notebook's start_spark() defaults so library
    code and notebooks stay consistent.
    """
    from pyspark.sql import SparkSession
    from pyspark import SparkContext

    username = get_username()
    cores = executor_instances * executor_cores
    partitions = cores * 4
    name = app_name or spark_app_name(username)

    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home=/tmp/{username}/spark/",
        )
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.executor.instances", str(executor_instances))
        .config("spark.executor.cores", str(executor_cores))
        .config("spark.cores.max", str(cores))
        .config("spark.driver.memory", f"{master_memory}g")
        .config("spark.executor.memory", f"{worker_memory}g")
        .config("spark.driver.maxResultSize", "0")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config(
            "spark.kubernetes.container.image",
            "madsregistry001.azurecr.io/hadoop-spark:v3.3.5-openjdk-8",
        )
        .config("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
        .config("spark.kubernetes.memoryOverheadFactor", "0.3")
        .config("spark.memory.fraction", "0.1")
        .config(azure_sas_config_key(), azure_sas_token())
        .config("spark.app.name", name)
        .getOrCreate()
    )
    sc = SparkContext.getOrCreate()
    return spark, sc


def random_ui_port(base: int = 4000) -> int:
    return base + random.randint(1, 999)


def conf_snapshot(sc: Any) -> dict[str, str]:
    return dict(sc.getConf().getAll())
