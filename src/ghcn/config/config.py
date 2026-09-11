"""Spark session configuration defaults for the assignment."""

from __future__ import annotations

from .paths import (
    AZURE_ACCOUNT_NAME,
    AZURE_USER_CONTAINER,
    azure_user_token,
    get_username,
)

# Soft caps from the assignment brief (Processing Q4).
MAX_EXECUTOR_INSTANCES = 4
MAX_EXECUTOR_CORES = 2
MAX_EXECUTOR_MEMORY_GB = 4
MAX_MASTER_MEMORY_GB = 4

# Sensible defaults for early exploration (Processing Q1–Q2).
DEFAULT_EXECUTOR_INSTANCES = 2
DEFAULT_EXECUTOR_CORES = 1
DEFAULT_EXECUTOR_MEMORY_GB = 1
DEFAULT_MASTER_MEMORY_GB = 1

# Heavier defaults once enriched stations / full daily scans are needed.
ANALYSIS_EXECUTOR_INSTANCES = 4
ANALYSIS_EXECUTOR_CORES = 2
ANALYSIS_EXECUTOR_MEMORY_GB = 4
ANALYSIS_MASTER_MEMORY_GB = 4

# Most recent complete year of daily used for schema trials / subset joins.
# Update if a newer year file appears under ghcnd/daily/.
MOST_RECENT_DAILY_YEAR = 2025

# Target year for "active in 2026" inventory / daily questions.
ACTIVE_YEAR = 2026

# Choropleth year (Visualizations Q2b).
CHOROPLETH_YEAR = 2025


def spark_app_name(username: str | None = None) -> str:
    return f"{username or get_username()} (notebook)"


def azure_sas_config_key(
    container: str = AZURE_USER_CONTAINER,
    account: str = AZURE_ACCOUNT_NAME,
) -> str:
    return f"fs.azure.sas.{container}.{account}.blob.core.windows.net"


def azure_sas_token() -> str:
    return azure_user_token()
