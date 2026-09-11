"""Azure Blob / wasbs path helpers for GHCN data and user outputs."""

from __future__ import annotations

import getpass
import os
import re
from dataclasses import dataclass


AZURE_ACCOUNT_NAME = "madsstorage002"
AZURE_DATA_CONTAINER = "campus-data"
AZURE_USER_CONTAINER = "campus-user"

# SAS token for the campus-user container (write access).
# NEVER hard-code the token. It is read from the environment so that no
# secret is committed to source control. See .env.example and README.
AZURE_USER_TOKEN_ENV = "AZURE_USER_SAS_TOKEN"


def azure_user_token() -> str:
    """Return the SAS token for the user output container from the environment.

    Raises a clear error if the token has not been provided so notebooks fail
    fast with an actionable message instead of an opaque Azure auth error.
    """
    token = os.environ.get(AZURE_USER_TOKEN_ENV, "").strip()
    if not token:
        raise RuntimeError(
            f"Environment variable {AZURE_USER_TOKEN_ENV} is not set. "
            "Provide the campus-user SAS token (see .env.example) before "
            "reading or writing to the user container."
        )
    return token

GHCND_PREFIX = "ghcnd"


def get_username() -> str:
    """Return the cluster username without the domain suffix."""
    return re.sub(r"@.*", "", getpass.getuser())


def wasbs(container: str, relative: str = "", account: str = AZURE_ACCOUNT_NAME) -> str:
    """Build a wasbs:// URI for the given container and relative path."""
    relative = relative.lstrip("/")
    base = f"wasbs://{container}@{account}.blob.core.windows.net"
    return f"{base}/{relative}" if relative else f"{base}/"


@dataclass(frozen=True)
class Paths:
    """Resolved data and output paths for one username."""

    username: str
    account: str = AZURE_ACCOUNT_NAME
    data_container: str = AZURE_DATA_CONTAINER
    user_container: str = AZURE_USER_CONTAINER

    @property
    def ghcnd_root(self) -> str:
        return wasbs(self.data_container, GHCND_PREFIX)

    @property
    def daily_dir(self) -> str:
        return wasbs(self.data_container, f"{GHCND_PREFIX}/daily")

    def daily_year(self, year: int) -> str:
        return wasbs(self.data_container, f"{GHCND_PREFIX}/daily/{year}.csv.gz")

    @property
    def stations(self) -> str:
        return wasbs(self.data_container, f"{GHCND_PREFIX}/ghcnd-stations.txt")

    @property
    def countries(self) -> str:
        return wasbs(self.data_container, f"{GHCND_PREFIX}/ghcnd-countries.txt")

    @property
    def states(self) -> str:
        return wasbs(self.data_container, f"{GHCND_PREFIX}/ghcnd-states.txt")

    @property
    def inventory(self) -> str:
        return wasbs(self.data_container, f"{GHCND_PREFIX}/ghcnd-inventory.txt")

    @property
    def user_root(self) -> str:
        return wasbs(self.user_container, self.username)

    def output(self, *parts: str) -> str:
        """User-container output path, e.g. output('enriched_stations')."""
        rel = "/".join([self.username, *[p.strip("/") for p in parts]])
        return wasbs(self.user_container, rel)


def default_paths(username: str | None = None) -> Paths:
    return Paths(username=username or get_username())
