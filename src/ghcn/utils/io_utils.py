"""I/O helpers for small collected results and path utilities."""

from __future__ import annotations

import re
from typing import Iterable

from pyspark.sql import DataFrame


def parse_hdfs_ls_size(line: str) -> tuple[str, int] | None:
    """Parse an `hdfs dfs -ls` line into (path, size_bytes).

    Handles both classic HDFS listings (with user/group) and Azure wasbs
    listings that omit those columns::

      -rw-r--r--  1 user group  12345 2024-01-01 00:00 wasbs://.../file
      -rw-r--r--  1          12345 2024-01-01 00:00 wasbs://.../file
    """
    line = line.strip()
    if not line or line.startswith("Found "):
        return None
    pat = re.compile(
        r"^\S+\s+\d+\s+(?:\S+\s+\S+\s+)?(\d+)\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s+(\S+)\s*$"
    )
    m = pat.match(line)
    if m:
        return m.group(2), int(m.group(1))
    return None


def year_from_daily_filename(path: str) -> int | None:
    """Extract YYYY from a daily filename like .../1875.csv.gz."""
    m = re.search(r"(?:/|^)(\d{4})\.csv\.gz$", path)
    return int(m.group(1)) if m else None


def parse_daily_year_sizes(ls_lines: list[str]) -> list[dict]:
    """Build ``[{year, size, name}, ...]`` from ``hdfs dfs -ls .../daily`` lines."""
    rows: list[dict] = []
    for line in ls_lines:
        parsed = parse_hdfs_ls_size(line)
        if not parsed:
            # Fallback: filename + largest integer token on the line
            ym = re.search(r"(\d{4})\.csv\.gz", line)
            if not ym:
                continue
            year = int(ym.group(1))
            ints = [int(x) for x in re.findall(r"\b\d+\b", line)]
            sizes = [n for n in ints if n != year and n > 1000]
            if not sizes:
                continue
            rows.append({"year": year, "size": max(sizes), "name": f"{year}.csv.gz"})
            continue
        path, size = parsed
        year = year_from_daily_filename(path)
        if year is None:
            continue
        rows.append({"year": year, "size": size, "name": f"{year}.csv.gz"})
    return sorted(rows, key=lambda r: r["year"])


def bytes_to_human(n: int | float) -> str:
    """Human-readable byte size."""
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:,.2f} {unit}"
        n /= 1024.0
    return f"{n:,.2f} PB"


def safe_count(df: DataFrame) -> int:
    """Count rows — only call on filtered/aggregated frames or metadata."""
    return df.count()


def assert_not_full_daily_collect(label: str, n_rows: int, hard_limit: int = 5_000_000) -> None:
    """Guardrail: refuse to collect enormous frames to the driver."""
    if n_rows > hard_limit:
        raise RuntimeError(
            f"Refusing to collect '{label}' ({n_rows:,} rows). "
            "Aggregate or filter further before collecting."
        )


def chunked(items: Iterable, size: int):
    buf = []
    for item in items:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf
