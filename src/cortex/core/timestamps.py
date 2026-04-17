"""Timestamp utilities for Cortex.

All timestamps stored and returned by Cortex use the canonical UTC format:
    2026-04-13T10:00:00.000000Z  (microseconds, Z suffix)

Use `now_utc()` to generate and `canonicalize()` to normalise external input.
"""
from __future__ import annotations

from datetime import datetime, timezone

CANONICAL_TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def now_utc() -> str:
    """Return the current UTC time in canonical form."""
    return datetime.now(timezone.utc).strftime(CANONICAL_TS_FORMAT)


def canonicalize(ts: str | datetime) -> str:
    """Parse any ISO-8601 timestamp and return canonical UTC form.

    Accepts:
    - datetime objects (must be timezone-aware)
    - Strings with Z suffix  (e.g. "2026-04-13T10:00:00Z")
    - Strings with +00:00    (e.g. "2026-04-13T10:00:00+00:00")
    - Strings with microseconds
    - Strings without microseconds (padded to 0)

    Raises ValueError on un-parseable input or naive datetime.
    """
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            raise ValueError(
                f"Naive datetime is not allowed; provide a timezone-aware datetime: {ts!r}"
            )
        dt = ts.astimezone(timezone.utc)
        return dt.strftime(CANONICAL_TS_FORMAT)

    # It's a string — normalise the Z suffix to +00:00 for fromisoformat
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise ValueError(f"Cannot parse timestamp: {ts!r}")

    if dt.tzinfo is None:
        raise ValueError(
            f"Naive timestamp string is not allowed (no timezone info): {ts!r}"
        )

    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime(CANONICAL_TS_FORMAT)
