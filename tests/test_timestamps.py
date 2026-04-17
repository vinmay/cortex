from datetime import datetime, timezone, timedelta

import pytest

from cortex.core.timestamps import CANONICAL_TS_FORMAT, now_utc, canonicalize


class TestNowUtc:
    def test_returns_string(self):
        ts = now_utc()
        assert isinstance(ts, str)

    def test_ends_with_z(self):
        ts = now_utc()
        assert ts.endswith("Z")

    def test_parseable_by_canonical_format(self):
        ts = now_utc()
        dt = datetime.strptime(ts, CANONICAL_TS_FORMAT)
        assert dt is not None

    def test_has_microseconds(self):
        ts = now_utc()
        # canonical format is %Y-%m-%dT%H:%M:%S.%fZ — 6 microsecond digits
        assert "." in ts


class TestCanonicalize:
    def test_z_suffix(self):
        assert canonicalize("2026-04-13T10:00:00Z") == "2026-04-13T10:00:00.000000Z"

    def test_plus_zero_offset(self):
        assert canonicalize("2026-04-13T10:00:00+00:00") == "2026-04-13T10:00:00.000000Z"

    def test_microseconds_preserved(self):
        assert canonicalize("2026-04-13T10:00:00.123456Z") == "2026-04-13T10:00:00.123456Z"

    def test_canonical_round_trips(self):
        canonical = "2026-04-13T10:00:00.000000Z"
        assert canonicalize(canonical) == canonical

    def test_non_zero_offset_converted_to_utc(self):
        # +01:00 offset => subtract 1 hour
        result = canonicalize("2026-04-13T11:00:00+01:00")
        assert result == "2026-04-13T10:00:00.000000Z"

    def test_datetime_aware_accepted(self):
        dt = datetime(2026, 4, 13, 10, 0, 0, tzinfo=timezone.utc)
        assert canonicalize(dt) == "2026-04-13T10:00:00.000000Z"

    def test_datetime_with_offset_accepted(self):
        tz = timezone(timedelta(hours=5, minutes=30))
        dt = datetime(2026, 4, 13, 15, 30, 0, tzinfo=tz)
        assert canonicalize(dt) == "2026-04-13T10:00:00.000000Z"

    def test_naive_string_rejected(self):
        with pytest.raises(ValueError, match="Naive"):
            canonicalize("2026-04-13T10:00:00")  # no timezone

    def test_naive_datetime_rejected(self):
        dt = datetime(2026, 4, 13, 10, 0, 0)  # no tzinfo
        with pytest.raises(ValueError, match="Naive"):
            canonicalize(dt)

    def test_unparseable_string_rejected(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            canonicalize("not-a-timestamp")

    def test_z_vs_plus00_normalization(self):
        """Z and +00:00 must produce the same output."""
        assert canonicalize("2026-04-13T10:00:00Z") == canonicalize("2026-04-13T10:00:00+00:00")
