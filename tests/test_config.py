from pathlib import Path

import pytest

from cortex.core.config import CortexSettings


def test_default_settings():
    settings = CortexSettings()
    # H2: default host is now 127.0.0.1, not 0.0.0.0
    assert settings.host == "127.0.0.1"
    assert settings.port == 8742
    assert settings.log_level == "INFO"
    assert str(settings.db_path).endswith("cortex.db")


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("CORTEX_HOST", "0.0.0.0")
    monkeypatch.setenv("CORTEX_PORT", "9999")
    monkeypatch.setenv("CORTEX_DB_PATH", "/tmp/test.db")
    monkeypatch.setenv("CORTEX_LOG_LEVEL", "DEBUG")
    settings = CortexSettings()
    assert settings.host == "0.0.0.0"
    assert settings.port == 9999
    assert str(settings.db_path) == "/tmp/test.db"
    assert settings.log_level == "DEBUG"


def test_db_path_is_absolute():
    """db_path must always be an absolute path after validation."""
    settings = CortexSettings()
    assert settings.db_path.is_absolute()


def test_db_path_from_env_is_absolute(monkeypatch):
    monkeypatch.setenv("CORTEX_DB_PATH", "/tmp/cortex-test.db")
    settings = CortexSettings()
    assert settings.db_path.is_absolute()
    assert str(settings.db_path) == "/tmp/cortex-test.db"


def test_extra_fields_ignored(monkeypatch):
    """H8: extra env vars must be silently ignored (extra='ignore')."""
    monkeypatch.setenv("CORTEX_UNKNOWN_FIELD", "should-be-ignored")
    # Should not raise
    settings = CortexSettings()
    assert not hasattr(settings, "unknown_field")
