import os
from cortex.core.config import CortexSettings


def test_default_settings():
    settings = CortexSettings()
    assert settings.host == "0.0.0.0"
    assert settings.port == 8742
    assert settings.log_level == "INFO"
    assert str(settings.db_path).endswith("cortex.db")


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("CORTEX_HOST", "127.0.0.1")
    monkeypatch.setenv("CORTEX_PORT", "9999")
    monkeypatch.setenv("CORTEX_DB_PATH", "/tmp/test.db")
    monkeypatch.setenv("CORTEX_LOG_LEVEL", "DEBUG")
    settings = CortexSettings()
    assert settings.host == "127.0.0.1"
    assert settings.port == 9999
    assert str(settings.db_path) == "/tmp/test.db"
    assert settings.log_level == "DEBUG"
