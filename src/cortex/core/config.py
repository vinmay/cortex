from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CortexSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CORTEX_", extra="ignore")

    # H2: default to localhost — do not bind to 0.0.0.0 by default.
    host: str = "127.0.0.1"
    port: int = 8742
    db_path: Path = Path.home() / ".cortex" / "cortex.db"
    log_level: str = "INFO"

    @field_validator("db_path", mode="before")
    @classmethod
    def validate_db_path(cls, v: object) -> Path:
        """Resolve to absolute path and reject traversal components."""
        p = Path(str(v)).expanduser().resolve()
        # Reject paths that still contain '..' after resolution (should be
        # impossible after resolve(), but belt-and-suspenders).
        for part in p.parts:
            if part == "..":
                raise ValueError(
                    f"db_path must not contain '..' components after resolution: {p}"
                )
        return p


settings = CortexSettings()
