from pathlib import Path

from pydantic_settings import BaseSettings


class CortexSettings(BaseSettings):
    model_config = {"env_prefix": "CORTEX_"}

    host: str = "0.0.0.0"
    port: int = 8742
    db_path: Path = Path.home() / ".cortex" / "cortex.db"
    log_level: str = "INFO"
