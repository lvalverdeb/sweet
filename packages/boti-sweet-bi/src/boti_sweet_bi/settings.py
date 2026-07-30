"""ClickHouse settings: BI's first real (if minimal) capability.

Generic BI infrastructure — unlike this package's client-specific
counterparts (routing, security, ...), which stay at the deployment level
(see the sandbox), ClickHouse is a plausible backend for any BI deployment.
No ClickHouse client dependency is added here, only the typed settings shape.
"""

from __future__ import annotations

import os
from pathlib import Path

from boti_sweet_config import load_settings
from pydantic import BaseModel, SecretStr

ENV_PREFIX = "CLICKHOUSE_"


def _config_dir() -> Path:
    return Path(os.environ.get("BOTI_SWEET_CONFIG_DIR", "config"))


class ClickHouseSettings(BaseModel):
    host: str | None = None
    port: int = 8123
    database: str = "default"
    user: str = "default"
    password: SecretStr | None = None
    secure: bool = False
    verify: bool = True


def get_clickhouse_settings() -> ClickHouseSettings:
    config_dir = _config_dir()
    return load_settings(
        ClickHouseSettings,
        prefix=ENV_PREFIX,
        yaml_file=config_dir / "clickhouse.yaml",
        env_file=config_dir / ".env",
    )
