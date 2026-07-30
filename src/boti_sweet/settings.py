"""Global settings for the boti-sweet suite, shared across all installed packages."""

from __future__ import annotations

import os
from pathlib import Path

from boti_sweet_config import load_settings
from pydantic import BaseModel

ENV_PREFIX = "BOTI_SWEET_"


def _config_dir() -> Path:
    return Path(os.environ.get("BOTI_SWEET_CONFIG_DIR", "config"))


class SuiteSettings(BaseModel):
    """Suite-wide settings, layered from `config/settings.yaml` and `config/.env`."""

    environment: str = "development"
    log_level: str = "INFO"


def get_settings() -> SuiteSettings:
    config_dir = _config_dir()
    return load_settings(
        SuiteSettings,
        prefix=ENV_PREFIX,
        yaml_file=config_dir / "settings.yaml",
        env_file=config_dir / ".env",
    )
