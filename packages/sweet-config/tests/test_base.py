from pathlib import Path

import pytest
from pydantic import BaseModel
from sweet_config import load_settings


class _Settings(BaseModel):
    debug: bool = False
    log_level: str = "INFO"


def test_field_defaults_apply_when_no_sources_present() -> None:
    settings = load_settings(_Settings, prefix="APP_")

    assert settings.debug is False
    assert settings.log_level == "INFO"


def test_yaml_file_overrides_field_defaults(tmp_path: Path) -> None:
    yaml_file = tmp_path / "settings.yaml"
    yaml_file.write_text("log_level: DEBUG\n")

    settings = load_settings(_Settings, prefix="APP_", yaml_file=yaml_file)

    assert settings.log_level == "DEBUG"


def test_env_file_overrides_yaml_file(tmp_path: Path) -> None:
    yaml_file = tmp_path / "settings.yaml"
    yaml_file.write_text("log_level: DEBUG\n")
    env_file = tmp_path / ".env"
    env_file.write_text("APP_LOG_LEVEL=WARNING\n")

    settings = load_settings(_Settings, prefix="APP_", yaml_file=yaml_file, env_file=env_file)

    assert settings.log_level == "WARNING"


def test_environment_variable_overrides_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("APP_LOG_LEVEL=WARNING\n")
    monkeypatch.setenv("APP_LOG_LEVEL", "ERROR")

    settings = load_settings(_Settings, prefix="APP_", env_file=env_file)

    assert settings.log_level == "ERROR"


def test_missing_yaml_file_is_treated_as_no_defaults(tmp_path: Path) -> None:
    settings = load_settings(_Settings, prefix="APP_", yaml_file=tmp_path / "missing.yaml")

    assert settings.log_level == "INFO"
