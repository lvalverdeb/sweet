from pathlib import Path

import pytest

from sweet.settings import get_settings


def test_defaults_apply_when_config_dir_is_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWEET_CONFIG_DIR", str(tmp_path))

    settings = get_settings()

    assert settings.environment == "development"
    assert settings.log_level == "INFO"


def test_environment_variable_overrides_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWEET_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("SWEET_LOG_LEVEL", "DEBUG")

    settings = get_settings()

    assert settings.log_level == "DEBUG"
