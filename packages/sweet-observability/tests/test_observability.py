import logging
from pathlib import Path

import pytest
from sweet_observability import (
    NAME,
    ObservabilitySettings,
    describe,
    get_logger,
    get_observability_settings,
)


def test_name_matches_entry_point_name() -> None:
    assert NAME == "observability"


def test_settings_default() -> None:
    settings = ObservabilitySettings()

    assert settings.logger_name is None
    assert settings.enable_otel is False


def test_get_observability_settings_reads_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWEET_CONFIG_DIR", str(tmp_path))
    (tmp_path / ".env").write_text(
        "OO_LOGGER_NAME=test-logger\nOO_ENABLE_OTEL=true\nOO_OTEL_SERVICE_NAME=test-service\n"
    )

    settings = get_observability_settings()

    assert settings.logger_name == "test-logger"
    assert settings.enable_otel is True
    assert settings.otel_service_name == "test-service"


def test_describe_reports_otel_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWEET_CONFIG_DIR", str(tmp_path))

    assert "disabled" in describe()


def test_get_logger_uses_configured_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWEET_CONFIG_DIR", str(tmp_path))
    (tmp_path / ".env").write_text("OO_LOGGER_NAME=test-logger-name\n")

    logger = get_logger()

    assert logger.logger_name == "test-logger-name"


def test_get_logger_uses_configured_log_dir_file_and_level(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    log_dir = tmp_path / "logs"
    monkeypatch.setenv("SWEET_CONFIG_DIR", str(config_dir))
    (config_dir / ".env").write_text(
        f"OO_LOGGER_NAME=level-test-logger\n"
        f"OO_LOG_DIR={log_dir}\n"
        "OO_LOG_FILE=level-test.log\n"
        "OO_LOG_LEVEL=DEBUG\n"
    )

    logger = get_logger()

    assert logger.log_dir == log_dir
    assert logger.log_file == "level-test.log"
    assert logger.log_level == logging.DEBUG


def test_get_logger_level_override_takes_precedence_over_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv("SWEET_CONFIG_DIR", str(config_dir))
    (config_dir / ".env").write_text(
        f"OO_LOGGER_NAME=override-test-logger\nOO_LOG_DIR={tmp_path / 'logs'}\nOO_LOG_LEVEL=DEBUG\n"
    )

    logger = get_logger(log_level="WARNING")

    assert logger.log_level == logging.WARNING


def test_get_logger_rejects_invalid_log_level(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWEET_CONFIG_DIR", str(tmp_path))

    with pytest.raises(ValueError, match="Invalid log level"):
        get_logger(log_level="NOT_A_LEVEL")
