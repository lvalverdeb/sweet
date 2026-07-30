from pathlib import Path

import pytest
from boti_sweet_observability import (
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
    monkeypatch.setenv("BOTI_SWEET_CONFIG_DIR", str(tmp_path))
    (tmp_path / ".env").write_text(
        "OO_LOGGER_NAME=test-logger\nOO_ENABLE_OTEL=true\nOO_OTEL_SERVICE_NAME=test-service\n"
    )

    settings = get_observability_settings()

    assert settings.logger_name == "test-logger"
    assert settings.enable_otel is True
    assert settings.otel_service_name == "test-service"


def test_describe_reports_otel_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BOTI_SWEET_CONFIG_DIR", str(tmp_path))

    assert "disabled" in describe()


def test_get_logger_uses_configured_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BOTI_SWEET_CONFIG_DIR", str(tmp_path))
    (tmp_path / ".env").write_text("OO_LOGGER_NAME=test-logger-name\n")

    logger = get_logger()

    assert logger.logger_name == "test-logger-name"
