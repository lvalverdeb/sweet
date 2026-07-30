from pathlib import Path

import pytest
from boti_sweet_bi import NAME, ClickHouseSettings, describe, get_clickhouse_settings


def test_describe_reports_installed() -> None:
    assert "installed" in describe()


def test_name_matches_entry_point_name() -> None:
    assert NAME == "bi"


def test_clickhouse_settings_default() -> None:
    settings = ClickHouseSettings()

    assert settings.host is None
    assert settings.port == 8123
    assert settings.secure is False


def test_get_clickhouse_settings_reads_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BOTI_SWEET_CONFIG_DIR", str(tmp_path))
    (tmp_path / ".env").write_text(
        "CLICKHOUSE_HOST=clickhouse.example\nCLICKHOUSE_DATABASE=analytics\n"
    )

    settings = get_clickhouse_settings()

    assert settings.host == "clickhouse.example"
    assert settings.database == "analytics"
    assert settings.password is None
