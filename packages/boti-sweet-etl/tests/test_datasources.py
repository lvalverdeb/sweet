from pathlib import Path

import pytest
from boti_sweet_etl import BaseDataCube, DataHelper, Datasources


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "datasources.yaml"
    path.write_text(content)
    return path


def test_missing_file_yields_empty_catalog(tmp_path: Path) -> None:
    datasources = Datasources(tmp_path / "missing.yaml")

    with pytest.raises(KeyError):
        datasources.filesystem("etl")


def test_filesystem_profile_extracts_credentials(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        filesystems:
          source:
            fs_type: s3
            fs_path: s3://my-bucket/source
            fs_key: AKIA_EXAMPLE
            fs_secret: shh
        """,
    )

    config = Datasources(path).filesystem("source")

    assert config.fs_type == "s3"
    assert config.fs_path == "s3://my-bucket/source"
    assert config.fs_key == "AKIA_EXAMPLE"
    assert config.fs_secret is not None
    assert config.fs_secret.get_secret_value() == "shh"


def test_private_endpoint_is_trusted_not_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        filesystems:
          etl:
            fs_type: s3
            fs_path: s3://staging
            fs_endpoint: http://10.99.99.99:9000
        """,
    )

    config = Datasources(path).filesystem("etl")

    assert config.fs_endpoint == "http://10.99.99.99:9000"


def test_sql_connection_merges_shared_defaults(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        sql:
          defaults:
            pool_size: 20
            query_only: true
          connections:
            replica:
              connection_url: postgresql://user:pass@localhost/db
        """,
    )

    config = Datasources(path).sql("replica")

    assert config.pool_size == 20
    assert config.query_only is True
    assert config.connection_url.get_secret_value() == "postgresql://user:pass@localhost/db"


def test_connection_specific_override_wins_over_shared_default(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        sql:
          defaults:
            query_only: true
          connections:
            paf:
              connection_url: postgresql://user:pass@localhost/db
              query_only: false
        """,
    )

    config = Datasources(path).sql("paf")

    assert config.query_only is False


def test_malformed_filesystem_profile_names_the_profile(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        filesystems:
          etl:
            fs_type: s3
        """,
    )

    with pytest.raises(ValueError, match="filesystems.etl"):
        Datasources(path)


def test_unknown_sql_connection_raises_key_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "sql:\n  connections: {}\n")

    with pytest.raises(KeyError):
        Datasources(path).sql("replica")


def test_redis_profile_extracts_credentials(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
        redis:
          cache:
            host: 10.0.0.5
            port: 6380
            db: 2
            decode_responses: true
            password: shh
        """,
    )

    config = Datasources(path).redis("cache")

    assert config.host == "10.0.0.5"
    assert config.port == 6380
    assert config.db == 2
    assert config.decode_responses is True
    assert config.password is not None
    assert config.password.get_secret_value() == "shh"


def test_redis_profile_defaults(tmp_path: Path) -> None:
    path = _write(tmp_path, "redis:\n  cache:\n    host: 10.0.0.5\n")

    config = Datasources(path).redis("cache")

    assert config.port == 6379
    assert config.db == 0
    assert config.decode_responses is False
    assert config.password is None


def test_unknown_redis_profile_raises_key_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "redis:\n  cache: {host: 10.0.0.5}\n")

    with pytest.raises(KeyError, match="other"):
        Datasources(path).redis("other")


def test_malformed_redis_profile_names_the_profile(tmp_path: Path) -> None:
    path = _write(tmp_path, "redis:\n  cache: {port: 6379}\n")

    with pytest.raises(ValueError, match="redis.cache"):
        Datasources(path)


def _write_sqlite_profile(tmp_path: Path, name: str) -> Path:
    # query_only=False: sqlite in-memory can't enforce native read-only mode
    # (SqlDatabaseResource's own default query_only=True fails fast on it) —
    # sqlite is used here only because it needs no server and no extra driver
    # dependency, keeping this test self-contained.
    return _write(
        tmp_path,
        f"""
        sql:
          connections:
            {name}:
              connection_url: "sqlite:///:memory:"
              query_only: false
        """,
    )


def test_data_helper_builds_from_sql_profile(tmp_path: Path) -> None:
    path = _write_sqlite_profile(tmp_path, "replica")

    helper = Datasources(path).data_helper("replica")
    try:
        assert isinstance(helper, DataHelper)
    finally:
        helper.close()


def test_datacube_builds_from_sql_profile(tmp_path: Path) -> None:
    path = _write_sqlite_profile(tmp_path, "replica")

    cube = Datasources(path).datacube("replica")
    try:
        assert isinstance(cube, BaseDataCube)
    finally:
        cube.close()
