import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from boti_data.pipelines.sinks import ParquetDestination
from sweet_etl import BronzeCube, BronzeJobs, Datasources


def _write_config(tmp_path: Path) -> tuple[Path, Path]:
    db_path = tmp_path / "orders.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, amount REAL)")
    conn.executemany(
        "INSERT INTO orders (status, amount) VALUES (?, ?)",
        [("active", 100.0), ("active", 250.5), ("cancelled", 40.0)],
    )
    conn.commit()
    conn.close()

    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()

    datasources_path = tmp_path / "datasources.yaml"
    datasources_path.write_text(
        "filesystems:\n"
        "  etl:\n"
        "    fs_type: file\n"
        f"    fs_path: {bronze_dir}\n"
        "sql:\n"
        "  connections:\n"
        "    demo:\n"
        f'      connection_url: "sqlite:///{db_path}"\n'
        "      query_only: false\n"
    )

    jobs_path = tmp_path / "bronze_jobs.yaml"
    jobs_path.write_text(
        "jobs:\n"
        "  orders:\n"
        "    sql_profile: demo\n"
        "    table: orders\n"
        "    sticky_filters:\n"
        "      status: active\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: orders\n"
        "  nested_orders:\n"
        "    sql_profile: demo\n"
        "    table: orders\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: support/orders\n"
    )
    return datasources_path, jobs_path


class _OrdersBronzeCube(BronzeCube):
    def fix_data(self, **kwargs: Any) -> None:
        self.df = self.df.assign(amount_doubled=self.df["amount"] * 2)

    @property
    def bronze_destination(self) -> ParquetDestination:
        return self.config["bronze_destination"]


def test_job_resolves_configured_fields(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    job = jobs.job("orders")

    assert job.sql_profile == "demo"
    assert job.table == "orders"
    assert job.sticky_filters == {"status": "active"}
    assert job.destination.filesystem_profile == "etl"
    assert job.destination.path == "orders"


def test_unknown_job_raises_key_error(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    with pytest.raises(KeyError, match="other"):
        jobs.job("other")


def test_malformed_job_names_the_job(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs_path.write_text("jobs:\n  broken:\n    sql_profile: demo\n")  # missing required fields

    with pytest.raises(ValueError, match="jobs.broken"):
        BronzeJobs(jobs_path, Datasources(datasources_path))


def test_bronze_destination_joins_filesystem_storage_path_and_job_path(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    destination = jobs.bronze_destination("orders")

    assert destination["parquet_storage_path"] == str(tmp_path / "bronze" / "orders")


def test_bronze_destination_supports_nested_path(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    destination = jobs.bronze_destination("nested_orders")

    assert destination["parquet_storage_path"] == str(tmp_path / "bronze" / "support" / "orders")


def test_save_to_parquet_creates_missing_nested_directories(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    datasources = Datasources(datasources_path)
    jobs = BronzeJobs(jobs_path, datasources)

    helper = datasources.data_helper("demo", **jobs.data_helper_kwargs("nested_orders"))
    cube = _OrdersBronzeCube.from_helper(
        helper, bronze_destination=jobs.bronze_destination("nested_orders")
    )
    try:
        result = cube.save_to_parquet(return_type="pandas")

        assert result.path == str(tmp_path / "bronze" / "support" / "orders")
        assert pd.read_parquet(result.path).shape[0] == 3
    finally:
        cube.close()


def test_build_and_write_bronze_cube_from_job_config(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    datasources = Datasources(datasources_path)
    jobs = BronzeJobs(jobs_path, datasources)

    helper = datasources.data_helper("demo", **jobs.data_helper_kwargs("orders"))
    cube = _OrdersBronzeCube.from_helper(
        helper, bronze_destination=jobs.bronze_destination("orders")
    )
    try:
        job = jobs.job("orders")
        result = cube.save_to_parquet(return_type="pandas", partition_on=job.partition_on)

        written = pd.read_parquet(result.path)
        # sticky_filters={"status": "active"} applied via data_helper_kwargs().
        assert set(written["status"]) == {"active"}
        assert (written["amount_doubled"] == written["amount"] * 2).all()
    finally:
        cube.close()
