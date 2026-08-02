import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from boti_data.pipelines.sinks import ParquetDestination
from boti_data.watermark import FileWatermarkStore
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


def _write_watermark_config(tmp_path: Path) -> tuple[Path, Path]:
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, updated_at TEXT)")
    conn.executemany(
        "INSERT INTO events (updated_at) VALUES (?)",
        [("2026-01-01",), ("2026-01-02",)],
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
        "  events:\n"
        "    sql_profile: demo\n"
        "    table: events\n"
        "    watermark_field: updated_at\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: events\n"
        "  no_watermark:\n"
        "    sql_profile: demo\n"
        "    table: events\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: no_watermark\n"
    )
    return datasources_path, jobs_path


def test_load_incremental_kwargs_requires_watermark_field(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_watermark_config(tmp_path)
    jobs = BronzeJobs(
        jobs_path,
        Datasources(datasources_path),
        watermark_store=FileWatermarkStore(str(tmp_path / "watermarks.json")),
    )

    with pytest.raises(ValueError, match="no_watermark.*watermark_field"):
        jobs.load_incremental_kwargs("no_watermark")


def test_load_incremental_kwargs_requires_watermark_store(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_watermark_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))  # no watermark_store

    with pytest.raises(ValueError, match="watermark_store"):
        jobs.load_incremental_kwargs("events")


def test_load_incremental_kwargs_shape(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_watermark_config(tmp_path)
    store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path), watermark_store=store)

    kwargs = jobs.load_incremental_kwargs("events")

    assert kwargs == {
        "watermark_field": "updated_at",
        "watermark_source": "events",
        "watermark_store": store,
        "operator": "gt",
        "initial_value": None,
    }


def test_load_incremental_only_returns_rows_past_the_committed_watermark(
    tmp_path: Path,
) -> None:
    datasources_path, jobs_path = _write_watermark_config(tmp_path)
    datasources = Datasources(datasources_path)
    store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
    jobs = BronzeJobs(jobs_path, datasources, watermark_store=store)

    helper = datasources.data_helper("demo", **jobs.data_helper_kwargs("events"))
    try:
        first = helper.load_incremental(
            **jobs.load_incremental_kwargs("events"), return_type="pandas"
        )
        assert len(first.frame) == 2  # both existing rows, no watermark committed yet
        assert first.current_watermark == "2026-01-02"
        assert store.read(source="events") == "2026-01-02"

        # Re-running with no new rows returns nothing.
        second = helper.load_incremental(
            **jobs.load_incremental_kwargs("events"), return_type="pandas"
        )
        assert len(second.frame) == 0
    finally:
        helper.close()

    # A genuinely new row lands...
    conn = sqlite3.connect(tmp_path / "events.db")
    conn.execute("INSERT INTO events (updated_at) VALUES ('2026-01-03')")
    conn.commit()
    conn.close()

    helper = datasources.data_helper("demo", **jobs.data_helper_kwargs("events"))
    try:
        third = helper.load_incremental(
            **jobs.load_incremental_kwargs("events"), return_type="pandas"
        )
        # ...and only that row comes back.
        assert len(third.frame) == 1
        assert third.frame["updated_at"].tolist() == ["2026-01-03"]
        assert store.read(source="events") == "2026-01-03"
    finally:
        helper.close()


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
