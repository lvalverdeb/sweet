import sqlite3
from pathlib import Path
from typing import Any

import fsspec
import pandas as pd
import pytest
from boti_data.pipelines import ParquetSink
from boti_data.pipelines.sinks import ParquetDestination
from boti_data.watermark import FileWatermarkStore
from sweet_etl import BronzeCube, Datasources


def _write_orders_profile(tmp_path: Path, name: str = "demo") -> Path:
    db_path = tmp_path / "orders.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, amount REAL)")
    conn.executemany(
        "INSERT INTO orders (status, amount) VALUES (?, ?)",
        [("active", 100.0), ("active", 250.5), ("cancelled", 40.0)],
    )
    conn.commit()
    conn.close()

    yaml_path = tmp_path / "datasources.yaml"
    yaml_path.write_text(
        "sql:\n"
        "  connections:\n"
        f"    {name}:\n"
        f'      connection_url: "sqlite:///{db_path}"\n'
        "      query_only: false\n"
    )
    return yaml_path


class _OrdersBronzeCube(BronzeCube):
    def fix_data(self, **kwargs: Any) -> None:
        self.df = self.df[self.df["status"] == "active"]

    @property
    def bronze_destination(self) -> ParquetDestination:
        return self.config["bronze_destination"]


def _build_cube(tmp_path: Path, destination_dir: Path) -> _OrdersBronzeCube:
    datasources = Datasources(_write_orders_profile(tmp_path))
    helper = datasources.data_helper("demo", table="orders")
    return _OrdersBronzeCube.from_helper(
        helper,
        bronze_destination={
            "parquet_storage_path": str(destination_dir),
            "fs": fsspec.filesystem("file"),
        },
    )


class _DaskAssumingBronzeCube(BronzeCube):
    """fix_data() only works on a dask frame — proves save_to_parquet()'s
    return_type="dask" default without the caller having to ask for it."""

    def fix_data(self, **kwargs: Any) -> None:
        self.df = self.df.map_partitions(
            lambda part: part.assign(amount_doubled=part["amount"] * 2)
        )

    @property
    def bronze_destination(self) -> ParquetDestination:
        return self.config["bronze_destination"]


def test_save_to_parquet_defaults_to_dask_for_map_partitions(tmp_path: Path) -> None:
    destination_dir = tmp_path / "bronze" / "orders_dask"
    datasources = Datasources(_write_orders_profile(tmp_path))
    helper = datasources.data_helper("demo", table="orders")
    cube = _DaskAssumingBronzeCube.from_helper(
        helper,
        bronze_destination={
            "parquet_storage_path": str(destination_dir),
            "fs": fsspec.filesystem("file"),
        },
    )
    try:
        cube.save_to_parquet()  # no return_type override — must still be dask

        written = pd.read_parquet(destination_dir)
        assert (written["amount_doubled"] == written["amount"] * 2).all()
    finally:
        cube.close()


def test_bronze_cube_cannot_be_instantiated_without_bronze_destination() -> None:
    class _Incomplete(BronzeCube):
        pass

    with pytest.raises(TypeError, match="bronze_destination"):
        _Incomplete.__new__(_Incomplete)


def test_save_to_parquet_writes_flat_dataset_through_fix_data(tmp_path: Path) -> None:
    destination_dir = tmp_path / "bronze" / "orders"
    cube = _build_cube(tmp_path, destination_dir)
    try:
        result = cube.save_to_parquet(return_type="pandas")

        assert result.path == str(destination_dir)
        written = pd.read_parquet(result.path)
        # fix_data() filters to status == "active" before the write happens.
        assert set(written["status"]) == {"active"}
        assert len(written) == 2
    finally:
        cube.close()


def test_save_to_parquet_partitions_when_requested(tmp_path: Path) -> None:
    destination_dir = tmp_path / "bronze" / "orders_partitioned"
    cube = _build_cube(tmp_path, destination_dir)
    try:
        cube.save_to_parquet(return_type="pandas", partition_on=["status"])

        assert (destination_dir / "status=active").is_dir()
    finally:
        cube.close()


def _write_events_db(db_path: Path, rows: list[tuple[str]]) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, updated_at TEXT)")
    conn.executemany("INSERT INTO events (updated_at) VALUES (?)", rows)
    conn.commit()
    conn.close()


def _write_events_profile(tmp_path: Path) -> Path:
    db_path = tmp_path / "events.db"
    _write_events_db(db_path, [("2026-01-01",), ("2026-01-02",)])

    yaml_path = tmp_path / "datasources.yaml"
    yaml_path.write_text(
        "sql:\n  connections:\n    demo:\n"
        f'      connection_url: "sqlite:///{db_path}"\n'
        "      query_only: false\n"
    )
    return yaml_path


class _EventsBronzeCube(BronzeCube):
    @property
    def bronze_destination(self) -> ParquetDestination:
        return self.config["bronze_destination"]


def _build_events_cube(tmp_path: Path, destination_dir: Path) -> _EventsBronzeCube:
    datasources = Datasources(_write_events_profile(tmp_path))
    helper = datasources.data_helper("demo", table="events")
    return _EventsBronzeCube.from_helper(
        helper,
        bronze_destination={
            "parquet_storage_path": str(destination_dir),
            "fs": fsspec.filesystem("file"),
        },
    )


def test_save_to_parquet_history_writes_new_partition_and_commits_watermark(
    tmp_path: Path,
) -> None:
    destination_dir = tmp_path / "bronze" / "events"
    cube = _build_events_cube(tmp_path, destination_dir)
    store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
    try:
        result = cube.save_to_parquet_history(
            watermark_field="updated_at",
            watermark_source="events",
            watermark_store=store,
            partition_value="2026-02-01",
        )

        assert result is not None
        written = pd.read_parquet(destination_dir)
        assert len(written) == 2
        assert set(written["extracted_on"]) == {"2026-02-01"}
        assert store.read(source="events") == "2026-01-02"
    finally:
        cube.close()


def test_save_to_parquet_history_returns_none_when_no_new_rows(tmp_path: Path) -> None:
    destination_dir = tmp_path / "bronze" / "events"
    store = FileWatermarkStore(str(tmp_path / "watermarks.json"))

    cube = _build_events_cube(tmp_path, destination_dir)
    try:
        cube.save_to_parquet_history(
            watermark_field="updated_at",
            watermark_source="events",
            watermark_store=store,
            partition_value="2026-02-01",
        )
    finally:
        cube.close()

    cube2 = _build_events_cube(tmp_path, destination_dir)
    try:
        result = cube2.save_to_parquet_history(
            watermark_field="updated_at",
            watermark_source="events",
            watermark_store=store,
            partition_value="2026-02-02",
        )

        assert result is None
        assert not (destination_dir / "extracted_on=2026-02-02").exists()
    finally:
        cube2.close()


def test_save_to_parquet_history_appends_across_multiple_runs(tmp_path: Path) -> None:
    destination_dir = tmp_path / "bronze" / "events"
    store = FileWatermarkStore(str(tmp_path / "watermarks.json"))

    cube = _build_events_cube(tmp_path, destination_dir)
    try:
        cube.save_to_parquet_history(
            watermark_field="updated_at",
            watermark_source="events",
            watermark_store=store,
            partition_value="2026-02-01",
        )
    finally:
        cube.close()

    # A new row lands after the first run.
    _write_events_db(tmp_path / "events.db", [("2026-01-03",)])

    cube2 = _build_events_cube(tmp_path, destination_dir)
    try:
        result = cube2.save_to_parquet_history(
            watermark_field="updated_at",
            watermark_source="events",
            watermark_store=store,
            partition_value="2026-02-02",
        )
    finally:
        cube2.close()

    assert result is not None
    written = pd.read_parquet(destination_dir)
    # Both partitions survive: 2 rows from run 1, 1 new row from run 2.
    assert len(written) == 3
    assert set(written["extracted_on"]) == {"2026-02-01", "2026-02-02"}
    assert store.read(source="events") == "2026-01-03"


def test_save_to_parquet_history_raises_on_existing_partition(tmp_path: Path) -> None:
    destination_dir = tmp_path / "bronze" / "events"
    store = FileWatermarkStore(str(tmp_path / "watermarks.json"))

    destination_dir.mkdir(parents=True)
    (destination_dir / "extracted_on=2026-02-01").mkdir()

    cube = _build_events_cube(tmp_path, destination_dir)
    try:
        with pytest.raises(FileExistsError, match="extracted_on=2026-02-01"):
            cube.save_to_parquet_history(
                watermark_field="updated_at",
                watermark_source="events",
                watermark_store=store,
                partition_value="2026-02-01",
            )
    finally:
        cube.close()


def test_save_to_parquet_history_does_not_commit_watermark_when_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination_dir = tmp_path / "bronze" / "events"
    store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
    cube = _build_events_cube(tmp_path, destination_dir)

    def _boom(self: ParquetSink, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr(ParquetSink, "write", _boom)

    try:
        with pytest.raises(RuntimeError, match="simulated write failure"):
            cube.save_to_parquet_history(
                watermark_field="updated_at",
                watermark_source="events",
                watermark_store=store,
                partition_value="2026-02-01",
            )

        # The load already computed the advanced watermark internally, but
        # the write never landed — the watermark must NOT have moved, or
        # these rows would never be re-extracted on the next run.
        assert store.read(source="events") is None
    finally:
        cube.close()
