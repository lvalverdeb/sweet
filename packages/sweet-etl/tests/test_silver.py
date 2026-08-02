from pathlib import Path

import dask.dataframe as dd
import fsspec
import pandas as pd
import pytest
from boti_data.enrichment import Deduplicator, RowFilter, TypeCaster
from boti_data.parquet import ParquetReader
from boti_data.pipelines.sinks import ParquetDestination
from sweet_etl import SilverCube


class _OrdersSilverCube(SilverCube):
    transformers = [
        RowFilter(["status == 'active'"]),
        TypeCaster({"amount": "float64"}),
        Deduplicator(subset=["id"], keep="first"),
    ]

    @property
    def silver_destination(self) -> ParquetDestination:
        return self.config["silver_destination"]


class _DaskSafeOrdersSilverCube(SilverCube):
    """Same cleaning intent as `_OrdersSilverCube`, minus `TypeCaster` —
    `RowFilter`/`Deduplicator` are the two transformers verified to stay
    lazy end-to-end on a `dd.DataFrame` (see `silver.py`'s module
    docstring)."""

    transformers = [
        RowFilter(["status == 'active'"]),
        Deduplicator(subset=["id"], keep="first"),
    ]

    @property
    def silver_destination(self) -> ParquetDestination:
        return self.config["silver_destination"]


def _write_bronze_parquet(bronze_dir: Path) -> None:
    bronze_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "id": [1, 1, 2, 3],
            "status": ["active", "active", "active", "cancelled"],
            "amount": [100.0, 100.0, 250.5, 40.0],
        }
    )
    df.to_parquet(bronze_dir / "part-0.parquet", engine="pyarrow")


def _build_cube(bronze_dir: Path, silver_dir: Path) -> _OrdersSilverCube:
    reader = ParquetReader(parquet_storage_path=str(bronze_dir), fs=fsspec.filesystem("file"))
    return _OrdersSilverCube.from_helper(
        reader,
        silver_destination={
            "parquet_storage_path": str(silver_dir),
            "fs": fsspec.filesystem("file"),
        },
    )


def _build_dask_safe_cube(bronze_dir: Path, silver_dir: Path) -> _DaskSafeOrdersSilverCube:
    reader = ParquetReader(parquet_storage_path=str(bronze_dir), fs=fsspec.filesystem("file"))
    return _DaskSafeOrdersSilverCube.from_helper(
        reader,
        silver_destination={
            "parquet_storage_path": str(silver_dir),
            "fs": fsspec.filesystem("file"),
        },
    )


def test_silver_cube_cannot_be_instantiated_without_silver_destination() -> None:
    class _Incomplete(SilverCube):
        pass

    with pytest.raises(TypeError, match="silver_destination"):
        _Incomplete.__new__(_Incomplete)


async def test_afix_data_rejects_arrow_and_polars(tmp_path: Path) -> None:
    """pd.DataFrame/dd.DataFrame are both accepted (this package is
    dask-first by default); pa.Table/pl.DataFrame are rejected up front with
    a clear message, since RowFilter/DerivedColumn's `.eval()` calls have no
    equivalent on either and would otherwise fail deep inside
    boti_data.enrichment with a much less legible AttributeError."""
    bronze_dir = tmp_path / "bronze" / "orders"
    _write_bronze_parquet(bronze_dir)
    silver_dir = tmp_path / "silver" / "orders"
    cube = _build_cube(bronze_dir, silver_dir)
    try:
        with pytest.raises(TypeError, match="return_type='pandas' or 'dask'"):
            await cube.save_to_parquet(return_type="arrow")
    finally:
        cube.close()


async def test_typecaster_breaks_on_dask_return_type(tmp_path: Path) -> None:
    """Empirically verified upstream gap (not a sweet_etl restriction):
    dd.DataFrame.astype() doesn't accept the errors= kwarg TypeCaster.
    transform() always passes, so any transformers list containing a
    TypeCaster still fails on a dask frame — SilverCube no longer blocks
    dask pre-emptively, but this specific transformer still breaks it. See
    silver.py's module docstring."""
    bronze_dir = tmp_path / "bronze" / "orders"
    _write_bronze_parquet(bronze_dir)
    silver_dir = tmp_path / "silver" / "orders"
    cube = _build_cube(bronze_dir, silver_dir)
    try:
        with pytest.raises(TypeError, match="errors"):
            await cube.save_to_parquet(return_type="dask")
    finally:
        cube.close()


async def test_save_to_parquet_runs_composite_transformer_pipeline(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze" / "orders"
    _write_bronze_parquet(bronze_dir)
    silver_dir = tmp_path / "silver" / "orders"
    cube = _build_cube(bronze_dir, silver_dir)
    try:
        result = await cube.save_to_parquet(return_type="pandas")

        written = pd.read_parquet(result.path)
        # RowFilter dropped the cancelled row, Deduplicator collapsed the
        # duplicate id=1 row, TypeCaster left amount as float64.
        assert set(written["id"]) == {1, 2}
        assert len(written) == 2
        assert written["amount"].dtype == "float64"
    finally:
        cube.close()


async def test_save_to_parquet_stays_lazy_on_dask_return_type(tmp_path: Path) -> None:
    """The decisive dask-first check: RowFilter/Deduplicator (no TypeCaster)
    run against a dd.DataFrame without ever calling .compute() before
    ParquetSink.write() does its own internal materialization — proving
    afix_data() doesn't silently force an eager pandas conversion anywhere
    in its own code path."""
    bronze_dir = tmp_path / "bronze" / "orders"
    _write_bronze_parquet(bronze_dir)
    silver_dir = tmp_path / "silver" / "orders"
    cube = _build_dask_safe_cube(bronze_dir, silver_dir)
    try:
        df = await cube.aload(return_type="dask")
        assert isinstance(df, dd.DataFrame), (
            f"afix_data() must not eagerly convert to pandas; got {type(df).__name__}"
        )

        result = await cube.save_to_parquet(return_type="dask")
        written = pd.read_parquet(result.path)
        assert set(written["id"]) == {1, 2}
        assert len(written) == 2
    finally:
        cube.close()
