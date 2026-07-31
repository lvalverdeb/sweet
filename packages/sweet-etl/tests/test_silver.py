from pathlib import Path

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


def test_silver_cube_cannot_be_instantiated_without_silver_destination() -> None:
    class _Incomplete(SilverCube):
        pass

    with pytest.raises(TypeError, match="silver_destination"):
        _Incomplete.__new__(_Incomplete)


async def test_afix_data_requires_pandas_return_type(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze" / "orders"
    _write_bronze_parquet(bronze_dir)
    silver_dir = tmp_path / "silver" / "orders"
    cube = _build_cube(bronze_dir, silver_dir)
    try:
        with pytest.raises(TypeError, match="return_type='pandas'"):
            await cube.save_to_parquet(return_type="dask")
    finally:
        cube.close()


async def test_save_to_parquet_runs_composite_transformer_pipeline(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze" / "orders"
    _write_bronze_parquet(bronze_dir)
    silver_dir = tmp_path / "silver" / "orders"
    cube = _build_cube(bronze_dir, silver_dir)
    try:
        result = await cube.save_to_parquet()

        written = pd.read_parquet(result.path)
        # RowFilter dropped the cancelled row, Deduplicator collapsed the
        # duplicate id=1 row, TypeCaster left amount as float64.
        assert set(written["id"]) == {1, 2}
        assert len(written) == 2
        assert written["amount"].dtype == "float64"
    finally:
        cube.close()
