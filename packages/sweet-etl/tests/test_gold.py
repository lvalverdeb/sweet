import sqlite3
from pathlib import Path
from typing import Any

import fsspec
import pandas as pd
import pytest
from boti_data.dataset import HybridDataset
from boti_data.db.sql_config import SqlDatabaseConfig
from boti_data.enrichment import AttachmentSpec
from boti_data.helper import DataHelper
from boti_data.parquet import ParquetReader
from boti_data.pipelines import ParquetSink
from boti_data.pipelines.sinks import ParquetDestination
from sweet_etl import GoldCube


def _write_orders(tmp_path: Path) -> tuple[Path, Path]:
    bronze_dir = tmp_path / "bronze" / "orders"
    historical = pd.DataFrame(
        {
            "order_id": [1, 2],
            "customer_id": ["c1", "c2"],
            "order_date": ["2026-01-01", "2026-01-02"],
            "amount": [100.0, 200.0],
        }
    )
    with ParquetSink(
        {"parquet_storage_path": str(bronze_dir), "fs": fsspec.filesystem("file")},
        partition_on=None,
    ) as sink:
        sink.write(historical, overwrite=True)

    db_path = tmp_path / "orders.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE orders (order_id INTEGER, customer_id TEXT, order_date TEXT, amount REAL)"
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [
            (1, "c1", "2026-01-01", 100.0),
            (2, "c2", "2026-01-02", 200.0),
            (3, "c3", "2026-01-05", 300.0),
        ],
    )
    conn.commit()
    conn.close()
    return bronze_dir, db_path


def _write_customers(tmp_path: Path) -> Path:
    customers_dir = tmp_path / "bronze" / "customers"
    customers = pd.DataFrame(
        {"customer_id": ["c1", "c2", "c3", "c4"], "name": ["Alice", "Bob", "Carol", "Dave"]}
    )
    with ParquetSink(
        {"parquet_storage_path": str(customers_dir), "fs": fsspec.filesystem("file")},
        partition_on=None,
    ) as sink:
        sink.write(customers, overwrite=True)
    return customers_dir


class _OrdersGoldCube(GoldCube):
    @property
    def gold_destination(self) -> ParquetDestination:
        return self.config["gold_destination"]


async def test_save_to_parquet_combines_hybrid_load_and_enrichment(tmp_path: Path) -> None:
    orders_bronze_dir, db_path = _write_orders(tmp_path)
    customers_dir = _write_customers(tmp_path)

    async def fetch_customers(customer_id: list[Any]) -> pd.DataFrame:
        reader = ParquetReader(
            parquet_storage_path=str(customers_dir), fs=fsspec.filesystem("file")
        )
        try:
            return await reader.aload(
                filters={"customer_id__in": customer_id}, return_type="pandas"
            )
        finally:
            reader.close()

    spec = AttachmentSpec(
        key="customers",
        required_cols={"customer_id"},
        attachment_fn=fetch_customers,
        col_to_kwarg={"customer_id": "customer_id"},
        left_on=["customer_id"],
        right_on=["customer_id"],
        drop_cols=[],
    )

    historical = ParquetReader(
        parquet_storage_path=str(orders_bronze_dir), fs=fsspec.filesystem("file")
    )
    live = DataHelper(
        SqlDatabaseConfig(connection_url=f"sqlite:///{db_path}", query_only=False),
        table="orders",
    )
    hybrid = HybridDataset(historical, live, date_field="order_date", split_date="2026-01-03")

    gold_dir = tmp_path / "gold" / "orders"
    cube = _OrdersGoldCube.from_hybrid(
        hybrid,
        [spec],
        gold_destination={
            "parquet_storage_path": str(gold_dir),
            "fs": fsspec.filesystem("file"),
        },
    )
    try:
        result = await cube.save_to_parquet(start="2026-01-01", end="2026-01-05")

        written = pd.read_parquet(result.path)
        assert len(written) == 3  # 2 historical + 1 live
        assert set(zip(written["customer_id"], written["name"], strict=True)) == {
            ("c1", "Alice"),
            ("c2", "Bob"),
            ("c3", "Carol"),
        }
    finally:
        cube.close()


async def test_gold_cube_cannot_be_instantiated_without_gold_destination(
    tmp_path: Path,
) -> None:
    class _Incomplete(GoldCube):
        pass

    orders_bronze_dir, db_path = _write_orders(tmp_path)
    historical = ParquetReader(
        parquet_storage_path=str(orders_bronze_dir), fs=fsspec.filesystem("file")
    )
    live = DataHelper(
        SqlDatabaseConfig(connection_url=f"sqlite:///{db_path}", query_only=False),
        table="orders",
    )
    hybrid = HybridDataset(historical, live, date_field="order_date", split_date="2026-01-03")

    with pytest.raises(TypeError, match="gold_destination"):
        _Incomplete.__new__(_Incomplete)

    hybrid.close()
