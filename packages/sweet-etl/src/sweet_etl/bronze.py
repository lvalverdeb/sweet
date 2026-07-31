"""Abstract base for bronze-layer cubes: read from a SQL source, write the
result to a parquet destination — flat or Hive-partitioned via
`partition_on`.

Composes `boti_data.datacube.BaseDataCube` (source + optional `fix_data()`
transform hook) with `boti_data.pipelines.ParquetSink` (destination); no new
read/write primitives. This is the same `self.load()` + `ParquetSink.write()`
pairing wired by hand for `CustomerCube`/`LocaliserCube` in
`sandbox/notebooks/datacubes.ipynb`, extracted once a second table needed it.

`save_to_parquet()` deliberately loads via `self.load()` rather than handing
a `DataHelper` straight to `boti_data.pipelines.SinkPipeline`: `SinkPipeline`
loads from its source directly, bypassing a `BaseDataCube` subclass's
`fix_data()` transform entirely — see `LocaliserCube`'s notebook write-up for
why that gap matters.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from boti_data.datacube import BaseDataCube
from boti_data.pipelines import ParquetSink, SinkWriteResult
from boti_data.pipelines.sinks import ParquetDestination


class BronzeCube(BaseDataCube, ABC):
    """Base for bronze-layer cubes: DB source in, parquet destination out.

    Build the same way as any other `BaseDataCube` subclass —
    `SubclassCube.from_helper(datasources.data_helper(profile, table=...,
    field_map=...))` — this class only adds `bronze_destination` (which
    subclasses must implement) and `save_to_parquet()`.
    """

    @property
    @abstractmethod
    def bronze_destination(self) -> ParquetDestination:
        """Where `save_to_parquet()` writes to.

        Anything `boti_data.pipelines.ParquetSink` accepts: a
        `{"parquet_storage_path": ..., "fs": ...}` mapping, a
        `ParquetDataConfig`, or a `ParquetReader`. A common implementation
        reads from `self.config` (populated via `from_helper(helper,
        bronze_destination=...)`'s `**overrides`), so the destination is
        supplied at construction time instead of hardcoded per subclass.
        """

    def save_to_parquet(
        self,
        *,
        partition_on: Sequence[str] | None = None,
        **load_options: Any,
    ) -> SinkWriteResult:
        """Load (through `fix_data()`) and write to `bronze_destination`.

        Pass `partition_on=[...]` for a Hive-partitioned dataset (the loaded
        frame must already contain those columns, or pass `date_field=...` —
        see `ParquetSink.write()`); leave it unset for a flat dataset
        directory.

        Defaults `return_type` to `"dask"` (`DataGateway`'s own default too,
        but stated explicitly here rather than relied on) so `fix_data()`
        implementations can assume a dask frame — e.g. use `.map_partitions()`
        — without every caller having to remember not to override it to
        `"pandas"`. Pass `return_type="pandas"` explicitly if a subclass's
        `fix_data()` genuinely needs it.
        """
        load_options.setdefault("return_type", "dask")
        df = self.load(**load_options)
        with ParquetSink(self.bronze_destination, partition_on=partition_on) as sink:
            return sink.write(df)


__all__ = ["BronzeCube"]
