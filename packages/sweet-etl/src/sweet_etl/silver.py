"""Abstract base for silver-layer cubes: read a bronze parquet dataset, run it
through a declarative `boti_data.enrichment.CompositeTransformer` pipeline,
write the cleaned result to a parquet destination.

Same shape as `BronzeCube`, one layer up: `BronzeCube.fix_data()` hand-writes
ad hoc pandas per subclass; `SilverCube.transformers` names a list of
`boti_data.enrichment` steps instead — `TypeCaster`, `RowFilter`,
`DerivedColumn`, `Deduplicator`, or a custom `DataFrameTransformer`. This
isn't a new transform framework: `boti_data.enrichment.protocol`'s own
docstring already calls this exact set "Silver-layer transforms (type
casting, filtering, derived columns)" — `SilverCube` only composes it the
same way `BronzeCube` composes `BaseDataCube` + `ParquetSink`.

`DataFrameTransformer.transform()` is `async def` (so `AsyncFrameEnricher`'s
real I/O-bound lookups fit the same protocol), which is why this overrides
`afix_data()`/uses `aload()` rather than `BronzeCube`'s sync
`fix_data()`/`load()` pair — `boti_data`'s own sync/async hook split maps
directly onto bronze (sync, no CompositeTransformer) vs. silver (async,
CompositeTransformer) here.

Dask compatibility (empirically verified, not assumed from type hints —
`boti_data.enrichment.transformers` is typed `pd.DataFrame -> pd.DataFrame`
throughout, which understates what actually works): `RowFilter` and
`DerivedColumn` run correctly against a `dd.DataFrame` and stay lazy
end-to-end (`.eval()`/`.loc[mask]` both have real dask implementations).
`Deduplicator` also produces a correct result on dask — including across
partition boundaries, since `dd.DataFrame.drop_duplicates()` is a real
distributed operation, not a naive per-partition one — but forces one eager
`len(df)` call first (for its debug-log row count), so it doesn't stay
100% lazy end-to-end. `TypeCaster` used to be a genuine break (`dd.DataFrame.
astype()` doesn't accept the `errors=` kwarg `TypeCaster.transform()`
unconditionally passed), reported upstream and fixed in `boti-data` 1.3.8 —
`TypeCaster` now branches on frame type internally, so all four transformers
work on dask without any special handling here. `afix_data()` below accepts
dask input by default (this package is dask-first end to end, `boti_dask`
being a foundational, always-installed dependency) rather than pre-emptively
rejecting it.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

import dask.dataframe as dd
import pandas as pd
from boti_data.datacube import BaseDataCube
from boti_data.enrichment import CompositeTransformer, DataFrameTransformer
from boti_data.pipelines import ParquetSink, SinkWriteResult
from boti_data.pipelines.sinks import ParquetDestination


class SilverCube(BaseDataCube, ABC):
    """Base for silver-layer cubes: bronze parquet source in, cleaned parquet
    destination out.

    Build the same way as `BronzeCube` — typically
    `SubclassCube.from_helper(datasources.data_helper(profile,
    parquet_storage_path=...))` — except silver's source is usually a bronze
    parquet dataset (via `boti_data.parquet.ParquetReader`), not a live SQL
    profile: silver reads what bronze already landed and cleans it, it
    doesn't re-query the source system.
    """

    @property
    @abstractmethod
    def silver_destination(self) -> ParquetDestination:
        """Where `save_to_parquet()` writes to.

        Same accepted shapes as `BronzeCube.bronze_destination`: a
        `{"parquet_storage_path": ..., "fs": ...}` mapping, a
        `ParquetDataConfig`, or a `ParquetReader`. A common implementation
        reads from `self.config` (populated via `from_helper(helper,
        silver_destination=...)`'s `**overrides`), same convention as
        `BronzeCube.bronze_destination`.
        """

    @property
    @abstractmethod
    def transformers(self) -> Sequence[DataFrameTransformer]:
        """Ordered `boti_data.enrichment` steps run over the loaded frame,
        e.g. `[TypeCaster({...}), RowFilter([...]), Deduplicator(subset=[...])]`.
        Composed and run in order via `CompositeTransformer` in `afix_data()`.

        Stays Python, same as `BronzeCube.fix_data()`'s own business logic —
        `SilverJobConfig`/`SilverJobs` (see `silver_jobs.py`) externalize
        `silver_destination`'s source/destination locations, but deliberately
        cannot express this: a fixed class attribute (`transformers = [...]`
        on the subclass), or supplied via `from_helper(reader,
        transformers=[...])` the same way `silver_destination` is supplied,
        are both fine — `self.config` works for either.
        """

    async def afix_data(self, **kwargs: Any) -> None:
        """Runs `transformers` over `self.df` via `CompositeTransformer`.

        Accepts `pd.DataFrame` or `dd.DataFrame` — see this module's
        docstring for which `boti_data.enrichment` transformers stay fully
        lazy on dask (`RowFilter`, `DerivedColumn`) versus which force one
        eager call (`Deduplicator`); all four (including `TypeCaster`) now
        run correctly. `pa.Table`/`pl.DataFrame` are rejected up front with
        a clear error —
        `RowFilter`/`DerivedColumn`'s `.eval()` calls have no polars/arrow
        equivalent, so running the pipeline against either would otherwise
        fail deep inside `boti_data.enrichment` with a much less legible
        `AttributeError`.
        """
        if not isinstance(self.df, pd.DataFrame | dd.DataFrame):
            raise TypeError(
                f"{type(self).__name__}.afix_data() requires return_type='pandas' "
                "or 'dask' (boti_data.enrichment's RowFilter/DerivedColumn call "
                "DataFrame.eval(), which pa.Table/pl.DataFrame don't implement); "
                f"got {type(self.df).__name__}."
            )
        pipeline = CompositeTransformer(self.transformers)
        self.df = await pipeline.transform(self.df)

    async def save_to_parquet(
        self,
        *,
        partition_on: Sequence[str] | None = None,
        **load_options: Any,
    ) -> SinkWriteResult:
        """Load a bronze frame (through `afix_data()`'s transform pipeline)
        and write it to `silver_destination`.

        Same `partition_on`/flat-dataset semantics as
        `BronzeCube.save_to_parquet()`, including its `return_type` default:
        this doesn't override it, so it inherits `DataHelper`/`DataGateway`'s
        own dask-first default rather than forcing pandas. Async, unlike
        `BronzeCube`'s sync `save_to_parquet()` — forced by `afix_data()`
        needing to `await` `CompositeTransformer.transform()`.
        """
        df = await self.aload(**load_options)
        with ParquetSink(self.silver_destination, partition_on=partition_on) as sink:
            return sink.write(df)


__all__ = ["SilverCube"]
