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

`save_to_parquet_history()` is a second materialization strategy, not a
replacement: a full `save_to_parquet()` reload is right for small tables;
this is for tables where re-reading everything every run is wasteful. It
loads only rows past the last committed watermark (`DataHelper.
load_incremental()`, wired through `sweet_etl.bronze_jobs.
BronzeJobs.load_incremental_kwargs()`) and writes them into a fresh,
never-before-written `extracted_on=<today>` partition — an *append log*,
each partition immutable once written, not a live "current state" table
(the same entity re-extracted after an update reappears in a later
partition; only a downstream latest-per-key pass, e.g. a silver job,
collapses that). Deliberately does not attempt true incremental writing:
`ParquetSink.write(overwrite=False)` (scratch-tested, not assumed) writes
into a *new* partition safely but silently destroys whatever was already in
a partition it revisits — a business-date column can legitimately revisit
an old partition (a late update); the run's own extraction date cannot, so
that's what's used, at the cost of the append-log semantics above.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any, Literal

import pandas as pd
from boti_data.datacube import BaseDataCube
from boti_data.pipelines import ParquetSink, SinkWriteResult
from boti_data.pipelines.sinks import ParquetDestination
from boti_data.watermark import WatermarkStore


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

    def save_to_parquet_history(
        self,
        *,
        watermark_field: str,
        watermark_source: str,
        watermark_store: WatermarkStore,
        operator: Literal["gt", "gte"] = "gt",
        initial_value: Any = None,
        partition_field: str = "extracted_on",
        partition_value: str | None = None,
        **load_options: Any,
    ) -> SinkWriteResult | None:
        """Load only rows past the committed watermark and append them as a
        new `partition_field=partition_value` partition — never rewriting an
        existing one. See this module's own docstring for the append-log
        semantics this implies and why the partition key is the run's own
        extraction date, not a business date column.

        Returns `None` (no write) when there are no new rows since the last
        watermark.

        Defaults `return_type` to `"pandas"` (unlike `save_to_parquet()`'s
        `"dask"` default) — `DataHelper.load_incremental()` already
        materializes its result eagerly to compute the new watermark, so
        there is no lazy dask graph benefit left to preserve, and a plain
        `pd.DataFrame` is what `partition_field`'s column assignment below
        and most `fix_data()` implementations expect.

        Commits the watermark itself, *after* a successful write — not via
        `load_incremental()`'s own `commit_on_success` (this passes
        `commit_on_success=False` regardless of `load_options`). Committing
        inside `load_incremental()` happens before the write; a crash
        between load and write would advance the watermark past rows that
        were never actually written, silently skipping them forever on every
        later run. Committing after the write closes that gap — at the cost
        of a crash between write and commit re-extracting (not losing) the
        same rows next run, which `save_to_parquet_history`'s partition
        pre-existence check turns into a loud `FileExistsError`, not a
        silent overwrite.

        There is deliberately no clean "redo this partition" option: with
        `ParquetSink`, `overwrite=False` on an existing partition corrupts
        it (silent partial overwrite, proven above) and `overwrite=True`
        replaces the *entire* dataset, not just this partition. Recovering
        from a failed run means deleting the partial `partition_field=
        partition_value` partition directory by hand before rerunning.
        """
        load_options.setdefault("return_type", "pandas")
        load_options.pop("commit_on_success", None)
        result = self._helper.load_incremental(
            watermark_field=watermark_field,
            watermark_source=watermark_source,
            watermark_store=watermark_store,
            operator=operator,
            initial_value=initial_value,
            commit_on_success=False,
            **load_options,
        )
        if not result:
            return None

        self.df = result.frame
        if self._guard_has_data() and self._fix_data_is_overridden():
            self.fix_data()
        if not isinstance(self.df, pd.DataFrame):
            raise TypeError(
                f"{type(self).__name__}.save_to_parquet_history() requires "
                f"return_type='pandas' (forced above; a fix_data() override must "
                f"not change the frame type); got {type(self.df).__name__}."
            )

        value = partition_value or datetime.now(UTC).date().isoformat()
        date.fromisoformat(value)  # fail fast on a malformed explicit partition_value
        self.df[partition_field] = value

        with ParquetSink(self.bronze_destination, partition_on=[partition_field]) as sink:
            fs = sink.reader.resource.require_fs()
            target = f"{sink.reader.parquet_storage_path.rstrip('/')}/{partition_field}={value}"
            if fs.exists(target):
                raise FileExistsError(
                    f"{target} already exists — save_to_parquet_history() never "
                    "overwrites an existing partition (see this method's docstring). "
                    "Delete it and rerun to retry."
                )
            write_result = sink.write(self.df, overwrite=False)

        if result.current_watermark is not None:
            watermark_store.write(source=watermark_source, value=result.current_watermark)
        return write_result


__all__ = ["BronzeCube"]
