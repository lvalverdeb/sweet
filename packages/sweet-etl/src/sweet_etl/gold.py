"""Abstract base for gold-layer cubes: combine a `boti_data.dataset.
HybridDataset` (historical bronze parquet + live SQL, split on a date) with
a bounded lookup enrichment (`boti_data.enrichment.AsyncFrameEnricher` +
`AttachmentSpec`) against a second source, write the result to a parquet
destination.

Not a `BaseDataCube` subclass, unlike `BronzeCube`/`SilverCube`:
`BaseDataCube.from_helper()` expects a single `DataHelper` as its source, but
this composes three — `historical`/`live` (both `DataHelper`s, wrapped by
`HybridDataset`) plus whatever each `AttachmentSpec.attachment_fn` fetches
from (typically a second `ParquetReader`, itself also a `DataHelper`, built
inside the fetch function you supply). `GoldCube` instead owns a
`HybridDataset` + `AsyncFrameEnricher` directly and manages their lifecycle
itself — the same reason `HybridDataset` itself doesn't subclass
`BaseDataCube` either (it subclasses `PicklableLifecycleCoreMixin`/
`LifecycleCore` directly).

No new fetch/merge machinery: `boti_data.enrichment.protocol`'s own
docstring already calls `AttachmentSpec`-driven lookup merges "Gold-layer
enrichment" by name — this only composes it with `HybridDataset`. Each
`AttachmentSpec.attachment_fn` you write is responsible for building its own
`ParquetReader`/`DataHelper` against whatever second source it enriches
from, and for closing it — `GoldCube` only owns the `HybridDataset` and
`AsyncFrameEnricher` lifecycles, it has no way to see inside an
`attachment_fn` closure.

Naming note (bronze/silver/gold vs. the Databricks medallion convention):
Databricks' own writeups draw the silver/gold line at *aggregation* — silver
is cleaned/conformed/enriched at the source grain (a same-grain lookup join
included), gold is business-level rollups. Read strictly, that would put
`GoldCube`'s lookup-merge in silver, and leave "gold" empty until this
package grows aggregation support. This module instead follows
`boti_data`'s own, narrower convention (lookup-merge = gold), because that's
the label already load-bearing in this ecosystem's source
(`boti_data.enrichment.protocol`'s docstring, quoted above) — introducing a
second, stricter convention on top of it inside `sweet_etl` would make the
same word mean different things depending which file you're reading, for a
naming preference rather than a defect. `SilverCube` (`silver.py`) still
covers the Databricks-style "conform" step (type-casting, filtering,
dedup) via `CompositeTransformer`; `GoldCube` covers the lookup-merge step
that `boti_data` calls gold. If this package later grows real
aggregation/rollup work, that's the point to revisit whether a third,
stricter-gold concept is worth the churn — not before.

Async-only (`aload()`/`save_to_parquet()`, no sync `load()`/`save_to_parquet_
sync()`): both `HybridDataset.aload()` and `AsyncFrameEnricher.aenrich()` are
already async at the primitive level, unlike `BronzeCube`'s plain
`DataHelper.load()`, so there's no synchronous path worth adding in parallel.

Two real-data gotchas (found running `GoldCube` against actual production
tables in `sandbox/notebooks/gold_materialization.ipynb`'s real-infra
variant, not present in that notebook's synthetic version — its date field
was a plain string column, which sidesteps both):

1. **Genuine upstream gap**: if `HybridDataset`'s `date_field` is a
   tz-aware `Timestamp` column, period loads can break. `boti_data.gateway.
   normalization.prepare_period_filters` unconditionally truncates `start`/
   `end` to a bare `datetime.date`, and PyArrow has no comparison kernel
   between `date32` and a tz-aware `timestamp` column when filtering a lazy
   (dask-backed) parquet read — verified directly: the identical filter
   with a real `pd.Timestamp` value succeeds, the same filter with a
   `datetime.date` value raises `ArrowNotImplementedError`/`ArrowInvalid`.
   This is a `boti_data`/PyArrow limitation, not a `sweet_etl` one — the
   workaround is on the historical (parquet) side: write that branch's
   date column as plain `datetime.date` (`date32`, which *does* have a
   kernel against `datetime.date` filters) before it lands in bronze
   parquet, rather than keeping the tz-aware dtype `HybridDataset.
   date_field` was pointed at.
2. **Not a bug, a modeling mistake to avoid**: the `date32` workaround
   above then makes the historical branch's date column diverge in dtype
   from the live (SQL) branch's tz-aware `Timestamp` for that same column.
   Combined with pulling every raw column off a wide source table (most
   entirely null in any given slice), `HybridDataset`'s dask-level concat
   produces per-partition metadata PyArrow can't reconcile, and
   `ParquetSink.write()` fails with `ArrowInvalid: Could not convert
   <object object at ...>` — a symptom of dask's own `_meta_nonempty`
   schema-inference giving up, not real data corruption (`.compute()` on
   the same lazy frame succeeds and returns a complete, correct
   `pd.DataFrame` throughout). Fixed by not carrying the mistake in the
   first place: project to only the columns the cube actually needs (a
   gold/mart output should be a narrow, purpose-built view anyway, not a
   copy of a wide raw table) and normalize any remaining divergent column
   (e.g. the `date_field` from gotcha 1) in `fix_data()` — the hook that
   exists for exactly this, run once after enrichment, before the frame
   reaches `ParquetSink`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from boti.core.lifecycle import LifecycleCore
from boti.core.lifecycle_pickle import PicklableLifecycleCoreMixin
from boti_data.dataset import HybridDataset
from boti_data.dataset.hybrid_support import FrameResult, HybridSource
from boti_data.enrichment import AsyncFrameEnricher, AttachmentSpec
from boti_data.pipelines import ParquetSink, SinkWriteResult
from boti_data.pipelines.sinks import ParquetDestination


class GoldCube(PicklableLifecycleCoreMixin, LifecycleCore, ABC):
    """Base for gold-layer cubes: historical+live source in (via
    `HybridDataset`), bounded lookup enrichment applied, parquet destination
    out.

    Build from an already-constructed `HybridDataset` and the
    `AttachmentSpec`s to enrich it with:

    ```python
    hybrid = HybridDataset(historical_reader, live_helper,
                            date_field="order_date", split_date="2026-01-03")
    cube = MyGoldCube.from_hybrid(hybrid, [customer_lookup_spec])
    ```

    This class only adds `gold_destination` (which subclasses must
    implement) and `save_to_parquet()`.
    """

    def __init__(
        self,
        hybrid: HybridDataset,
        attachment_specs: Sequence[AttachmentSpec],
        *,
        default_max_unique_values: int = 5000,
        max_concurrency: int = 4,
    ) -> None:
        self._hybrid = hybrid
        self._enricher = AsyncFrameEnricher(
            attachment_specs,
            default_max_unique_values=default_max_unique_values,
            max_concurrency=max_concurrency,
        )
        self.df: FrameResult | None = None
        self.config: dict[str, Any] = {}
        super().__init__()

    @classmethod
    def from_hybrid(
        cls,
        hybrid: HybridDataset,
        attachment_specs: Sequence[AttachmentSpec],
        **overrides: Any,
    ) -> GoldCube:
        """Construct from a pre-built `HybridDataset`, same `**overrides`
        convention as `BaseDataCube.from_helper()`/`BronzeCube.from_helper()`
        — pass `gold_destination=...` here, read back via
        `self.config["gold_destination"]`.
        """
        instance = cls.__new__(cls)
        instance._hybrid = hybrid
        instance._enricher = AsyncFrameEnricher(attachment_specs)
        instance.df = None
        instance.config = dict(overrides)
        LifecycleCore.__init__(instance)
        return instance

    @property
    @abstractmethod
    def gold_destination(self) -> ParquetDestination:
        """Where `save_to_parquet()` writes to.

        Same accepted shapes as `BronzeCube.bronze_destination`. A common
        implementation reads from `self.config` (populated via
        `from_hybrid(hybrid, specs, gold_destination=...)`'s `**overrides`),
        same convention as `BronzeCube`/`SilverCube`.
        """

    def fix_data(self, **kwargs: Any) -> None:
        """Optional sync transform hook, run after enrichment. Override in
        subclasses if needed — same role as `BronzeCube.fix_data()`."""
        return None

    def _fix_data_is_overridden(self) -> bool:
        return type(self).fix_data is not GoldCube.fix_data

    async def aload(
        self,
        *,
        start: str,
        end: str,
        source: HybridSource = "auto",
        cols: Sequence[str] | None = None,
        **hybrid_options: Any,
    ) -> FrameResult:
        """Load the historical+live range via `HybridDataset.aload()`, then
        enrich it via `AsyncFrameEnricher.aenrich()`, then run `fix_data()`
        if overridden.
        """
        base = await self._hybrid.aload(start=start, end=end, source=source, **hybrid_options)
        self.df = await self._enricher.aenrich(base, cols=cols)
        if self._fix_data_is_overridden():
            self.fix_data()
        return self.df

    async def save_to_parquet(
        self,
        *,
        start: str,
        end: str,
        source: HybridSource = "auto",
        cols: Sequence[str] | None = None,
        partition_on: Sequence[str] | None = None,
        **hybrid_options: Any,
    ) -> SinkWriteResult:
        """Load (through `aload()`'s hybrid-load + enrich + `fix_data()`)
        and write to `gold_destination`.

        Same `partition_on` semantics as `BronzeCube.save_to_parquet()`.
        """
        df = await self.aload(start=start, end=end, source=source, cols=cols, **hybrid_options)
        with ParquetSink(self.gold_destination, partition_on=partition_on) as sink:
            return sink.write(df)

    async def __aenter__(self) -> GoldCube:
        await super().__aenter__()
        await self._hybrid.__aenter__()
        return self

    def __enter__(self) -> GoldCube:
        super().__enter__()
        self._hybrid.__enter__()
        return self

    def _cleanup(self) -> None:
        self._hybrid.close()

    async def _acleanup(self) -> None:
        await self._hybrid.aclose()


__all__ = ["GoldCube"]
