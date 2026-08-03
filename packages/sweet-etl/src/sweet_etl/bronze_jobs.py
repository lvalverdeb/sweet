"""Named bronze-layer job configuration loaded from a `bronze_jobs.yaml` file.

`CustomerCube`/`LocaliserCube`/`ProductCube` (see `sandbox/notebooks/
datacubes.ipynb`) each hand-wire the same shape of parameters in Python: a
`Datasources` SQL profile + table + `field_map`/`sticky_filters`, and a
`Datasources` filesystem profile + sub-path as the parquet destination. Once
a third table needed the exact same shape, this stopped being one-off cube
config and became data worth externalizing — same reasoning as `Datasources`
itself for connection profiles.

This intentionally stops at *source and destination* parameters (table,
filters, where the parquet lands, `partition_on`, a `limit` for capping large
tables). It does not, and cannot, express `fix_data()` — which columns need
int/bool/date coercion, business rules like row exclusion, derived fields —
that stays Python on the `BronzeCube` subclass, same as `int_fields`/
`bool_fields`/`boolean_fields` already do there.

`sticky_filters` values use `boti_data.filters`' Django-QuerySet-style
lookup syntax — see `Datasources.data_helper()`'s own docstring for the
full operator list (`__gte`/`__lte`/`__gt`/`__lt`/`__in`/`__range`/
`__contains`/`__startswith`/`__isnull`/... plus `not_*` negation and
`__date`/`__year`/`__month`/... transforms), not re-listed here to avoid
the two drifting apart. `products`' `sticky_filters: {product_type_id: 1}`
in `bronze_jobs.yaml.example` is a bare (`exact`) lookup; a key like
`updated_at__gte: 2026-01-01` would use the same suffix syntax.

`watermark_field`/`watermark_operator`/`watermark_initial_value` +
`load_incremental_kwargs()` wire `boti_data.watermark`/`DataHelper.
load_incremental()` into the *extract* side: a configured job reads only
rows newer than its last committed watermark, instead of a full re-query on
every run.

`materialization`/`history_partition_field` pick which write strategy a job
uses. `"full"` (the default) is `BronzeCube.save_to_parquet()`, unchanged —
right for small tables. `"history"` is `BronzeCube.
save_to_parquet_history()`: write only the incrementally-loaded rows, as a
new `history_partition_field=<today>` partition each run, never rewriting an
existing one. This was deliberately *not* the obvious "just write the new
rows" approach — two were tried empirically (scratch-tested against real
`ParquetSink`, not assumed from its docstring) and rejected:

- `ParquetSink.write(overwrite=False)` writes new partition directories
  additively, but silently **overwrites** (data loss, not a merge) any
  partition a new batch's rows land back in — e.g. a late-arriving row for a
  date already materialized. Confirmed by writing to the same
  `partition_on` value twice: the first batch's rows were gone after the
  second write. This is exactly why `save_to_parquet_history()` partitions
  by the run's own extraction date rather than any business date column —
  a business date can legitimately revisit an old partition, extraction
  date can't.
- Read the existing parquet back, `pd.concat()` the incremental rows,
  `overwrite=True` the whole thing — hits its own real issue:
  `ParquetReader.load()` returns pandas' nullable extension dtypes
  (`Int64`/`Float64`), freshly-extracted SQL rows don't, and concatenating
  the two before `boti_data`'s dask/pyarrow write path can raise
  `ArrowInvalid: Cannot yet unify dictionaries with nulls`.

`save_to_parquet_history()`'s own docstring covers what's still a real
constraint: it's an append *log* (an updated row reappears in a later
partition rather than replacing itself in place — only a downstream
latest-per-key pass collapses that), and there's no clean way to redo a
failed partition — delete it by hand and rerun.

`columns` projects a job's load to just the fields it actually needs — do
this deliberately for any wide source table; pulling every raw column risks
the dask meta/schema issues documented in `sweet_etl/gold.py`'s module
docstring. It's a load-time parameter (`DataGateway.load(columns=...)`),
unlike `table`/`field_map`/`sticky_filters`, which bind at `DataHelper`
*construction* time — see `data_helper_kwargs()` vs. `load_kwargs()` below.

`refresh`/`refresh_field` (`sweet_etl.refresh_presets`) are a second,
*stateless* extraction strategy, alongside — never combined with, see
`BronzeJobConfig`'s own validator — `watermark_field`'s *stateful* one: a
named calendar-relative window (`"today"`/`"current_week"`/
`"current_month"`/`"ytd"`/`"itd"`) recomputed fresh from the real current
date on every run, rather than a persisted "last extracted value" advanced
through a `WatermarkStore`. Resolved via `load_kwargs()`, which — like
`load_incremental_kwargs()` — is a separate call from `data_helper_kwargs()`
(construction-time) since `columns`/`limit`/a resolved `refresh` filter are
all load-time parameters. `load_kwargs()`'s own `period=` argument overrides
a job's configured `refresh` preset for one call — the hook
`sweet_etl.extract_runner.run_bronze_fleet()` uses to apply a single preset
across every `refresh_field`-configured job in a fleet run, without editing
`bronze_jobs.yaml` itself.

This module has no fleet-level "run every configured job" entry point —
`BronzeJobs` only holds per-job config and per-job accessors, on purpose
(same one-thing-per-module split as the rest of this package). See
`sweet_etl.extract_runner` for that.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any, Literal

from boti_data.watermark import WatermarkStore
from pydantic import BaseModel, ValidationError, model_validator
from sweet_config import load_yaml_defaults

from sweet_etl.datasources import Datasources
from sweet_etl.refresh_presets import RefreshPreset, resolve_refresh_filter


class BronzeDestination(BaseModel):
    """Where a bronze job's parquet output lands.

    `filesystem_profile` names a `Datasources` filesystem profile (e.g. the
    `etl` S3 bucket); `path` is a sub-path under that profile's own
    `storage_path` root (e.g. `bronze/customers`).
    """

    filesystem_profile: str
    path: str


class BronzeJobConfig(BaseModel):
    """One bronze job: a SQL source plus a parquet destination."""

    sql_profile: str
    table: str
    field_map: dict[str, str] | None = None
    sticky_filters: dict[str, Any] | None = None
    columns: list[str] | None = None
    destination: BronzeDestination
    partition_on: list[str] | None = None
    limit: int | None = None
    watermark_field: str | None = None
    watermark_operator: Literal["gt", "gte"] = "gt"
    watermark_initial_value: Any | None = None
    refresh: RefreshPreset | None = None
    refresh_field: str | None = None
    materialization: Literal["full", "history"] = "full"
    history_partition_field: str = "extracted_on"

    @model_validator(mode="after")
    def _validate_extraction_strategy(self) -> BronzeJobConfig:
        if self.watermark_field is not None and self.refresh is not None:
            raise ValueError(
                "watermark_field and refresh are two different extraction strategies "
                "(stateful incremental vs. stateless calendar-relative window) — "
                "configure at most one, not both."
            )
        if (self.refresh is None) != (self.refresh_field is None):
            raise ValueError("refresh and refresh_field must be set together, or not at all.")
        return self


class BronzeJobs:
    """Named bronze job configs loaded from a YAML file.

    ```yaml
    jobs:
      customers:
        sql_profile: replica
        table: crm_clientes_archivo
        field_map:
          cliente_id: customer_id
        destination:
          filesystem_profile: etl
          path: bronze/customers
        partition_on: null
        limit: null
    ```
    """

    def __init__(
        self,
        jobs_file: str | Path,
        datasources: Datasources,
        *,
        watermark_store: WatermarkStore | None = None,
    ) -> None:
        self._path = Path(jobs_file)
        self.datasources = datasources
        self.watermark_store = watermark_store
        self._jobs: dict[str, BronzeJobConfig] = {}
        self._load()

    def job(self, name: str) -> BronzeJobConfig:
        try:
            return self._jobs[name]
        except KeyError as exc:
            raise KeyError(
                f"Unknown bronze job {name!r}. Available: {sorted(self._jobs)}"
            ) from exc

    def job_names(self) -> list[str]:
        """Every configured job name, in `bronze_jobs.yaml`'s own order —
        the whitelist `sweet_etl.extract_runner.run_bronze_fleet()` iterates
        by default."""
        return list(self._jobs)

    def data_helper_kwargs(self, name: str) -> dict[str, Any]:
        """`Datasources.data_helper()`'s own kwargs for this job's source."""
        job = self.job(name)
        kwargs: dict[str, Any] = {"table": job.table}
        if job.field_map:
            kwargs["field_map"] = job.field_map
        if job.sticky_filters:
            kwargs["sticky_filters"] = job.sticky_filters
        return kwargs

    def bronze_destination(self, name: str) -> dict[str, Any]:
        """A `ParquetSink`-compatible destination mapping for this job."""
        destination = self.job(name).destination
        return self.datasources.parquet_location(
            filesystem_profile=destination.filesystem_profile, path=destination.path
        )

    def load_incremental_kwargs(self, name: str) -> dict[str, Any]:
        """`DataHelper.load_incremental()`'s own kwargs for this job's
        watermark — a separate call from `data_helper_kwargs()`
        (`table`/`field_map`/`sticky_filters` feed `Datasources.data_helper()`
        *construction*; this feeds `helper.load_incremental()` at *load
        time*): `helper = datasources.data_helper(job.sql_profile, **jobs.
        data_helper_kwargs(name)); result = helper.load_incremental(**jobs.
        load_incremental_kwargs(name))`.

        Raises `ValueError` if the job has no `watermark_field` configured,
        or this `BronzeJobs` was constructed without a `watermark_store` —
        both are required for `load_incremental()` to have anywhere to
        persist the watermark it advances to. See this module's own
        docstring for why the result still isn't safe to feed straight into
        `BronzeCube.save_to_parquet()`.
        """
        job = self.job(name)
        if job.watermark_field is None:
            raise ValueError(f"Bronze job {name!r} has no watermark_field configured.")
        if self.watermark_store is None:
            raise ValueError(
                f"BronzeJobs was constructed without a watermark_store; "
                f"cannot load {name!r} incrementally."
            )
        return {
            "watermark_field": job.watermark_field,
            "watermark_source": name,
            "watermark_store": self.watermark_store,
            "operator": job.watermark_operator,
            "initial_value": job.watermark_initial_value,
        }

    def load_kwargs(
        self,
        name: str,
        *,
        today: datetime.date | None = None,
        period: RefreshPreset | None = None,
    ) -> dict[str, Any]:
        """Load-time kwargs (`columns`/`limit`/a resolved refresh filter)
        for this job's `save_to_parquet()` call — a separate call from
        `data_helper_kwargs()` (construction-time `table`/`field_map`/
        `sticky_filters`) and `load_incremental_kwargs()` (the stateful
        watermark strategy): `helper = datasources.data_helper(job.
        sql_profile, **jobs.data_helper_kwargs(name)); cube.save_to_parquet(
        **jobs.load_kwargs(name))`.

        `today` defaults to the real current date; pass it explicitly for a
        deterministic test of a `refresh`-configured job.

        `period`, if given, *overrides* the job's own configured `refresh`
        preset for this call only (`sweet_etl.extract_runner.
        run_bronze_fleet()`'s own `period=` argument feeds this, to apply
        one preset across an entire fleet run) — but only for a job that
        already has `refresh_field` configured; a job with no `refresh_field`
        (watermark-based, or unfiltered "full" reload) has nothing for a
        date-window preset to filter on, so `period` has no effect on it.
        """
        job = self.job(name)
        kwargs: dict[str, Any] = {}
        if job.columns:
            kwargs["columns"] = job.columns
        if job.limit:
            kwargs["limit"] = job.limit
        effective_preset = period if period is not None else job.refresh
        if effective_preset is not None and job.refresh_field is not None:
            kwargs["filters"] = resolve_refresh_filter(
                effective_preset, field=job.refresh_field, today=today
            )
        return kwargs

    def _load(self) -> None:
        data = load_yaml_defaults(self._path)

        for name, job in data.get("jobs", {}).items():
            try:
                self._jobs[name] = BronzeJobConfig(**job)
            except ValidationError as exc:
                raise ValueError(f"{self._path}: jobs.{name} is invalid: {exc}") from exc


__all__ = ["BronzeDestination", "BronzeJobConfig", "BronzeJobs"]
