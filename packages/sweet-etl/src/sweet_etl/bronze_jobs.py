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
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from boti_data.watermark import WatermarkStore
from pydantic import BaseModel, ValidationError
from sweet_config import load_yaml_defaults

from sweet_etl.datasources import Datasources


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
    destination: BronzeDestination
    partition_on: list[str] | None = None
    limit: int | None = None
    watermark_field: str | None = None
    watermark_operator: Literal["gt", "gte"] = "gt"
    watermark_initial_value: Any | None = None
    materialization: Literal["full", "history"] = "full"
    history_partition_field: str = "extracted_on"


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

    def _load(self) -> None:
        data = load_yaml_defaults(self._path)

        for name, job in data.get("jobs", {}).items():
            try:
                self._jobs[name] = BronzeJobConfig(**job)
            except ValidationError as exc:
                raise ValueError(f"{self._path}: jobs.{name} is invalid: {exc}") from exc


__all__ = ["BronzeDestination", "BronzeJobConfig", "BronzeJobs"]
