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
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

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

    def __init__(self, jobs_file: str | Path, datasources: Datasources) -> None:
        self._path = Path(jobs_file)
        self.datasources = datasources
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

    def _load(self) -> None:
        data = load_yaml_defaults(self._path)

        for name, job in data.get("jobs", {}).items():
            try:
                self._jobs[name] = BronzeJobConfig(**job)
            except ValidationError as exc:
                raise ValueError(f"{self._path}: jobs.{name} is invalid: {exc}") from exc


__all__ = ["BronzeDestination", "BronzeJobConfig", "BronzeJobs"]
