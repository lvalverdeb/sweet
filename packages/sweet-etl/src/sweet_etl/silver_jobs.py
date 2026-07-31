"""Named silver-layer job configuration loaded from a `silver_jobs.yaml` file.

Mirrors `sweet_etl.bronze_jobs.BronzeJobs` one layer up: `SilverJobConfig`
externalizes *source and destination* parameters only — `source` (a parquet
dataset, typically a bronze job's own output) and `destination` (where the
cleaned parquet lands), both the same `{filesystem_profile, path}` shape as
`BronzeDestination` — reused here rather than duplicated, since it's already
a generic "named filesystem profile + sub-path" reference despite the
"Bronze" name.

Deliberately decoupled from `BronzeJobs` — `source` names a filesystem
profile + path directly rather than referencing a `BronzeJobConfig` by name,
so a silver job can read from anything, not only sweet-managed bronze output.
`SqlDatabaseConfig`'s own `filesystem_profile` field on `ParquetDataConfig`
(the "official" boti_data way to name a filesystem profile inside a config
object) was considered and dropped: it requires a `catalog` to be threaded
through `DataGateway`/`DataHelper` construction to resolve, and nothing in
that path actually accepts one (checked) — the resolved-`fs` mapping
`Datasources.parquet_location()` already returns for `BronzeJobs` sidesteps
that gap entirely.

This does not, and cannot, express `transformers` — same reasoning as
`BronzeJobConfig` not expressing `fix_data()`: `TypeCaster`/`RowFilter`/
`DerivedColumn`/`Deduplicator` instances stay Python, on the `SilverCube`
subclass (see `silver.py`).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError
from sweet_config import load_yaml_defaults

from sweet_etl.bronze_jobs import BronzeDestination
from sweet_etl.datasources import Datasources


class SilverJobConfig(BaseModel):
    """One silver job: a parquet source plus a parquet destination."""

    source: BronzeDestination
    destination: BronzeDestination
    partition_on: list[str] | None = None


class SilverJobs:
    """Named silver job configs loaded from a YAML file.

    ```yaml
    jobs:
      products:
        source:
          filesystem_profile: bronze_demo
          path: bronze/products
        destination:
          filesystem_profile: bronze_demo
          path: silver/products
        partition_on: null
    ```
    """

    def __init__(self, jobs_file: str | Path, datasources: Datasources) -> None:
        self._path = Path(jobs_file)
        self.datasources = datasources
        self._jobs: dict[str, SilverJobConfig] = {}
        self._load()

    def job(self, name: str) -> SilverJobConfig:
        try:
            return self._jobs[name]
        except KeyError as exc:
            raise KeyError(
                f"Unknown silver job {name!r}. Available: {sorted(self._jobs)}"
            ) from exc

    def source_reader_kwargs(self, name: str) -> dict[str, Any]:
        """A `ParquetReader`-compatible mapping for this job's parquet source."""
        source = self.job(name).source
        return self.datasources.parquet_location(
            filesystem_profile=source.filesystem_profile, path=source.path
        )

    def silver_destination(self, name: str) -> dict[str, Any]:
        """A `ParquetSink`-compatible destination mapping for this job."""
        destination = self.job(name).destination
        return self.datasources.parquet_location(
            filesystem_profile=destination.filesystem_profile, path=destination.path
        )

    def _load(self) -> None:
        data = load_yaml_defaults(self._path)

        for name, job in data.get("jobs", {}).items():
            try:
                self._jobs[name] = SilverJobConfig(**job)
            except ValidationError as exc:
                raise ValueError(f"{self._path}: jobs.{name} is invalid: {exc}") from exc


__all__ = ["SilverJobConfig", "SilverJobs"]
