"""Named datasource configuration loaded from a `datasources.yaml` file.

`boti.core.filesystem.FilesystemConfig` and `boti_data.db.sql_config.
SqlDatabaseConfig` are the ecosystem's own configuration objects for a
filesystem/SQL connection profile — but both are plain pydantic models with
only env-prefix loaders (`from_env_prefix`), no YAML support (checked).
`Datasources` is the YAML-loading glue those types don't have themselves: it
reads named profiles from a YAML file, constructs the real boti/boti_data
config objects directly (extracting each profile's credentials —
fs_key/fs_secret/fs_token, connection_url), and registers them into a
`boti_data.ConnectionCatalog`, which already provides live-resource access
(`.filesystem(name)`, `.create_sql_resource(name)`) beyond the raw configs.

Redis has no ConnectionCatalog-equivalent registry in the ecosystem (checked
— no Redis references anywhere in boti/boti_data/boti_dask), so `RedisConfig`
profiles are kept in a plain dict here instead.

`data_helper()`/`datacube()` build a `boti_data.helper.DataHelper` /
`boti_data.datacube.BaseDataCube` directly from a named SQL profile's
`SqlDatabaseConfig` — `DataHelper` accepts a `SqlDatabaseConfig` (one of
`DataGateway`'s own `BackendConfig` union members) as its `config` argument
natively, so no adapter is needed between the two.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from boti.core.filesystem import FilesystemConfig, add_endpoint_to_allowlist
from boti_data.connection_catalog import ConnectionCatalog
from boti_data.datacube import BaseDataCube
from boti_data.db.sql_config import SqlDatabaseConfig
from boti_data.helper import DataHelper
from boti_data.watermark import FsspecWatermarkStore
from pydantic import ValidationError
from sweet_config import load_yaml_defaults

from sweet_etl.redis_config import RedisConfig


def _trust_endpoint(endpoint: str) -> None:
    """Allowlist `endpoint`'s host[:port] so a private IP from trusted
    deployment config doesn't trip FilesystemConfig's SSRF guard — the same
    trust `FilesystemConfig.from_env_prefix(trust_env_endpoint=True)` gives
    on the env-prefix path. Mirrors boti.core.filesystem's own (private)
    `_allowlist_endpoint_from_url`; there's no public equivalent that takes
    a config value directly.
    """
    parsed = urlparse(endpoint.strip())
    hostname = parsed.hostname or ""
    if not hostname:
        return
    key = f"{hostname}:{parsed.port}" if parsed.port else hostname
    add_endpoint_to_allowlist(key, hostname)


class Datasources:
    """Named filesystem/SQL connection profiles loaded from a YAML file.

    ```yaml
    filesystems:
      source:
        fs_type: s3
        fs_path: s3://my-bucket/source
        fs_key: ...
        fs_secret: ...
        fs_endpoint: ...
    sql:
      defaults:              # merged under each connection below
        pool_size: 20
      connections:
        replica:
          connection_url: ...
    redis:
      cache:
        host: ...
        port: 6379
    ```
    """

    def __init__(self, datasources_file: str | Path) -> None:
        self._path = Path(datasources_file)
        self.catalog = ConnectionCatalog()
        self._redis_configs: dict[str, RedisConfig] = {}
        self._load()

    def filesystem(self, name: str) -> FilesystemConfig:
        return self.catalog.filesystem_config(name)

    def sql(self, name: str) -> SqlDatabaseConfig:
        return self.catalog.sql_config(name)

    def redis(self, name: str) -> RedisConfig:
        try:
            return self._redis_configs[name]
        except KeyError as exc:
            raise KeyError(
                f"Unknown redis profile {name!r}. Available: {sorted(self._redis_configs)}"
            ) from exc

    def data_helper(self, name: str, **gateway_kwargs: Any) -> DataHelper:
        """A `DataHelper` wired to the named SQL profile's credentials.

        `gateway_kwargs` are `boti_data.gateway.DataGateway`'s own
        constructor kwargs (`table=`, `field_map=`, `sticky_filters=`, ...) —
        pass `table=` for configured-mode loads, or leave unset and pass
        `statement=`/`model=` to `DataHelper.load()` instead (structured
        mode). See `DataGateway`'s own docstring for both.

        `sticky_filters=` here and `filters=`/bare kwargs on `.load()`/
        `.aload()` use `boti_data.filters`' Django-QuerySet-style lookup
        syntax (`boti_data/filters/value_parsing.py`, checked): no suffix is
        `exact` (e.g. `product_type_id=1`), `field__<op>` covers `gte`,
        `lte`, `gt`, `lt`, `in`, `range`, `contains`/`icontains`,
        `startswith`/`istartswith`, `endswith`/`iendswith`, `isnull`,
        `regex`/`iregex`, `exact`/`iexact` — matching Django's own lookup
        names almost verbatim — plus `not_exact`/`not_contains`/`not_in`
        (aliases `ne`/`nin`) for negation, which Django instead expresses
        via `.exclude()`. Date/time transforms chain the same way Django's
        do: `field__date`, `field__year`/`__month`/`__day`/`__hour`/
        `__minute`/`__second`/`__week_day`, and `field__date__gte=...`-style
        3-part chains combining a transform with an operator. Only a subset
        (`exact`, `gt`, `gte`, `lt`, `lte`, `in`, `range`, `not_exact`,
        `not_in` — `boti_data.filters.value_parsing.pushdown_ops()`) push
        down to the SQL/parquet layer natively; the rest are applied as a
        residual in-memory filter after load.
        """
        return DataHelper(self.sql(name), **gateway_kwargs)

    def datacube(self, name: str, **gateway_kwargs: Any) -> BaseDataCube:
        """A `BaseDataCube` backed by the named SQL profile's credentials."""
        return BaseDataCube.from_helper(self.data_helper(name, **gateway_kwargs))

    def parquet_location(self, *, filesystem_profile: str, path: str) -> dict[str, Any]:
        """A `{"parquet_storage_path": ..., "fs": ...}` mapping for `path`
        under `filesystem_profile`'s own `storage_path` root — the shape
        `boti_data.pipelines.ParquetSink`/`boti_data.parquet.ParquetReader`
        both accept directly. Shared by `BronzeJobs.bronze_destination()`
        and `SilverJobs`' source/destination resolution — both name a
        filesystem profile + sub-path, nothing more.
        """
        fs_config = self.filesystem(filesystem_profile)
        fs = self.catalog.filesystem(filesystem_profile)
        resolved_path = f"{fs_config.storage_path.rstrip('/')}/{path.lstrip('/')}"
        return {"parquet_storage_path": resolved_path, "fs": fs}

    def watermark_store(self, *, filesystem_profile: str, path: str) -> FsspecWatermarkStore:
        """A `boti_data.watermark.FsspecWatermarkStore` backed by a JSON file
        at `path` under `filesystem_profile`'s own `storage_path` root — one
        file holds every job's watermark, keyed by `source=` (see
        `boti_data.watermark.store`'s own `_JsonWatermarkStoreBase`), so this
        is typically constructed once and shared across `BronzeJobs` jobs,
        not built per job.
        """
        fs_config = self.filesystem(filesystem_profile)
        fs = self.catalog.filesystem(filesystem_profile)
        resolved_path = f"{fs_config.storage_path.rstrip('/')}/{path.lstrip('/')}"
        return FsspecWatermarkStore(fs=fs, path=resolved_path)

    def _load(self) -> None:
        data = load_yaml_defaults(self._path)

        for name, profile in data.get("filesystems", {}).items():
            endpoint = profile.get("fs_endpoint")
            if endpoint:
                _trust_endpoint(endpoint)
            try:
                fs_config = FilesystemConfig(**profile)
            except ValidationError as exc:
                raise ValueError(f"{self._path}: filesystems.{name} is invalid: {exc}") from exc
            self.catalog.register_filesystem(name, fs_config)

        sql_section = data.get("sql", {})
        defaults = sql_section.get("defaults", {})
        for name, profile in sql_section.get("connections", {}).items():
            try:
                sql_config = SqlDatabaseConfig(**{**defaults, **profile})
            except ValidationError as exc:
                raise ValueError(
                    f"{self._path}: sql.connections.{name} is invalid: {exc}"
                ) from exc
            self.catalog.register_sql(name, sql_config)

        for name, profile in data.get("redis", {}).items():
            try:
                self._redis_configs[name] = RedisConfig(**profile)
            except ValidationError as exc:
                raise ValueError(f"{self._path}: redis.{name} is invalid: {exc}") from exc
