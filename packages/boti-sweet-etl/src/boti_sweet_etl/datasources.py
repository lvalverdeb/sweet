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
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from boti.core.filesystem import FilesystemConfig, add_endpoint_to_allowlist
from boti_data.connection_catalog import ConnectionCatalog
from boti_data.db.sql_config import SqlDatabaseConfig
from boti_sweet_config import load_yaml_defaults
from pydantic import ValidationError

from boti_sweet_etl.redis_config import RedisConfig


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
