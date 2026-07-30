"""This specific deployment's own configuration.

Everything here is client-specific wiring that has no home in a generic
`boti-sweet-*` package — either because it's built entirely from existing
`boti`/`boti_data` primitives (the connection catalog) or because it's this
client's own business config with no generic shape to extract yet (ETL
service integration, routing/geo, security). See CLAUDE.md for the
"generalize only once there's a second real consumer" rule this follows.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from boti_sweet_config import load_settings, load_yaml_defaults
from pydantic import BaseModel, SecretStr, ValidationError

if TYPE_CHECKING:
    from boti_data import ConnectionCatalog


def _trust_endpoint(endpoint: str) -> None:
    """Allowlist `endpoint`'s host[:port] so a private IP from trusted
    deployment config (datasources.yaml) doesn't trip FilesystemConfig's SSRF
    guard. Mirrors boti.core.filesystem's own (private) _allowlist_endpoint_from_url
    — there's no public equivalent that takes a config value directly.
    """
    from boti.core.filesystem import add_endpoint_to_allowlist

    parsed = urlparse(endpoint.strip())
    hostname = parsed.hostname or ""
    if not hostname:
        return
    key = f"{hostname}:{parsed.port}" if parsed.port else hostname
    add_endpoint_to_allowlist(key, hostname)


def build_connection_catalog(*, datasources_file: str | Path) -> ConnectionCatalog:
    # Lazy: boti-data (and its sqlalchemy/dask/pandas/polars dependency chain)
    # only comes with the "etl" extra, which this sandbox must work without.
    from boti.core.filesystem import FilesystemConfig
    from boti_data import ConnectionCatalog
    from boti_data.db.sql_config import SqlDatabaseConfig

    catalog = ConnectionCatalog()
    data = load_yaml_defaults(datasources_file)

    for name, profile in data.get("filesystems", {}).items():
        endpoint = profile.get("fs_endpoint")
        if endpoint:
            _trust_endpoint(endpoint)
        try:
            fs_config = FilesystemConfig(**profile)
        except ValidationError as exc:
            raise ValueError(f"datasources.yaml: filesystems.{name} is invalid: {exc}") from exc
        catalog.register_filesystem(name, fs_config)

    sql_section = data.get("sql", {})
    defaults = sql_section.get("defaults", {})
    for name, profile in sql_section.get("connections", {}).items():
        try:
            sql_config = SqlDatabaseConfig(**{**defaults, **profile})
        except ValidationError as exc:
            raise ValueError(f"datasources.yaml: sql.connections.{name} is invalid: {exc}") from exc
        catalog.register_sql(name, sql_config)

    return catalog


class EtlServiceSettings(BaseModel):
    """This client's own upstream ETL microservice — no generic contract to
    build `boti-sweet-etl` support against, so it stays here."""

    etl_service_url: str | None = None
    etl_grpc_server: str | None = None
    etl_max_range_days: int = 90


class SecuritySettings(BaseModel):
    environment: str = "development"
    api_key: SecretStr | None = None


class ExternalServicesSettings(BaseModel):
    """This client's logistics/routing business domain: OSRM routing, a test
    geocoding place, and the IBIS PPP gateway. Purely business config."""

    osrm_service_url: str | None = None
    osrm_timeout: float = 30.0
    geolocator_test_place: str | None = None
    ibis_ppp_url: str | None = None


def load_etl_service_settings(*, env_file: str | Path | None = None) -> EtlServiceSettings:
    return load_settings(EtlServiceSettings, prefix="", env_file=env_file)


def load_security_settings(*, env_file: str | Path | None = None) -> SecuritySettings:
    return load_settings(SecuritySettings, prefix="SECURITY_", env_file=env_file)


def load_external_services_settings(
    *, env_file: str | Path | None = None
) -> ExternalServicesSettings:
    return load_settings(ExternalServicesSettings, prefix="", env_file=env_file)
