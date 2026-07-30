"""This specific deployment's own configuration.

Everything here is client-specific wiring that has no home in a generic
`boti-sweet-*` package — either because it's built entirely from existing
`boti`/`boti_data` primitives (the connection catalog) or because it's this
client's own business config with no generic shape to extract yet (ETL
service integration, routing/geo, security). See CLAUDE.md for the
"generalize only once there's a second real consumer" rule this follows.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from boti.core.settings import load_dotenv_values
from boti_sweet_config import load_settings
from pydantic import BaseModel, SecretStr, ValidationError

if TYPE_CHECKING:
    from boti_data import ConnectionCatalog


def _resolve_env_value(name: str, *, env_file: str | Path | None) -> str | None:
    """Look up `name`, process environment first, falling back to `env_file`.

    `load_settings`/`load_prefixed_model` merge env_file + os.environ this
    same way internally; this exists because SQL profile URLs are read
    directly (see SQL_PROFILES below) rather than through a settings model.
    """
    value = os.environ.get(name)
    if value:
        return value
    if env_file is not None and Path(env_file).is_file():
        return load_dotenv_values(Path(env_file)).get(name)
    return None

# name -> env prefix, one boti.core.filesystem.FilesystemConfig each.
# FilesystemConfig's own fields are named fs_type/fs_path/... (the "FS_"
# already lives in the field name), so the prefix here is just "ETL_", not
# "ETL_FS_" — prefix + "FS_PATH".upper() = "ETL_" + "FS_PATH" = "ETL_FS_PATH".
FILESYSTEM_PROFILES = {
    "etl": "ETL_",
    "source": "SOURCE_",
    "target": "TARGET_",
    "persons": "PERSONS_",
}

# name -> env var holding the full connection URL. Shared "DB_" pool tuning
# (DB_POOL_SIZE, DB_MAX_OVERFLOW, ...) applies to every named connection;
# REPLICA_DB_URL/PAF_DB_URL don't follow the {PREFIX}CONNECTION_URL
# convention SqlDatabaseSettings expects, so the URL is passed as an
# explicit override on top of that shared prefix instead.
SQL_PROFILES = {
    "replica": "REPLICA_DB_URL",
    "paf": "PAF_DB_URL",
}


def build_connection_catalog(*, env_file: str | Path | None = None) -> ConnectionCatalog:
    # Lazy: boti-data (and its sqlalchemy/dask/pandas/polars dependency chain)
    # only comes with the "etl" extra, which this sandbox must work without.
    from boti_data import ConnectionCatalog

    catalog = ConnectionCatalog()

    for name, prefix in FILESYSTEM_PROFILES.items():
        # fs_path is a required field with no default: a profile with none of
        # its {PREFIX}FS_* vars set (e.g. no sandbox/config/.env yet) raises
        # here rather than returning an empty config, so skip it explicitly —
        # same "not configured, move on" behavior as the SQL profiles below.
        try:
            catalog.load_filesystem(name, prefix, env_file=env_file, trust_env_endpoint=True)
        except ValidationError:
            continue

    for name, url_env_var in SQL_PROFILES.items():
        connection_url = _resolve_env_value(url_env_var, env_file=env_file)
        if not connection_url:
            continue
        # query_only is already SqlDatabaseSettings' own default; written out
        # explicitly so a future reader has to consciously change it rather
        # than rely on an invisible default (e.g. once "paf" needs writes).
        catalog.load_sql(
            name, "DB_", env_file=env_file, connection_url=connection_url, query_only=True
        )

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
