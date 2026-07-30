"""This specific deployment's own configuration.

Everything here is client-specific wiring that has no home in a generic
`boti-sweet-*` package — except `build_connection_catalog`, which delegates
to `boti_sweet_etl.Datasources`: that logic was never client-specific (it
doesn't hardcode any of this deployment's profile names), so it lives in the
generic package, not here. What's genuinely client-specific — ETL service
integration, routing/geo, security — has no generic shape to extract yet.
See CLAUDE.md for the "generalize only once there's a second real consumer"
rule this follows.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from boti_sweet_config import load_settings
from pydantic import BaseModel, SecretStr

if TYPE_CHECKING:
    from boti_data.connection_catalog import ConnectionCatalog


def build_connection_catalog(*, datasources_file: str | Path) -> ConnectionCatalog:
    # Lazy: boti-sweet-etl (and its boti-data/sqlalchemy/dask/pandas/polars
    # dependency chain) only comes with the "etl" extra, which this sandbox
    # must work without.
    from boti_sweet_etl import Datasources

    return Datasources(datasources_file).catalog


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
