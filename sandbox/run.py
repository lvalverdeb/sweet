"""Sandbox: model what a client deployment of boti-sweet would see.

Points BOTI_SWEET_CONFIG_DIR at sandbox/config/ (a stand-in for a real
deployment's config/) instead of the repo's own config/, then prints the
resulting settings, installed optional packages, and this deployment's own
connection catalog / client-specific config (see deployment_settings.py).

    uv sync                     # boti-sweet-dummy always comes along (dev group)
    uv run python sandbox/run.py

    uv sync --all-extras        # simulate a client that needs ETL + BI + observability
    uv run python sandbox/run.py

Copy sandbox/config/.env.example to sandbox/config/.env, and
sandbox/config/datasources.yaml.example to sandbox/config/datasources.yaml
(both gitignored) and fill in real values to see this deployment's actual
configuration resolve.

By default this only constructs and validates typed config objects — no
network calls. Pass --check-connectivity to additionally probe each
filesystem/SQL connection (opt-in, since these can point at infrastructure
that isn't reachable from wherever this runs).
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from deployment_settings import (
    load_etl_service_settings,
    load_external_services_settings,
    load_security_settings,
)

from boti_sweet import apply_tz, get_settings, installed_packages

SANDBOX_CONFIG_DIR = Path(__file__).parent / "config"
SANDBOX_ENV_FILE = SANDBOX_CONFIG_DIR / ".env"
SANDBOX_DATASOURCES_FILE = SANDBOX_CONFIG_DIR / "datasources.yaml"


def print_suite_settings() -> None:
    settings = get_settings()
    print(f"environment = {settings.environment!r}")
    print(f"log_level   = {settings.log_level!r}")


def print_installed_packages() -> None:
    packages = installed_packages()
    names = [package.name for package in packages]
    print(f"installed optional packages: {names or 'none'}")
    for package in packages:
        module = package.load()
        describe = getattr(module, "describe", None)
        if describe is not None:
            print(f"  {package.name}: {describe()}")

        if package.name == "bi":
            get_clickhouse_settings = getattr(module, "get_clickhouse_settings", None)
            if get_clickhouse_settings is not None:
                ch = get_clickhouse_settings()
                print(f"  {package.name}: clickhouse host={ch.host!r} database={ch.database!r}")

        if package.name == "observability":
            get_observability_settings = getattr(module, "get_observability_settings", None)
            if get_observability_settings is not None:
                oo = get_observability_settings()
                print(
                    f"  {package.name}: logger_name={oo.logger_name!r} "
                    f"otel_service_name={oo.otel_service_name!r}"
                )


def print_connection_catalog() -> None:
    from deployment_settings import build_datasources

    try:
        datasources = build_datasources(datasources_file=SANDBOX_DATASOURCES_FILE)
    except ImportError:
        print("  skipped: boti-data not installed (uv sync --extra etl to see this)")
        return

    for name in ("etl", "source", "target", "persons"):
        try:
            fs_config = datasources.filesystem(name)
        except KeyError:
            print(f"  filesystem[{name}]: not configured (missing from datasources.yaml)")
            continue
        print(f"  filesystem[{name}]: type={fs_config.fs_type} path={fs_config.fs_path}")

    for name in ("replica", "paf"):
        try:
            sql_config = datasources.sql(name)
        except KeyError:
            print(f"  sql[{name}]: not configured (missing from datasources.yaml)")
            continue
        print(f"  sql[{name}]: query_only={sql_config.query_only} pool_size={sql_config.pool_size}")

    for name in ("cache",):
        try:
            redis_config = datasources.redis(name)
        except KeyError:
            print(f"  redis[{name}]: not configured (missing from datasources.yaml)")
            continue
        print(
            f"  redis[{name}]: host={redis_config.host} "
            f"port={redis_config.port} db={redis_config.db}"
        )


def print_client_settings() -> None:
    etl_service = load_etl_service_settings(env_file=SANDBOX_ENV_FILE)
    print(
        f"  etl service: url={etl_service.etl_service_url!r} "
        f"grpc={etl_service.etl_grpc_server!r}"
    )

    security = load_security_settings(env_file=SANDBOX_ENV_FILE)
    print(f"  security: environment={security.environment!r} api_key={security.api_key}")

    external = load_external_services_settings(env_file=SANDBOX_ENV_FILE)
    print(
        f"  external services: osrm={external.osrm_service_url!r} "
        f"ibis_ppp={external.ibis_ppp_url!r}"
    )


def check_connectivity() -> None:
    """Opt-in: actually reach the configured infrastructure. Never called by default."""
    try:
        from deployment_settings import build_datasources
        from sqlalchemy import text
    except ImportError:
        print("  skipped: boti-data not installed (uv sync --extra etl to see this)")
        return

    datasources = build_datasources(datasources_file=SANDBOX_DATASOURCES_FILE)
    catalog = datasources.catalog

    for name in ("etl", "source", "target", "persons"):
        try:
            fs_config = catalog.filesystem_config(name)
        except KeyError:
            continue
        try:
            catalog.filesystem(name).ls(fs_config.fs_path, detail=False)
        except Exception as exc:  # noqa: BLE001 - best-effort probe, report and move on
            print(f"  filesystem[{name}]: FAILED ({exc})")
        else:
            print(f"  filesystem[{name}]: OK")

    for name in ("replica", "paf"):
        try:
            catalog.sql_config(name)
        except KeyError:
            continue
        try:
            with catalog.create_sql_resource(name) as resource, resource.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as exc:  # noqa: BLE001 - best-effort probe, report and move on
            print(f"  sql[{name}]: FAILED ({exc})")
        else:
            print(f"  sql[{name}]: OK")

    for name in ("cache",):
        try:
            redis_config = datasources.redis(name)
        except KeyError:
            continue
        # A bare TCP reachability check — no redis client dependency added
        # just for this opt-in probe, so it can't verify the RESP protocol
        # or auth, only that something is listening on host:port.
        import socket

        try:
            with socket.create_connection((redis_config.host, redis_config.port), timeout=3):
                pass
        except OSError as exc:
            print(f"  redis[{name}]: FAILED ({exc})")
        else:
            print(f"  redis[{name}]: OK (TCP reachable, protocol not verified)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check-connectivity",
        action="store_true",
        help="Also probe each configured filesystem/SQL connection (network calls).",
    )
    args = parser.parse_args()

    os.environ.setdefault("BOTI_SWEET_CONFIG_DIR", str(SANDBOX_CONFIG_DIR))
    apply_tz()
    print("--- timezone ---")
    print(
        f"  process TZ env var = {os.environ.get('TZ')!r}  "
        "(NOT read from sandbox/config/.env — see module docstring)"
    )

    print("--- suite settings ---")
    print_suite_settings()

    print("--- installed optional packages ---")
    print_installed_packages()

    print("--- connection catalog (settings only, no network calls) ---")
    print_connection_catalog()

    print("--- client-specific settings (sandbox/deployment_settings.py) ---")
    print_client_settings()

    if args.check_connectivity:
        print("--- connectivity checks ---")
        check_connectivity()


if __name__ == "__main__":
    main()
