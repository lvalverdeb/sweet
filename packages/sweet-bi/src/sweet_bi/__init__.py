"""Stub optional BI package for the sweet suite.

No `boti-bi` runtime exists yet in the ecosystem for this package to wrap
(the way `sweet-etl` wraps `boti_data`/`boti_dask`). Registers into the
`sweet.packages` entry-point group now so the suite's wiring (extras,
registry, sandbox) works end-to-end before a real BI dependency exists.
ClickHouse settings are its first real capability — a plausible generic BI
backend, unlike client-specific config (routing, security, ...), which stays
at the deployment level.
"""

from __future__ import annotations

from sweet_bi.settings import ClickHouseSettings, get_clickhouse_settings

NAME = "bi"

__all__ = ["NAME", "ClickHouseSettings", "describe", "get_clickhouse_settings"]


def describe() -> str:
    return "sweet-bi is installed and registered (stub, no BI backend wired yet)."
