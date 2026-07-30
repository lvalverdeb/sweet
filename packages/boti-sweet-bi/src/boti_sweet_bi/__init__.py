"""Stub optional BI package for the boti-sweet suite.

No `boti-bi` runtime exists yet in the ecosystem for this package to wrap
(the way `boti-sweet-etl` wraps `boti_data`/`boti_dask`). Registers into the
`boti_sweet.packages` entry-point group now so the suite's wiring (extras,
registry, sandbox) works end-to-end before a real BI dependency exists.
"""

from __future__ import annotations

NAME = "bi"


def describe() -> str:
    return "boti-sweet-bi is installed and registered (stub, no BI backend wired yet)."
