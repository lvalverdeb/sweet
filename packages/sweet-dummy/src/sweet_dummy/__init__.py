"""Minimal reference optional package for sweet.

Demonstrates the contract a real optional package (etl, bi, ...) implements to
plug into the suite: declare an entry point in the `sweet.packages` group
so `sweet.registry.installed_packages()` can discover it. Dev/sandbox
only — never installed for a client deployment.
"""

from __future__ import annotations

NAME = "dummy"


def describe() -> str:
    return "sweet-dummy is installed and registered."
