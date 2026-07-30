"""Minimal reference optional package for boti-sweet.

Demonstrates the contract a real optional package (etl, bi, ...) implements to
plug into the suite: declare an entry point in the `boti_sweet.packages` group
so `boti_sweet.registry.installed_packages()` can discover it. Dev/sandbox
only — never installed for a client deployment.
"""

from __future__ import annotations

NAME = "dummy"


def describe() -> str:
    return "boti-sweet-dummy is installed and registered."
