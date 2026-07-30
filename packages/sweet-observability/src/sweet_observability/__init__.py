"""Observability settings and logging for the sweet suite.

Registers into the `sweet.packages` entry-point group like every other
optional package. `get_logger()` is a real facade over
`boti.core.logger.Logger`; the OpenTelemetry fields on `ObservabilitySettings`
are typed-settings-only for now — see settings.py's docstring for why.
"""

from __future__ import annotations

from sweet_observability.logging import get_logger
from sweet_observability.settings import ObservabilitySettings, get_observability_settings

NAME = "observability"

__all__ = [
    "NAME",
    "ObservabilitySettings",
    "describe",
    "get_logger",
    "get_observability_settings",
]


def describe() -> str:
    settings = get_observability_settings()
    otel = "enabled" if settings.enable_otel else "disabled"
    return f"sweet-observability is installed and registered (OTEL export {otel})"
