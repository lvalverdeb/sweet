"""A `boti.core.logger.Logger` facade configured from `ObservabilitySettings`."""

from __future__ import annotations

from boti.core.logger import Logger

from sweet_observability.settings import get_observability_settings


def get_logger() -> Logger:
    settings = get_observability_settings()
    return Logger.default_logger(logger_name=settings.logger_name)
