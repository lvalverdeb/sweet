"""A `boti.core.logger.Logger` facade configured from `ObservabilitySettings`."""

from __future__ import annotations

import logging

from boti.core.logger import Logger

from sweet_observability.settings import get_observability_settings


def _resolve_log_level(level: str) -> int:
    try:
        return logging.getLevelNamesMapping()[level.upper()]
    except KeyError:
        raise ValueError(
            f"Invalid log level {level!r} in OO_LOG_LEVEL. "
            f"Must be one of {sorted(logging.getLevelNamesMapping())}."
        ) from None


def get_logger(*, log_level: str | None = None) -> Logger:
    """Build a `Logger` from `ObservabilitySettings`.

    `log_level` overrides `ObservabilitySettings.log_level` (`OO_LOG_LEVEL`)
    when passed — the hook a deployment's own bootstrap uses to feed in a
    suite-wide log level (e.g. `sweet.settings.SuiteSettings.log_level`)
    without `sweet_observability` depending on `sweet` core to read it
    itself; see `sandbox/run.py`.
    """
    settings = get_observability_settings()
    effective_level = log_level if log_level is not None else settings.log_level

    # Mirrors `Logger.default_logger()`'s own defaults ("logs", logging.INFO)
    # explicitly rather than relying on them silently, same reasoning as
    # `BronzeCube.save_to_parquet()`'s `return_type` default.
    return Logger.default_logger(
        log_dir=settings.log_dir if settings.log_dir is not None else "logs",
        logger_name=settings.logger_name,
        log_file=settings.log_file,
        log_level=_resolve_log_level(effective_level)
        if effective_level is not None
        else logging.INFO,
    )
