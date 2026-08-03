"""Stateless, calendar-relative date-window presets for bronze extraction.

An alternative to `BronzeJobConfig.watermark_field`'s *stateful* incremental
tracking (a persisted "last extracted value", advanced via a
`boti_data.watermark.WatermarkStore` after each run): these presets are
*stateless* — recomputed fresh from "today" on every run, no watermark store
involved, no history of what a previous run already saw. A job picks one
strategy or the other, never both — see `BronzeJobConfig`'s own validator.

Calendar-relative, not rolling/trailing, windows:

- `"today"`: rows dated today (`field >= today`).
- `"current_week"`: rows since the start of the current ISO calendar week
  (Monday), not a rolling 7-day window.
- `"current_month"`: rows since the first day of the current calendar month.
- `"ytd"`: rows since January 1st of the current year ("year-to-date").
- `"itd"`: "inception-to-date" — no lower bound, every row. Resolves to an
  *empty* filter dict, not a filter pinned to some sentinel/epoch date.
"""

from __future__ import annotations

import datetime
from typing import Any, Literal

RefreshPreset = Literal["today", "current_week", "current_month", "ytd", "itd"]

_PRESETS: tuple[RefreshPreset, ...] = ("today", "current_week", "current_month", "ytd", "itd")


def resolve_refresh_filter(
    preset: RefreshPreset,
    *,
    field: str,
    today: datetime.date | None = None,
) -> dict[str, Any]:
    """The `{field}__gte: ...}` filter (`boti_data.filters`' Django-QuerySet
    lookup syntax — see `Datasources.data_helper()`'s own docstring)
    implementing `preset`, as of `today`.

    `today` defaults to the real current date; pass it explicitly for
    deterministic tests. Raises `ValueError` for anything outside `"today"`/
    `"current_week"`/`"current_month"`/`"ytd"`/`"itd"`.
    """
    reference = today if today is not None else datetime.date.today()
    if preset == "today":
        start = reference
    elif preset == "current_week":
        start = reference - datetime.timedelta(days=reference.weekday())
    elif preset == "current_month":
        start = reference.replace(day=1)
    elif preset == "ytd":
        start = reference.replace(month=1, day=1)
    elif preset == "itd":
        return {}
    else:
        raise ValueError(f"Unknown refresh preset: {preset!r}. Supported: {_PRESETS}")
    return {f"{field}__gte": start.isoformat()}


__all__ = ["RefreshPreset", "resolve_refresh_filter"]
