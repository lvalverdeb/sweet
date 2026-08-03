import datetime

import pytest
from sweet_etl.refresh_presets import resolve_refresh_filter

# A Wednesday, deliberately not the 1st of the month/year, so week/month/
# year start dates are all genuinely different from `TODAY` and from
# each other.
TODAY = datetime.date(2026, 3, 18)


def test_today_filters_from_today() -> None:
    assert resolve_refresh_filter("today", field="last_activity_dt", today=TODAY) == {
        "last_activity_dt__gte": "2026-03-18"
    }


def test_current_week_filters_from_monday() -> None:
    assert resolve_refresh_filter("current_week", field="last_activity_dt", today=TODAY) == {
        "last_activity_dt__gte": "2026-03-16"
    }


def test_current_month_filters_from_first_of_month() -> None:
    assert resolve_refresh_filter("current_month", field="last_activity_dt", today=TODAY) == {
        "last_activity_dt__gte": "2026-03-01"
    }


def test_ytd_filters_from_january_first() -> None:
    assert resolve_refresh_filter("ytd", field="last_activity_dt", today=TODAY) == {
        "last_activity_dt__gte": "2026-01-01"
    }


def test_itd_has_no_lower_bound() -> None:
    assert resolve_refresh_filter("itd", field="last_activity_dt", today=TODAY) == {}


def test_current_week_when_today_is_monday_starts_today() -> None:
    monday = datetime.date(2026, 3, 16)
    assert resolve_refresh_filter("current_week", field="d", today=monday) == {
        "d__gte": "2026-03-16"
    }


def test_defaults_to_real_current_date_when_today_not_passed() -> None:
    result = resolve_refresh_filter("today", field="d")
    assert result == {"d__gte": datetime.date.today().isoformat()}


def test_unknown_preset_raises() -> None:
    with pytest.raises(ValueError, match="Unknown refresh preset"):
        resolve_refresh_filter("last_quarter", field="d")  # type: ignore[arg-type]
