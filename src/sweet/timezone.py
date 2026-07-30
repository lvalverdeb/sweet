"""Apply the process TZ environment variable, if the platform supports it.

`TZ` is a POSIX convention, not a `{prefix}{FIELD}` suite setting: it must be
set in the real process environment (not `config/.env`, which the suite's
settings loader only ever reads into a dict for pydantic validation — it
never re-exports values back into `os.environ`). Call this explicitly once
at process startup; it is not invoked implicitly by `get_settings()`.
"""

from __future__ import annotations

import time


def apply_tz() -> None:
    if hasattr(time, "tzset"):
        time.tzset()
