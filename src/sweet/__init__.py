"""sweet: the deployable skeleton suite for the Boti ecosystem.

Concentrates global configuration (`get_settings`) and reports which optional
packages (ETL, BI, ...) are installed for a given deployment (`installed_packages`).
"""

from __future__ import annotations

from sweet.registry import PackageInfo, installed_packages, is_installed
from sweet.settings import SuiteSettings, get_settings
from sweet.timezone import apply_tz

__all__ = [
    "PackageInfo",
    "SuiteSettings",
    "apply_tz",
    "get_settings",
    "installed_packages",
    "is_installed",
]
