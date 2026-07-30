"""boti-sweet: the deployable skeleton suite for the Boti ecosystem.

Concentrates global configuration (`get_settings`) and reports which optional
packages (ETL, BI, ...) are installed for a given deployment (`installed_packages`).
"""

from __future__ import annotations

from boti_sweet.registry import PackageInfo, installed_packages, is_installed
from boti_sweet.settings import SuiteSettings, get_settings

__all__ = [
    "PackageInfo",
    "SuiteSettings",
    "get_settings",
    "installed_packages",
    "is_installed",
]
