"""Discovery of optional boti-sweet packages (etl, bi, ...) installed for a deployment."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib.metadata import EntryPoint, entry_points
from types import ModuleType

ENTRY_POINT_GROUP = "boti_sweet.packages"


@dataclass(frozen=True)
class PackageInfo:
    """A single optional package registered under the `boti_sweet.packages` entry-point group."""

    name: str
    entry_point: EntryPoint

    def load(self) -> ModuleType:
        module = self.entry_point.load()
        assert isinstance(module, ModuleType)
        return module


@lru_cache(maxsize=1)
def installed_packages() -> tuple[PackageInfo, ...]:
    """Optional packages installed in the current environment, e.g. `boti-sweet-etl`."""
    return tuple(
        PackageInfo(name=ep.name, entry_point=ep)
        for ep in entry_points(group=ENTRY_POINT_GROUP)
    )


def is_installed(name: str) -> bool:
    return any(package.name == name for package in installed_packages())
