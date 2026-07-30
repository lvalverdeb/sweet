"""Sandbox: model what a client deployment of boti-sweet would see.

Points BOTI_SWEET_CONFIG_DIR at sandbox/config/ (a stand-in for a real
deployment's config/) instead of the repo's own config/, then prints the
resulting settings and which optional packages are installed.

    uv sync                # boti-sweet-dummy always comes along (dev group)
    uv run python sandbox/run.py

    uv sync --extra etl    # simulate a client that also needs ETL
    uv run python sandbox/run.py
"""

from __future__ import annotations

import os
from pathlib import Path

from boti_sweet import get_settings, installed_packages

SANDBOX_CONFIG_DIR = Path(__file__).parent / "config"


def main() -> None:
    os.environ.setdefault("BOTI_SWEET_CONFIG_DIR", str(SANDBOX_CONFIG_DIR))

    settings = get_settings()
    print(f"environment = {settings.environment!r}")
    print(f"log_level   = {settings.log_level!r}  (staging from settings.yaml, "
          "overridden to WARNING by .env)")

    packages = installed_packages()
    names = [package.name for package in packages]
    print(f"installed optional packages: {names or 'none'}")
    for package in packages:
        module = package.load()
        describe = getattr(module, "describe", None)
        if describe is not None:
            print(f"  {package.name}: {describe()}")


if __name__ == "__main__":
    main()
