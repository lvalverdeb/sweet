# boti-sweet

Deployable skeleton suite for the [Boti](https://github.com/lvalverdeb/boti) ecosystem.
`boti-sweet` itself concentrates global configuration (`config/settings.yaml`, `config/.env`)
and reports which optional packages a deployment has installed; the packages themselves
(ETL, BI, ...) are added only where a client needs them.

This repo is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/):

```
.
├── pyproject.toml            # workspace root + the boti-sweet skeleton app
├── config/                   # concentrated settings for a deployment
│   ├── settings.yaml         # committed defaults
│   └── .env.example          # copy to .env for local/per-deployment overrides
├── src/boti_sweet/           # settings loader + optional-package registry
└── packages/
    ├── boti-sweet-config/    # generic typed-settings base (YAML + .env + env vars)
    └── boti-sweet-etl/       # generic extract/transform/load abstractions (optional)
```

`boti-sweet-config` is a required, always-installed dependency. Optional packages are
declared as extras and discovered at runtime via the `boti_sweet.packages` entry-point
group (see `boti_sweet.registry.installed_packages`):

```python
from boti_sweet import get_settings, installed_packages

settings = get_settings()
[package.name for package in installed_packages()]  # e.g. ["etl"]
```

## Setup

```bash
uv sync                   # skeleton only: boti-sweet + boti-sweet-config
uv sync --extra etl       # skeleton + ETL package, for clients that need it
uv sync --all-extras      # everything, for local development across the workspace
```

## Development

```bash
uv run pytest
uv run ruff check .
uv run mypy src packages/*/src
```
