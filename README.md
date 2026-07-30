# boti-sweet

"Sweet" is just a catchy stand-in for "suite" — this is the deployable skeleton
**suite** for the [Boti](https://github.com/lvalverdeb/boti) ecosystem.
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
├── sandbox/                  # a pretend client deployment, for local exploration
└── packages/
    ├── boti-sweet-config/    # generic typed-settings base (YAML + .env + env vars)
    ├── boti-sweet-etl/       # facade over boti-data/boti-dask pipelines (optional)
    ├── boti-sweet-bi/        # BI stub — wired in, no BI runtime yet (optional)
    ├── boti-sweet-observability/  # OO_* settings + Logger facade (optional)
    └── boti-sweet-dummy/     # no-op optional package, dev-only (see sandbox/)
```

`boti-sweet-config` is a required, always-installed dependency. Optional packages are
declared as extras and discovered at runtime via the `boti_sweet.packages` entry-point
group (see `boti_sweet.registry.installed_packages`):

```python
from boti_sweet import get_settings, installed_packages

settings = get_settings()
[package.name for package in installed_packages()]  # e.g. ["etl"]
```

## Optional packages

| Extra | Package | Status |
| --- | --- | --- |
| `etl` | [`boti-sweet-etl`](packages/boti-sweet-etl) | Facade over `boti-data`/`boti-dask` pipeline primitives. |
| `bi` | [`boti-sweet-bi`](packages/boti-sweet-bi) | Mostly a placeholder — no BI runtime wired yet — but has one real capability: `ClickHouseSettings`. |
| `observability` | [`boti-sweet-observability`](packages/boti-sweet-observability) | `get_logger()` (real facade over `boti.core.logger.Logger`); OTEL fields are typed-settings-only, no exporter wired yet. |

<!-- TODO: expand this table as these packages grow past their current
     capabilities, and add new rows here whenever a new optional
     package/extra is introduced. -->

## Setup

```bash
uv sync                       # skeleton only: boti-sweet + boti-sweet-config
uv sync --extra etl           # skeleton + ETL package, for clients that need it
uv sync --extra bi            # skeleton + BI package
uv sync --extra observability # skeleton + observability package
uv sync --all-extras          # everything, for local development across the workspace
```

## Sandbox

`sandbox/` models a client deployment locally — its own `config/`, a
`deployment_settings.py` composing generic pieces (a `boti_data.
ConnectionCatalog` for named filesystem/SQL connections) with client-specific
config that has no home in a generic package, and a `run.py` that reports
all of it, without needing a real client's config or a real ETL/BI package
installed. See `sandbox/README.md`.

```bash
uv run python sandbox/run.py
uv run python sandbox/run.py --check-connectivity  # opt-in, reaches real infra
```

## Development

```bash
uv run pytest
uv run ruff check .
uv run mypy src packages/*/src sandbox
```
