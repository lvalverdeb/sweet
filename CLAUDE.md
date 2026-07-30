# boti-sweet

Deployable skeleton suite for the Boti ecosystem. `boti-sweet` itself concentrates
global configuration and reports which optional packages a deployment has
installed; the packages (ETL, BI, ...) are added only where a client needs them.
This repo is a uv workspace (`[tool.uv.workspace]` in the root `pyproject.toml`):
new packages live under `packages/<name>/` with their own `pyproject.toml` and
`src/<import_name>/` layout.

## Before building anything "generic"

`boti`, `boti-dask`, and `boti-data` are the foundational packages of this
ecosystem and are always installed in `.venv`. **Read their source first**
(`.venv/lib/python3.13/site-packages/{boti,boti_dask,boti_data}/`) before adding
a new abstraction here — settings loading, dotenv parsing, Dask session
management, and ETL pipeline primitives already exist there. Reimplementing
them in a `boti-sweet-*` package duplicates code and, for anything env/dotenv
related, silently drops `boti`'s security validation
(`boti.core.security.validate_environment_bindings`: rejects NUL bytes,
newlines, tabs, and invalid variable names).

Known building blocks worth checking before writing your own:

- `boti.core.settings.load_dotenv_values` / `load_prefixed_model` — secure
  dotenv parsing and env-prefixed typed-model loading.
- `boti.core.project.ProjectService` — project-root detection, `.env` discovery
  and loading.
- `boti.core.managed_resource.ManagedResource` / `boti.core.lifecycle.LifecycleCore`
  — sync/async context-manager lifecycle base classes used throughout the
  ecosystem (e.g. `DaskSession`, `SinkPipeline`).
- `boti_dask.DaskSession` / `dask_session_from_env_prefix` — managed Dask
  client/cluster sessions configured from `{PREFIX}*` env vars.
- `boti_dask.safe_compute` / `safe_gather` / `safe_persist` / ... — resilient
  wrappers around Dask operations.
- `boti_data.pipelines.SinkPipeline`, `CsvSink`/`JsonlSink`/`ParquetSink`,
  `SinkRegistry`/`create_sink`/`register_sink` — the ecosystem's
  extract-load-write pipeline framework (source = `DataHelper`/`HybridDataset`,
  sink = a `PipelineSink`).
- `boti_data.DataGateway` / `DataHelper` — data loading (SQL, Parquet, ...).

A `boti-sweet-*` package should be a thin, generic composition or facade over
these — not a parallel reimplementation. `boti-sweet-config` and
`boti-sweet-etl` under `packages/` are the reference examples: the former
wraps `boti.core.settings.load_dotenv_values` with a YAML-defaults layer (the
one thing missing upstream); the latter re-exports `boti_data`'s pipeline
primitives and `boti_dask`'s session management rather than defining its own
extract/transform/load types.

## Optional packages must not break `uv run pytest` when their extra isn't synced

`testpaths` includes `packages`, so pytest always tries to collect every
package's tests regardless of what's currently synced. A package that is a
client-facing extra (like `boti-sweet-etl`, and any future `boti-sweet-bi`)
must ship `packages/<name>/tests/conftest.py` with
`pytest.importorskip("<import_name>")` so its tests skip cleanly instead of
erroring when the extra isn't installed — see
`packages/boti-sweet-etl/tests/conftest.py`. Packages that are always
installed (`boti-sweet-config`, required; `boti-sweet-dummy`, dev-only) don't
need this. `sandbox/` is where you actually exercise both states — run it
after `uv sync` and again after `uv sync --extra etl`.

## Commands

```bash
uv sync                   # skeleton only: boti-sweet + boti-sweet-config
uv sync --extra etl       # skeleton + ETL package, for clients that need it
uv sync --all-extras      # everything, for local development across the workspace

uv run pytest
uv run ruff check .
uv run mypy src packages/*/src
```

# Claude Configuration Override
- Never append co-author credits, attribution lines, or footers to git commits.
- Force `gitAttribution` and `includeCoAuthoredBy` to false.

---