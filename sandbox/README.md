# sandbox

A stand-in for a client deployment of `sweet`, for exercising suite
behavior — and modeling a real client's configuration — without needing a
real client's config or a real ETL/BI/observability package installed.

```
sandbox/
├── config/
│   ├── settings.yaml              # a pretend deployment's committed YAML defaults
│   ├── .env.example               # documents every non-datasource key, committed
│   ├── .env                       # your real values, gitignored
│   ├── datasources.yaml.example   # documents filesystem/SQL/Redis profiles, committed
│   └── datasources.yaml           # your real connection profiles, gitignored
├── deployment_settings.py  # this "deployment"'s own config: delegates
│                            # datasources.yaml loading to sweet_etl.
│                            # Datasources (generic) + a few ad hoc settings
│                            # models loaded from .env (client-specific)
├── run.py                  # prints resolved settings, installed optional
│                            # packages, and this deployment's connection
│                            # catalog / client-specific config
└── notebooks/               # Jupyter notebooks using Datasources.datacube()
    └── datacubes.ipynb       # see notebooks/README.md
```

Copy `sandbox/config/.env.example` to `sandbox/config/.env`, and
`sandbox/config/datasources.yaml.example` to
`sandbox/config/datasources.yaml`, then fill in real values (both files are
gitignored — **never commit either**). Then:

```bash
uv sync                          # sweet-dummy is always present (workspace dev group)
uv run python sandbox/run.py

uv sync --all-extras              # simulate a client that needs ETL + BI + observability
uv run python sandbox/run.py

uv sync                          # back to skeleton-only
uv run python sandbox/run.py
```

By default `run.py` only constructs and validates typed config objects — no
network calls, and secrets print masked (`SecretStr`). Pass
`--check-connectivity` to additionally probe each configured
filesystem/SQL/Redis connection for real (Redis gets a bare TCP reachability
check only — no `redis` client dependency added just for this opt-in probe):

```bash
uv run python sandbox/run.py --check-connectivity
```

This reaches whatever `sandbox/config/datasources.yaml` points at, so expect
failures (not crashes) for infrastructure that isn't reachable from wherever
this runs, or for DB dialects whose driver package isn't installed (e.g.
`pymysql`/`psycopg2` — install them yourself if you need this to fully
succeed, they're deployment-specific, not a `sweet-etl` dependency).

## Notebooks

`notebooks/datacubes.ipynb` uses `Datasources.datacube()`/`.data_helper()`
interactively — see `notebooks/README.md` for kernel setup.

## What's generic vs. client-specific

`deployment_settings.py` demonstrates both:

- **Fully generic, lives in `sweet-etl` now**: `build_datasources` just
  delegates to `sweet_etl.Datasources`, which reads named
  filesystem/SQL/Redis connection profiles from `datasources.yaml` and
  constructs `boti.core.filesystem.FilesystemConfig` / `boti_data.db.
  sql_config.SqlDatabaseConfig` / `sweet_etl.RedisConfig` directly (the
  first two registered into a `boti_data.ConnectionCatalog`; Redis has no
  ecosystem registry to build on, so it's a plain dict). This never
  hardcoded any of this deployment's profile names, so it isn't sandbox-only
  glue — it moved to the generic package. See
  `packages/sweet-etl/README.md` and `CLAUDE.md`.
- **Client-specific, sandbox-only**: `EtlServiceSettings`, `SecuritySettings`,
  `ExternalServicesSettings` — plain models built with
  `sweet_config.load_settings()` for config that's this client's own
  business domain (an upstream microservice URL, routing/geo services, an
  API key) with no generic shape to extract and no second consumer yet. This
  is the pattern to copy for a real deployment's own one-off config — see
  `CLAUDE.md`'s "client-specific config has no home in a `sweet-*`
  package" section.

`sweet-observability` (`packages/sweet-observability/`) is a real,
if partial, package: `OO_*` settings plus `get_logger()`, a facade over
`boti.core.logger.Logger`. Its OTEL fields (`otel_grpc_endpoint`,
`enable_otel`, ...) are typed-settings-only — no OpenTelemetry SDK wired up,
since nothing in `boti`/`boti_data`/`boti_dask` exports OTEL either.

`sweet-dummy` (`packages/sweet-dummy/`) is a no-op package that
only exists to make optional-package discovery visible without needing a
real, heavyweight package installed — it registers itself the same way
`sweet-etl`/`sweet-bi`/`sweet-observability` do, via the
`sweet.packages` entry-point group. Edit
`sandbox/config/settings.yaml`/`.env` to see settings precedence (YAML <
`.env` < environment variable) for yourself.

## `TZ` is a special case

`TZ=America/Costa_Rica` in `sandbox/config/.env` does **nothing** by itself:
this suite's settings loader only ever reads `.env` into a dict to validate
a pydantic model — it never writes values back into the real process
environment. `TZ` must be exported in the actual process environment (shell,
systemd, `docker-compose environment:`, ...) to matter at all. `run.py`
calls `sweet.apply_tz()` at startup, which then picks up `TZ` from
there via `time.tzset()` (POSIX only) — a real deployment's own entrypoint
should call `apply_tz()` the same way.
