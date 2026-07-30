# sandbox

A stand-in for a client deployment of `boti-sweet`, for exercising suite
behavior — and modeling a real client's configuration — without needing a
real client's config or a real ETL/BI/observability package installed.

```
sandbox/
├── config/
│   ├── settings.yaml       # a pretend deployment's committed YAML defaults
│   ├── .env.example        # documents every key this sandbox reads, committed
│   └── .env                # your real values, gitignored — copy from .env.example
├── deployment_settings.py  # this "deployment"'s own config: a boti_data
│                            # ConnectionCatalog (generic) + a few ad hoc
│                            # settings models (client-specific, see below)
└── run.py                  # prints resolved settings, installed optional
                             # packages, and this deployment's connection
                             # catalog / client-specific config
```

Copy `sandbox/config/.env.example` to `sandbox/config/.env` and fill in real
values (this file is gitignored — **never commit it**). Then:

```bash
uv sync                          # boti-sweet-dummy is always present (workspace dev group)
uv run python sandbox/run.py

uv sync --all-extras              # simulate a client that needs ETL + BI + observability
uv run python sandbox/run.py

uv sync                          # back to skeleton-only
uv run python sandbox/run.py
```

By default `run.py` only constructs and validates typed config objects — no
network calls, and secrets print masked (`SecretStr`). Pass
`--check-connectivity` to additionally probe each configured
filesystem/SQL connection for real:

```bash
uv run python sandbox/run.py --check-connectivity
```

This reaches whatever `sandbox/config/.env` points at, so expect failures
(not crashes) for infrastructure that isn't reachable from wherever this
runs, or for DB dialects whose driver package isn't installed (e.g.
`pymysql`/`psycopg2` — install them yourself if you need this to fully
succeed, they're deployment-specific, not a `boti-sweet-etl` dependency).

## What's generic vs. client-specific

`deployment_settings.py` demonstrates both:

- **Generic, zero new code**: named filesystem/SQL connection profiles via
  `boti_data.ConnectionCatalog` — `catalog.load_filesystem("source", "SOURCE_")`,
  `catalog.load_sql("replica", "DB_", connection_url=...)`, etc. This is the
  ecosystem's own mechanism (see `CLAUDE.md`), not something built for this
  sandbox.
- **Client-specific, sandbox-only**: `EtlServiceSettings`, `SecuritySettings`,
  `ExternalServicesSettings` — plain models built with
  `boti_sweet_config.load_settings()` for config that's this client's own
  business domain (an upstream microservice URL, routing/geo services, an
  API key) with no generic shape to extract and no second consumer yet. This
  is the pattern to copy for a real deployment's own one-off config — see
  `CLAUDE.md`'s "client-specific config has no home in a `boti-sweet-*`
  package" section.

`boti-sweet-observability` (`packages/boti-sweet-observability/`) is a real,
if partial, package: `OO_*` settings plus `get_logger()`, a facade over
`boti.core.logger.Logger`. Its OTEL fields (`otel_grpc_endpoint`,
`enable_otel`, ...) are typed-settings-only — no OpenTelemetry SDK wired up,
since nothing in `boti`/`boti_data`/`boti_dask` exports OTEL either.

`boti-sweet-dummy` (`packages/boti-sweet-dummy/`) is a no-op package that
only exists to make optional-package discovery visible without needing a
real, heavyweight package installed — it registers itself the same way
`boti-sweet-etl`/`boti-sweet-bi`/`boti-sweet-observability` do, via the
`boti_sweet.packages` entry-point group. Edit
`sandbox/config/settings.yaml`/`.env` to see settings precedence (YAML <
`.env` < environment variable) for yourself.

## `TZ` is a special case

`TZ=America/Costa_Rica` in `sandbox/config/.env` does **nothing** by itself:
this suite's settings loader only ever reads `.env` into a dict to validate
a pydantic model — it never writes values back into the real process
environment. `TZ` must be exported in the actual process environment (shell,
systemd, `docker-compose environment:`, ...) to matter at all. `run.py`
calls `boti_sweet.apply_tz()` at startup, which then picks up `TZ` from
there via `time.tzset()` (POSIX only) — a real deployment's own entrypoint
should call `apply_tz()` the same way.
