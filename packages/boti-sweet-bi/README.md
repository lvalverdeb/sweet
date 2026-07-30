# boti-sweet-bi

Mostly a stub, plus one real capability: `ClickHouseSettings`. No `boti-bi`
runtime exists yet in the Boti ecosystem for this package to wrap (the way
`boti-sweet-etl` wraps `boti_data`/`boti_dask`), but ClickHouse is a plausible
generic BI backend for any deployment, so its typed settings shape lives here
(no ClickHouse client dependency added, just the config shape):

```python
from boti_sweet_bi import get_clickhouse_settings

settings = get_clickhouse_settings()  # host, port, database, user, password, secure, verify
```

Loaded the same way `boti_sweet.settings.get_settings()` is: YAML defaults
from `{BOTI_SWEET_CONFIG_DIR}/clickhouse.yaml`, overridden by `CLICKHOUSE_*`
in `{BOTI_SWEET_CONFIG_DIR}/.env`, overridden by the process environment.

It also registers into the `boti_sweet.packages` entry-point group, so the
suite's optional-package wiring (extras, registry discovery, `sandbox/`) is
exercised end-to-end:

```toml
[project.entry-points."boti_sweet.packages"]
bi = "boti_sweet_bi"
```

Install with:

```bash
uv sync --extra bi
```

Client-specific BI config (a particular dashboard tool, a specific client's
routing/business logic, ...) does **not** belong here — see `sandbox/` for
where that kind of deployment-specific config lives instead. When a real BI
runtime dependency exists, follow `boti-sweet-etl`'s shape: depend on it
directly, re-export/wrap its primitives here rather than reimplementing them.
