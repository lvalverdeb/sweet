# boti-sweet-etl

Generic ETL entry point for the `boti-sweet` suite. This package does **not**
define its own extract/transform/load abstractions — [boti-data](https://github.com/lvalverdeb/boti-data)
already provides those (`SinkPipeline`, sinks, a sink registry) and
[boti-dask](https://github.com/lvalverdeb/boti-dask) already manages Dask
sessions from environment configuration. `boti-sweet-etl` is a thin facade
that re-exports both and wires a pipeline run to a managed session:

```python
from boti_sweet_etl import DataHelper, run_with_dask_session, SinkPipeline


def build_pipeline() -> SinkPipeline:
    return SinkPipeline(
        source=DataHelper(...),
        sink="parquet",
        sink_config={...},
    )


run_with_dask_session(build_pipeline, env_prefix="DASK_", overwrite=True)
```

`Datasources` reads named filesystem/SQL connection profiles from a YAML
file and constructs the real `boti.core.filesystem.FilesystemConfig` /
`boti_data.db.sql_config.SqlDatabaseConfig` objects directly — extracting
each profile's credentials (`fs_key`/`fs_secret`/`fs_token`,
`connection_url`) — registered into a `boti_data.ConnectionCatalog`. Neither
config class has YAML support itself (only env-prefix loaders), so this is
the YAML-loading glue on top of them, not a parallel config system:

```yaml
# datasources.yaml
filesystems:
  source:
    fs_type: s3
    fs_path: s3://my-bucket/source
    fs_key: ...
    fs_secret: ...
sql:
  defaults:               # merged under each connection below
    pool_size: 20
  connections:
    replica:
      connection_url: ...
redis:
  cache:
    host: ...
    port: 6379
```

```python
from boti_sweet_etl import Datasources

datasources = Datasources("datasources.yaml")
fs_config = datasources.filesystem("source")  # FilesystemConfig
sql_config = datasources.sql("replica")       # SqlDatabaseConfig
redis_config = datasources.redis("cache")     # RedisConfig
filesystem = datasources.catalog.filesystem("source")  # live fsspec handle
```

`RedisConfig` (`host`, `port`, `db`, `decode_responses`, `password`) has no
`ConnectionCatalog`-equivalent registry to build on — checked, there's no
Redis reference anywhere in `boti`/`boti_data`/`boti_dask` — so profiles are
just kept in a plain dict on `Datasources`, not the catalog.

`data_helper()`/`datacube()` build a `boti_data.helper.DataHelper` /
`boti_data.datacube.BaseDataCube` directly from a named SQL profile —
`DataHelper` accepts a `SqlDatabaseConfig` natively (it's one of
`DataGateway`'s own `BackendConfig` union members), so no adapter is needed:

```python
helper = datasources.data_helper("replica")     # DataHelper
cube = datasources.datacube("replica")          # BaseDataCube, from_helper(helper)
df = cube.load(table="orders")                  # or pass table= to data_helper()/datacube() itself
```

Building either eagerly creates a SQL engine (fails fast if the DBAPI driver
for that dialect isn't installed — that's a deployment concern, not a
`boti-sweet-etl` dependency, same as the `sql()` connectivity check).

See `sandbox/deployment_settings.py` for a worked example, including how a
private-IP `fs_endpoint` needs the same SSRF-allowlist trust
`FilesystemConfig.from_env_prefix(trust_env_endpoint=True)` gives on the
env-prefix path.

Optional add-on to the `boti-sweet` suite — install with:

```bash
uv sync --extra etl
```
