# boti-sweet-etl

Generic ETL entry point for the `boti-sweet` suite. This package does **not**
define its own extract/transform/load abstractions — [boti-data](https://github.com/lvalverdeb/boti-data)
already provides those (`SinkPipeline`, sinks, a sink registry) and
[boti-dask](https://github.com/lvalverdeb/boti-dask) already manages Dask
sessions from environment configuration. `boti-sweet-etl` is a thin facade
that re-exports both and wires a pipeline run to a managed session:

```python
from boti_data import DataHelper
from boti_sweet_etl import run_with_dask_session, SinkPipeline


def build_pipeline() -> SinkPipeline:
    return SinkPipeline(
        source=DataHelper(...),
        sink="parquet",
        sink_config={...},
    )


run_with_dask_session(build_pipeline, env_prefix="DASK_", overwrite=True)
```

Optional add-on to the `boti-sweet` suite — install with:

```bash
uv sync --extra etl
```
