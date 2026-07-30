"""Glue: run a `SinkPipeline` write inside a `boti_dask`-managed Dask session.

`boti_data` already provides the pipeline (source/sink/enrich) and `boti_dask`
already provides managed Dask sessions configured from environment variables;
this only wires the two together so callers don't reimplement session
lifecycle around every pipeline run.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from boti_data import SinkPipeline, SinkWriteResult
from boti_dask import dask_session_from_env_prefix


def run_with_dask_session(
    pipeline_factory: Callable[[], SinkPipeline],
    *,
    env_prefix: str = "DASK_",
    env_file: str | Path | None = None,
    **write_options: Any,
) -> SinkWriteResult:
    """Build a `SinkPipeline` and run its `write()` inside a Dask session
    configured from `{env_prefix}*` environment variables.
    """
    session = dask_session_from_env_prefix(env_prefix, env_file=env_file)
    with session, pipeline_factory() as pipeline:
        return pipeline.write(**write_options)
