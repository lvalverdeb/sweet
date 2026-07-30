"""Generic ETL entry point for the Boti suite.

A facade over `boti_data`'s pipeline primitives (source -> sink, with optional
enrichment) and `boti_dask`'s managed session, not a new pipeline framework:
concrete extractors/sinks live in `boti_data`, this package only curates and
wires them for suite use.
"""

from __future__ import annotations

from boti_dask import DaskSession, dask_session_from_env_prefix
from boti_data import (
    CsvSink,
    CsvSinkConfig,
    JsonlSink,
    JsonlSinkConfig,
    ParquetSink,
    SinkPipeline,
    SinkRegistry,
    SinkWriteResult,
    available_sinks,
    create_sink,
    register_sink,
)

from boti_sweet_etl.runner import run_with_dask_session

__all__ = [
    "CsvSink",
    "CsvSinkConfig",
    "DaskSession",
    "JsonlSink",
    "JsonlSinkConfig",
    "ParquetSink",
    "SinkPipeline",
    "SinkRegistry",
    "SinkWriteResult",
    "available_sinks",
    "create_sink",
    "dask_session_from_env_prefix",
    "register_sink",
    "run_with_dask_session",
]
