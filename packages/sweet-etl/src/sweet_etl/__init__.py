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
from boti_data.datacube import BaseDataCube
from boti_data.helper import DataHelper

from sweet_etl.bronze import BronzeCube
from sweet_etl.bronze_jobs import BronzeDestination, BronzeJobConfig, BronzeJobs
from sweet_etl.datasources import Datasources
from sweet_etl.redis_config import RedisConfig
from sweet_etl.runner import run_with_dask_session
from sweet_etl.silver import SilverCube
from sweet_etl.silver_jobs import SilverJobConfig, SilverJobs

__all__ = [
    "BaseDataCube",
    "BronzeCube",
    "BronzeDestination",
    "BronzeJobConfig",
    "BronzeJobs",
    "CsvSink",
    "CsvSinkConfig",
    "DaskSession",
    "DataHelper",
    "Datasources",
    "JsonlSink",
    "JsonlSinkConfig",
    "ParquetSink",
    "RedisConfig",
    "SilverCube",
    "SilverJobConfig",
    "SilverJobs",
    "SinkPipeline",
    "SinkRegistry",
    "SinkWriteResult",
    "available_sinks",
    "create_sink",
    "dask_session_from_env_prefix",
    "register_sink",
    "run_with_dask_session",
]
