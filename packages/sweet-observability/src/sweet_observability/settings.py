"""Observability settings.

`boti.core.logger.Logger` has no OpenTelemetry export of its own (checked —
no `otel`/OTLP support anywhere in `boti`/`boti_data`/`boti_dask`), so unlike
`sweet_etl` or `sweet_bi.ClickHouseSettings` there's no upstream
primitive to wrap for the `otel_*` fields below: this only defines their
typed shape. No OpenTelemetry SDK dependency is added — that's for a real
exporter integration to add later, once something actually consumes it.
`logger_name` is the one field with a real consumer today: see `logging.py`.
"""

from __future__ import annotations

from pydantic import BaseModel
from sweet_config import default_config_dir, load_settings

ENV_PREFIX = "OO_"


class ObservabilitySettings(BaseModel):
    logger_name: str | None = None
    enable_otel: bool = False
    otel_grpc_endpoint: str | None = None
    otel_service_name: str | None = None
    otel_stream_name: str | None = None
    otel_insecure: bool = False


def get_observability_settings() -> ObservabilitySettings:
    config_dir = default_config_dir()
    return load_settings(
        ObservabilitySettings,
        prefix=ENV_PREFIX,
        yaml_file=config_dir / "observability.yaml",
        env_file=config_dir / ".env",
    )
