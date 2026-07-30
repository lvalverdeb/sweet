# boti-sweet-observability

Typed settings for observability config (`OO_*`), plus one real capability:
`get_logger()`, a facade over `boti.core.logger.Logger.default_logger`.

```python
from boti_sweet_observability import get_logger, get_observability_settings

logger = get_logger()  # Logger.default_logger(logger_name=settings.logger_name)
settings = get_observability_settings()  # logger_name, enable_otel, otel_*
```

Loaded the same way `boti_sweet.settings.get_settings()` is: YAML defaults
from `{BOTI_SWEET_CONFIG_DIR}/observability.yaml`, overridden by `OO_*` in
`{BOTI_SWEET_CONFIG_DIR}/.env`, overridden by the process environment.

**The `otel_*` fields are typed-settings-only.** `boti`/`boti_data`/`boti_dask`
have no OpenTelemetry export of their own to wrap (checked — see
`settings.py`'s docstring), and no OpenTelemetry SDK dependency is added
here. `ObservabilitySettings.enable_otel`/`otel_grpc_endpoint`/etc. just
give that config a typed home; wiring an actual OTLP exporter is future
work, once something real consumes it — same "generalize once there's a
real consumer" rule as everything else in this suite (see `CLAUDE.md`).

Install with:

```bash
uv sync --extra observability
```
