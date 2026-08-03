"""HTTP trigger for this deployment's bronze extract fleet.

`POST /extract/run-extract/` runs every job in `sandbox/config/
bronze_jobs.yaml` via `sweet_etl.run_bronze_fleet()`. Deployment-level, not
`sweet_etl` core, on purpose: an HTTP trigger is one of several equally
valid ways to fire the same run (cron, Airflow, a queue) — see
`sweet_etl/extract_runner.py`'s own module docstring for the actual
fleet-orchestration logic this only exposes over HTTP. `fastapi`/`uvicorn`
are `dev`-group-only dependencies for exactly this reason: they're sandbox
tooling, not something every `sweet-etl` install needs.

Run (from the repo root — `sandbox/` isn't a package, hence `--app-dir`):
    uv run uvicorn api:app --app-dir sandbox --reload --port 7000

Try (matches this module's own `RunExtractRequest` shape):
    curl -X POST 'http://127.0.0.1:7000/extract/run-extract/' \\
      -H 'accept: application/json' -H 'Content-Type: application/json' \\
      -d '{"period": "ytd", "params": {"debug": true, "reverse_order": true}}'

    curl -X POST 'http://127.0.0.1:7000/extract/redo-partition/' \\
      -H 'accept: application/json' -H 'Content-Type: application/json' \\
      -d '{"job": "events_history", "partition_value": "2026-08-01"}'
"""

from __future__ import annotations

import logging
from pathlib import Path

from deployment_settings import build_datasources
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sweet_etl import BronzeJobs, RefreshPreset, redo_partition, run_bronze_fleet

SANDBOX_DIR = Path(__file__).parent
SANDBOX_CONFIG_DIR = SANDBOX_DIR / "config"

_log = logging.getLogger(__name__)

# A handler must exist for setLevel() below to actually surface anything:
# without one, Python's logging module silently drops INFO/DEBUG records
# (only WARNING+ reaches console output, via the "handler of last resort")
# regardless of what level a logger is set to. Verified directly -- an
# earlier version of this module set the level with no basicConfig() call
# and the "show_progress"/"debug" logging documented below never appeared.
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

app = FastAPI(title="sweet-etl extract service")


class RunExtractParams(BaseModel):
    """Run-control knobs, matching the curl contract this endpoint was
    modeled on. Only some map onto anything `sweet_etl` does today — see
    `_UNWIRED_PARAMS` below and this endpoint's own docstring; the rest are
    accepted (so a caller sending the full original payload doesn't get a
    validation error) but have no effect, and are echoed back in the
    response's `unwired_params` so that's never silent.
    """

    debug: bool = False
    verbose: bool = False
    show_progress: bool = False
    ignore_missing: bool = False
    reverse_order: bool = False
    # max_threads/max_workers both map onto run_bronze_fleet()'s single
    # max_workers knob (concurrent *jobs*, not DB connections -- see
    # extract_runner's own docstring for why no pool-size clamp is needed
    # here). Accepting both names matches the original curl contract this
    # endpoint was modeled on; if both are set, the smaller wins.
    max_threads: int | None = None
    max_workers: int | None = None
    # Accepted, not yet wired -- see run_extract()'s own docstring.
    overwrite: bool = False
    max_age_minutes: int | None = None
    history_days_threshold: int | None = None


class RunExtractRequest(BaseModel):
    period: RefreshPreset | None = None
    params: RunExtractParams = Field(default_factory=RunExtractParams)


class JobResultOut(BaseModel):
    name: str
    ok: bool
    path: str | None = None
    error: str | None = None


class RunExtractResponse(BaseModel):
    succeeded: list[str]
    failed: list[str]
    results: list[JobResultOut]
    unwired_params: list[str]


class RedoPartitionRequest(BaseModel):
    job: str
    partition_value: str


_UNWIRED_PARAMS = (
    "overwrite",
    "max_age_minutes",
    "history_days_threshold",
)


def _effective_max_workers(params: RunExtractParams) -> int | None:
    """The smaller of `max_threads`/`max_workers` when either is set, else
    `None` (sequential — `run_bronze_fleet()`'s own default)."""
    candidates = [v for v in (params.max_threads, params.max_workers) if v is not None]
    return min(candidates) if candidates else None


def _build_jobs() -> BronzeJobs:
    datasources = build_datasources(datasources_file=SANDBOX_CONFIG_DIR / "datasources.yaml")
    watermark_store = datasources.watermark_store(
        filesystem_profile="etl", path="_watermarks.json"
    )
    return BronzeJobs(
        SANDBOX_CONFIG_DIR / "bronze_jobs.yaml", datasources, watermark_store=watermark_store
    )


@app.post("/extract/run-extract/", response_model=RunExtractResponse)
def run_extract(request: RunExtractRequest) -> RunExtractResponse:
    """Runs every configured bronze job, applying `period` as a
    refresh-preset override for the run — see `sweet_etl.run_bronze_fleet()`
    's own docstring for exactly which jobs that reaches (only
    `refresh_field`-configured ones; watermark-based and unfiltered jobs are
    unaffected by `period`).

    Param mapping:
    - `debug`/`verbose` -> `sweet_etl`'s logger set to `DEBUG` for this run.
    - `show_progress` -> the logger set to at least `INFO`, which is what
      makes `run_bronze_fleet()`'s own `"[i/n] running bronze job ..."`
      lines appear — there's no separate progress-bar mechanism, this *is*
      "show progress" for a synchronous, one-request-per-run endpoint.
    - `ignore_missing`/`reverse_order` -> passed straight through to
      `run_bronze_fleet()`.
    - `max_threads`/`max_workers` -> the smaller of the two (if either is
      set) becomes `run_bronze_fleet()`'s `max_workers` — concurrent *jobs*,
      not a DB connection cap; see `extract_runner`'s own module docstring
      for why `boti_data` already bounds real connection concurrency
      underneath this regardless of how high either is set.
    - `overwrite`/`max_age_minutes`/`history_days_threshold` -> accepted,
      not wired (see `RunExtractParams`'s own docstring for why each isn't a
      safe/obvious mapping onto today's `sweet_etl` — `overwrite` in
      particular intersects a proven data-loss path for history-mode jobs
      and is refused rather than guessed at). Reported back in
      `unwired_params` whenever set to a non-default value.
    """
    params = request.params
    level = logging.DEBUG if (params.debug or params.verbose) else logging.INFO
    if params.show_progress:
        level = min(level, logging.INFO)
    logging.getLogger("sweet_etl").setLevel(level)

    jobs = _build_jobs()
    fleet_result = run_bronze_fleet(
        jobs,
        period=request.period,
        reverse_order=params.reverse_order,
        ignore_missing=params.ignore_missing,
        max_workers=_effective_max_workers(params),
    )

    unwired = [name for name in _UNWIRED_PARAMS if getattr(params, name) not in (False, None)]
    if unwired:
        _log.warning("run-extract received params with no effect yet: %s", unwired)

    return RunExtractResponse(
        succeeded=fleet_result.succeeded,
        failed=fleet_result.failed,
        results=[
            JobResultOut(
                name=r.name,
                ok=r.ok,
                path=r.result.path if r.result is not None else None,
                error=r.error,
            )
            for r in fleet_result.results
        ],
        unwired_params=unwired,
    )


@app.post("/extract/redo-partition/", response_model=JobResultOut)
def redo_partition_endpoint(request: RedoPartitionRequest) -> JobResultOut:
    """Deletes and rewrites one `materialization: history` job's
    `partition_value` (`extracted_on=<partition_value>` by default) partition
    — see `sweet_etl.redo_partition()`'s own docstring for the full mechanism
    and, importantly, *when* it's actually safe to use: a same-run crash
    retry, before any later run has succeeded and advanced that job's
    watermark past the window the partition covered. This endpoint doesn't
    (and can't) check that — `WatermarkStore` only holds the current value,
    not history.

    Unlike `/extract/run-extract/`, a bad `job` name or a job that isn't
    `materialization: history` is a caller error, not a partial fleet
    result — returned as 404/400 rather than folded into `JobResultOut`
    with `ok=False`. A failure *during* the rerun itself (bad SQL, a
    filesystem error, ...) still comes back as `ok=False` in the 200
    response body, same as `/extract/run-extract/`'s per-job results.
    """
    jobs = _build_jobs()
    try:
        jobs.job(request.job)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        result = redo_partition(jobs, request.job, request.partition_value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JobResultOut(
        name=result.name,
        ok=result.ok,
        path=result.result.path if result.result is not None else None,
        error=result.error,
    )
