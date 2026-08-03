"""Fleet-level bronze job orchestration: run every configured job (or a
named subset) in one call, with a single `period` optionally overriding
every `refresh_field`-configured job's date window for the run.

`BronzeJobs` deliberately only holds per-job config and per-job accessors
(`data_helper_kwargs()`/`bronze_destination()`/`load_kwargs()`/
`load_incremental_kwargs()`) — nothing there iterates the fleet. This module
is that missing piece: build a `DataHelper` + a generic `BronzeCube` per job,
dispatch on `materialization` (`"full"` -> `save_to_parquet()`, `"history"`
-> `save_to_parquet_history()`, same as running each job by hand would), and
isolate one job's failure from the rest of the run.

Jobs that need real `fix_data()` business logic (int/bool/date coercion, row
exclusion, derived fields — see `bronze_jobs.py`'s own module docstring)
aren't a fit for this fleet-wide, config-only runner: it only ever builds
the generic `_ConfiguredBronzeCube` below, which has no `fix_data()`
override. Build and run that job's own `BronzeCube` subclass directly
instead (same as `sandbox/notebooks/datacubes.ipynb`'s `CustomerCube`/
`ProductCube` already do) — nothing stops mixing both approaches across a
deployment's jobs.

`redo_partition()` is a separate, one-job entry point (not part of
`run_bronze_fleet()`'s loop): the "delete a bad `history`-mode partition
and rewrite it" recovery sequence `BronzeCube.save_to_parquet_history()`'s
own docstring already describes by hand, as one call — see its own
docstring for the important caveat about *when* that's safe to do.

## Concurrency (`max_workers`) and DB connection safety

`max_workers > 1` runs jobs concurrently via a `ThreadPoolExecutor`, instead
of the default one-job-at-a-time loop. This does NOT risk exceeding a SQL
profile's connection limit, because `boti_data` already bounds real
concurrent DB connections two layers below this module, on both paths a job
can take:

- `BronzeCube.save_to_parquet()` goes through `SinkPipeline`, which forces
  `return_type="dask"` (`boti_data.pipelines.base` — materialization
  requires a lazy frame). A dask-returning SQL load defaults to
  `boti_data`'s *partitioned* loader (`should_use_partitioned_sql()` is
  `True` unless a caller explicitly passes `partitioned=False`), which never
  touches the pooled engine from `EngineRegistry` at all -- it fetches
  through thread-local `NullPool` "worker" engines, with actual concurrent
  fetches capped by a process-wide `threading.BoundedSemaphore` keyed by
  connection identity (`boti_data.db.partitioned_execution._get_fetch_gate`,
  sized by each load's `max_concurrent_fetches`, default 4). That cap
  applies across every concurrent caller against the same profile, not per
  `DataHelper` instance -- so N jobs sharing a `sql_profile` still can't
  open more than `max_concurrent_fetches` connections at once between them.
- Any load that opts out of the dask/partitioned path instead resolves a
  connection through `boti_data.db.engine_registry.EngineRegistry`, which
  caches one `Engine` (and therefore one bounded `QueuePool`) per distinct
  `SqlDatabaseConfig`, ref-counted across every caller. Multiple
  `DataHelper`s built from the same `sql_profile` (exactly what concurrent
  jobs in one fleet run do) share that single pool, sized by the profile's
  own `pool_size`/`max_overflow` in `datasources.yaml`. A checkout past
  that ceiling blocks (up to `pool_timeout`), it does not open connection
  N+1.

So `max_workers` here only controls fleet-level *job* parallelism -- how
many jobs' Python/orchestration code runs at once -- not raw DB connection
count, which was never this module's to bound. The one real consequence of
setting it too high relative to a profile's pool/gate capacity is a slower
run (jobs blocking on a checkout) or, in the worst case, a `pool_timeout`
surfacing as that job's own isolated failure -- never a fleet-wide crash or
an over-limit connection burst.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from typing import Any, cast

from boti_data.pipelines import ParquetSink, SinkWriteResult
from boti_data.pipelines.sinks import ParquetDestination

from sweet_etl.bronze import BronzeCube
from sweet_etl.bronze_jobs import BronzeJobs
from sweet_etl.refresh_presets import RefreshPreset

_log = logging.getLogger(__name__)


class _ConfiguredBronzeCube(BronzeCube):
    """Generic, no-op-`fix_data()` `BronzeCube` for fleet runs — destination
    supplied via `from_helper(helper, bronze_destination=...)`'s
    `**overrides`, same convention as every other `BronzeCube` subclass in
    this codebase (see this module's own docstring for what this can't do).
    """

    @property
    def bronze_destination(self) -> ParquetDestination:
        return self.config["bronze_destination"]


@dataclass
class JobRunResult:
    """One job's outcome from a fleet run — `ok=False` covers both a real
    failure (`error` set, `result=None`) and a clean no-op (`materialization
    == "full"` with `result=None` isn't possible; a `"history"` job with no
    new rows past its watermark returns `result=None` with `ok=True` — see
    `BronzeCube.save_to_parquet_history()`'s own docstring)."""

    name: str
    ok: bool
    result: SinkWriteResult | None = None
    error: str | None = None


@dataclass
class FleetRunResult:
    results: list[JobRunResult] = field(default_factory=list)

    @property
    def succeeded(self) -> list[str]:
        return [r.name for r in self.results if r.ok]

    @property
    def failed(self) -> list[str]:
        return [r.name for r in self.results if not r.ok]


def run_bronze_fleet(
    jobs: BronzeJobs,
    *,
    job_names: Sequence[str] | None = None,
    period: RefreshPreset | None = None,
    today: date | None = None,
    reverse_order: bool = False,
    ignore_missing: bool = False,
    max_workers: int | None = None,
    **load_options: Any,
) -> FleetRunResult:
    """Run every job in `jobs` (`jobs.job_names()`), or just `job_names` if
    given, via each job's configured `materialization` strategy.

    `period` overrides every `refresh_field`-configured job's date window
    for this run only — see `BronzeJobs.load_kwargs()`'s own `period=` — and
    has no effect on a `watermark_field`-based or unfiltered job. `today`
    likewise passes through to `load_kwargs()`, for a deterministic test of
    a `period`/`refresh`-driven run; defaults to the real current date.

    `reverse_order` reverses `job_names`' iteration order (config-file order
    otherwise). `ignore_missing` turns an unknown name in an explicit
    `job_names` into a failed `JobRunResult` instead of a raised `KeyError`
    — `jobs.job_names()`'s own default names are always valid, so this only
    matters when `job_names` is passed explicitly.

    `max_workers` (`None` or `<= 1`, the default) runs jobs one at a time, in
    order, exactly as before. A value `> 1` runs up to that many jobs
    concurrently via a `ThreadPoolExecutor` — see this module's own
    docstring ("Concurrency and DB connection safety") for why that's safe
    to do without a pool-size-derived cap here: `boti_data` already bounds
    real concurrent DB connections underneath this call, on both the
    dask/partitioned path (the default) and the pooled-engine path. The
    returned `FleetRunResult.results` are always in `names` order regardless
    of actual completion order or worker count, so downstream code (and
    every test in this module) doesn't need to special-case concurrent runs.

    A job raising anything is caught and recorded as a failed
    `JobRunResult` rather than aborting the rest of the run — one bad table
    shouldn't block every other job in the fleet. `**load_options` are
    forwarded to every "full"-materialization job's `save_to_parquet()`
    call, after that job's own `load_kwargs()` (e.g. pass `return_type=` to
    override the dask-first default for the whole run); "history" jobs
    ignore `load_options` — their kwargs come entirely from
    `load_incremental_kwargs()`, which has no room for caller overrides (see
    that method's own docstring for why).
    """
    names = list(job_names) if job_names is not None else jobs.job_names()
    if reverse_order:
        names = list(reversed(names))

    valid_names: list[str] = []
    name_to_result: dict[str, JobRunResult] = {}
    for name in names:
        try:
            jobs.job(name)
        except KeyError:
            if ignore_missing:
                _log.warning("skipping unknown bronze job %r", name)
                name_to_result[name] = JobRunResult(name=name, ok=False, error="unknown job")
                continue
            raise
        valid_names.append(name)

    if max_workers is not None and max_workers > 1 and len(valid_names) > 1:
        _run_concurrently(
            jobs,
            valid_names,
            max_workers=min(max_workers, len(valid_names)),
            period=period,
            today=today,
            name_to_result=name_to_result,
            **load_options,
        )
    else:
        for i, name in enumerate(valid_names, start=1):
            _log.info("[%d/%d] running bronze job %r", i, len(valid_names), name)
            result = _run_one(jobs, name, period=period, today=today, **load_options)
            status = "ok" if result.ok else "failed"
            _log.info("[%d/%d] bronze job %r: %s", i, len(valid_names), name, status)
            name_to_result[name] = result

    return FleetRunResult(results=[name_to_result[name] for name in names])


def _run_concurrently(
    jobs: BronzeJobs,
    names: list[str],
    *,
    max_workers: int,
    period: RefreshPreset | None,
    today: date | None,
    name_to_result: dict[str, JobRunResult],
    **load_options: Any,
) -> None:
    _log.info("running %d bronze jobs with max_workers=%d", len(names), max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_name = {
            executor.submit(_run_one, jobs, name, period=period, today=today, **load_options): name
            for name in names
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            result = future.result()
            _log.info("bronze job %r: %s", name, "ok" if result.ok else "failed")
            name_to_result[name] = result


def _build_configured_cube(jobs: BronzeJobs, name: str) -> _ConfiguredBronzeCube:
    job = jobs.job(name)
    helper = jobs.datasources.data_helper(job.sql_profile, **jobs.data_helper_kwargs(name))
    # BaseDataCube.from_helper() isn't Self-typed upstream, so mypy sees its
    # return as the base BaseDataCube regardless of which subclass it's
    # called on -- cast to what it actually is at runtime.
    return cast(
        _ConfiguredBronzeCube,
        _ConfiguredBronzeCube.from_helper(helper, bronze_destination=jobs.bronze_destination(name)),
    )


def _run_one(
    jobs: BronzeJobs,
    name: str,
    *,
    period: RefreshPreset | None,
    today: date | None,
    **load_options: Any,
) -> JobRunResult:
    job = jobs.job(name)
    cube = None
    try:
        cube = _build_configured_cube(jobs, name)
        if job.materialization == "history":
            result = cube.save_to_parquet_history(**jobs.load_incremental_kwargs(name))
        else:
            result = cube.save_to_parquet(
                partition_on=job.partition_on,
                **jobs.load_kwargs(name, today=today, period=period),
                **load_options,
            )
    except Exception as exc:  # noqa: BLE001 - fleet run: isolate one job's failure from the rest
        _log.exception("bronze job %r failed", name)
        return JobRunResult(name=name, ok=False, error=str(exc))
    finally:
        if cube is not None:
            cube.close()
    return JobRunResult(name=name, ok=True, result=result)


def redo_partition(jobs: BronzeJobs, name: str, partition_value: str) -> JobRunResult:
    """Delete an existing `materialization: history` partition and rewrite
    it by rerunning `name`'s incremental extraction, pinned to the same
    `partition_value` (an ISO date string, e.g. `"2026-08-01"`, matching
    `save_to_parquet_history()`'s own `partition_value` format).

    This is the manual recovery sequence `BronzeCube.
    save_to_parquet_history()`'s own docstring already describes for a
    failed run — delete the partial `history_partition_field=partition_value`
    directory, then rerun — as one call, instead of by hand.

    **Only reproduces the original partition's contents as a same-run crash
    retry** — i.e. run this before any later run has succeeded and advanced
    the job's watermark past the window that partition covered.
    `load_incremental()` always loads whatever is newer than the *current*
    committed watermark; if a later run already moved it forward, this
    instead captures whatever rows are new *now* and files them under
    `partition_value`'s (now stale) date — not the original historical
    rows. There's no way for this function to detect which case applies —
    `WatermarkStore` only holds the current value, not a history of past
    ones — so the caller is responsible for knowing whether the watermark
    has advanced since the partition being redone was written.

    Raises `ValueError` if `name`'s configured `materialization` isn't
    `"history"`: a `"full"` job's `save_to_parquet()` already overwrites its
    entire target on every run, so there's nothing to "redo" for it — just
    rerun it. Also raises (via `BronzeJobs.load_incremental_kwargs()`) if
    the job has no `watermark_field` configured or `jobs` has no
    `watermark_store` — both required for the rerun itself.
    """
    job = jobs.job(name)
    if job.materialization != "history":
        raise ValueError(
            f"redo_partition() only applies to materialization='history' jobs; "
            f"{name!r} is {job.materialization!r}. A 'full' job's save_to_parquet() "
            "already overwrites its entire target on every run -- just rerun it."
        )
    date.fromisoformat(partition_value)  # fail fast before deleting anything

    cube = _build_configured_cube(jobs, name)
    try:
        with ParquetSink(
            cube.bronze_destination, partition_on=[job.history_partition_field]
        ) as sink:
            fs = sink.reader.resource.require_fs()
            target = (
                f"{sink.reader.parquet_storage_path.rstrip('/')}/"
                f"{job.history_partition_field}={partition_value}"
            )
            if fs.exists(target):
                _log.warning("redo_partition: deleting existing partition %r", target)
                fs.rm(target, recursive=True)
            else:
                _log.info("redo_partition: %r does not exist yet, nothing to delete", target)
    finally:
        cube.close()

    cube = _build_configured_cube(jobs, name)
    try:
        result = cube.save_to_parquet_history(
            partition_field=job.history_partition_field,
            partition_value=partition_value,
            **jobs.load_incremental_kwargs(name),
        )
    except Exception as exc:  # noqa: BLE001 - report failure, don't raise past this point
        _log.exception("redo_partition: rerun of %r failed", name)
        return JobRunResult(name=name, ok=False, error=str(exc))
    finally:
        cube.close()
    return JobRunResult(name=name, ok=True, result=result)


__all__ = ["FleetRunResult", "JobRunResult", "redo_partition", "run_bronze_fleet"]
