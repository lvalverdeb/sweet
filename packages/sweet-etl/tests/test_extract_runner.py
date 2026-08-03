import datetime
import sqlite3
import threading
import time
from pathlib import Path

import pandas as pd
import pytest
from boti_data.watermark import FileWatermarkStore
from sweet_etl import BronzeJobs, Datasources, redo_partition, run_bronze_fleet
from sweet_etl.extract_runner import JobRunResult


def _write_fleet_config(tmp_path: Path) -> tuple[Path, Path]:
    db_path = tmp_path / "source.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)")
    conn.executemany(
        "INSERT INTO orders (amount) VALUES (?)", [(100.0,), (200.0,), (300.0,)]
    )
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, updated_at TEXT)")
    conn.executemany(
        "INSERT INTO events (updated_at) VALUES (?)",
        [("2025-12-20",), ("2026-01-05",), ("2026-01-15",)],
    )
    conn.execute("CREATE TABLE broken_table_ref (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()

    datasources_path = tmp_path / "datasources.yaml"
    datasources_path.write_text(
        "filesystems:\n"
        "  etl:\n"
        "    fs_type: file\n"
        f"    fs_path: {bronze_dir}\n"
        "sql:\n"
        "  connections:\n"
        "    demo:\n"
        f'      connection_url: "sqlite:///{db_path}"\n'
        "      query_only: false\n"
    )

    jobs_path = tmp_path / "bronze_jobs.yaml"
    jobs_path.write_text(
        "jobs:\n"
        "  orders:\n"
        "    sql_profile: demo\n"
        "    table: orders\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: orders\n"
        "  events:\n"
        "    sql_profile: demo\n"
        "    table: events\n"
        "    refresh: current_month\n"
        "    refresh_field: updated_at\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: events\n"
        "  broken:\n"
        "    sql_profile: demo\n"
        "    table: table_that_does_not_exist\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: broken\n"
    )
    return datasources_path, jobs_path


def test_run_bronze_fleet_runs_every_configured_job(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    fleet_result = run_bronze_fleet(
        jobs, job_names=["orders"], today=datetime.date(2026, 1, 20), return_type="pandas"
    )

    assert fleet_result.succeeded == ["orders"]
    assert fleet_result.failed == []
    written = pd.read_parquet(fleet_result.results[0].result.path)
    assert len(written) == 3


def test_run_bronze_fleet_isolates_one_jobs_failure_from_the_rest(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    fleet_result = run_bronze_fleet(
        jobs,
        job_names=["orders", "broken"],
        today=datetime.date(2026, 1, 20),
        return_type="pandas",
    )

    assert fleet_result.succeeded == ["orders"]
    assert fleet_result.failed == ["broken"]
    broken = fleet_result.results[1]
    assert broken.ok is False
    assert broken.result is None
    assert broken.error is not None


def test_run_bronze_fleet_applies_period_override_to_refresh_configured_jobs(
    tmp_path: Path,
) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    # events' own configured refresh is "current_month"; period="today"
    # overrides it for this run, so only rows dated exactly 2026-01-20
    # would qualify -- none of the fixture rows are, so the write result
    # covers 0 rows (still ok=True: a "full" job with zero matching rows is
    # a legitimate empty extract, not a failure).
    fleet_result = run_bronze_fleet(
        jobs,
        job_names=["events"],
        period="today",
        today=datetime.date(2026, 1, 20),
        return_type="pandas",
    )

    assert fleet_result.succeeded == ["events"]
    written = pd.read_parquet(fleet_result.results[0].result.path)
    assert len(written) == 0


def test_run_bronze_fleet_defaults_to_every_job_in_config_order(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    fleet_result = run_bronze_fleet(
        jobs, today=datetime.date(2026, 1, 20), return_type="pandas"
    )

    assert [r.name for r in fleet_result.results] == ["orders", "events", "broken"]


def test_run_bronze_fleet_reverse_order(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    fleet_result = run_bronze_fleet(
        jobs,
        job_names=["orders", "events"],
        reverse_order=True,
        today=datetime.date(2026, 1, 20),
        return_type="pandas",
    )

    assert [r.name for r in fleet_result.results] == ["events", "orders"]


def test_run_bronze_fleet_ignore_missing_skips_unknown_job_names(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    fleet_result = run_bronze_fleet(
        jobs,
        job_names=["orders", "does_not_exist"],
        ignore_missing=True,
        today=datetime.date(2026, 1, 20),
        return_type="pandas",
    )

    assert fleet_result.succeeded == ["orders"]
    assert fleet_result.failed == ["does_not_exist"]
    assert fleet_result.results[1].error == "unknown job"


def test_run_bronze_fleet_raises_for_unknown_job_name_without_ignore_missing(
    tmp_path: Path,
) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    with pytest.raises(KeyError, match="does_not_exist"):
        run_bronze_fleet(jobs, job_names=["does_not_exist"])


def test_run_bronze_fleet_max_workers_runs_jobs_concurrently_and_respects_the_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    active = 0
    max_active_seen = 0
    lock = threading.Lock()

    def _fake_run_one(
        jobs: BronzeJobs, name: str, *, period: object, today: object, **load_options: object
    ) -> JobRunResult:
        nonlocal active, max_active_seen
        with lock:
            active += 1
            max_active_seen = max(max_active_seen, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return JobRunResult(name=name, ok=True)

    monkeypatch.setattr("sweet_etl.extract_runner._run_one", _fake_run_one)

    names = ["orders", "events", "broken"]
    started = time.monotonic()
    fleet_result = run_bronze_fleet(jobs, job_names=names, max_workers=2)
    elapsed = time.monotonic() - started

    # 3 jobs at 0.05s each, capped at 2 concurrent workers: ~0.1s, not ~0.15s.
    assert elapsed < 0.14
    assert 2 <= max_active_seen <= 2
    # Results stay in job_names order regardless of actual completion order.
    assert [r.name for r in fleet_result.results] == names
    assert fleet_result.succeeded == names


def test_run_bronze_fleet_max_workers_none_or_one_stays_sequential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    active = 0
    max_active_seen = 0
    lock = threading.Lock()

    def _fake_run_one(
        jobs: BronzeJobs, name: str, *, period: object, today: object, **load_options: object
    ) -> JobRunResult:
        nonlocal active, max_active_seen
        with lock:
            active += 1
            max_active_seen = max(max_active_seen, active)
        time.sleep(0.01)
        with lock:
            active -= 1
        return JobRunResult(name=name, ok=True)

    monkeypatch.setattr("sweet_etl.extract_runner._run_one", _fake_run_one)

    names = ["orders", "events"]
    for kwargs in ({}, {"max_workers": 1}):
        max_active_seen = 0
        fleet_result = run_bronze_fleet(jobs, job_names=names, **kwargs)
        assert max_active_seen == 1
        assert [r.name for r in fleet_result.results] == names


def test_run_bronze_fleet_dispatches_history_materialization(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_fleet_config(tmp_path)
    jobs_path.write_text(
        "jobs:\n"
        "  events_history:\n"
        "    sql_profile: demo\n"
        "    table: events\n"
        "    watermark_field: updated_at\n"
        "    materialization: history\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: events_history\n"
    )
    watermark_store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path), watermark_store=watermark_store)

    fleet_result = run_bronze_fleet(jobs, job_names=["events_history"])

    assert fleet_result.succeeded == ["events_history"]
    written = pd.read_parquet(fleet_result.results[0].result.path)
    assert len(written) == 3
    assert "extracted_on" in written.columns


def _write_history_fleet_config(tmp_path: Path) -> tuple[Path, Path]:
    datasources_path, _ = _write_fleet_config(tmp_path)
    jobs_path = tmp_path / "bronze_jobs.yaml"
    jobs_path.write_text(
        "jobs:\n"
        "  orders:\n"
        "    sql_profile: demo\n"
        "    table: orders\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: orders\n"
        "  events_history:\n"
        "    sql_profile: demo\n"
        "    table: events\n"
        "    watermark_field: updated_at\n"
        "    materialization: history\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: events_history\n"
    )
    return datasources_path, jobs_path


def test_redo_partition_raises_for_full_materialization_job(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_history_fleet_config(tmp_path)
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path))

    with pytest.raises(ValueError, match="only applies to materialization='history'"):
        redo_partition(jobs, "orders", "2026-01-20")


def test_redo_partition_replaces_a_partial_partition_before_any_watermark_commit(
    tmp_path: Path,
) -> None:
    """The documented, safe use case: a run crashed after writing a partial
    partition but before committing the watermark (`save_to_parquet_history()`
    always commits *after* the write). `redo_partition()` deletes the partial
    directory and reruns from the still-uncommitted watermark, so it lands
    the *complete*, correct set of rows -- not a duplicate/partial mix."""
    datasources_path, jobs_path = _write_history_fleet_config(tmp_path)
    watermark_store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path), watermark_store=watermark_store)

    events_history_dir = tmp_path / "bronze" / "events_history"
    partial_partition_dir = events_history_dir / "extracted_on=2026-01-20"
    partial_partition_dir.mkdir(parents=True)
    garbage = pd.DataFrame({"id": [999], "updated_at": ["bad"], "extracted_on": ["2026-01-20"]})
    garbage.to_parquet(partial_partition_dir / "garbage.parquet")

    assert watermark_store.read(source="events_history") is None

    result = redo_partition(jobs, "events_history", "2026-01-20")

    assert result.ok is True
    written = pd.read_parquet(events_history_dir)
    assert len(written) == 3, "the full, correct row set replaced the partial/garbage write"
    assert set(written["id"]) == {1, 2, 3}
    assert watermark_store.read(source="events_history") is not None, (
        "a successful redo commits the watermark, same as any successful "
        "save_to_parquet_history() run"
    )


def test_redo_partition_proceeds_when_no_partition_exists_yet(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_history_fleet_config(tmp_path)
    watermark_store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path), watermark_store=watermark_store)

    result = redo_partition(jobs, "events_history", "2026-01-20")

    assert result.ok is True
    written = pd.read_parquet(tmp_path / "bronze" / "events_history")
    assert len(written) == 3


def test_redo_partition_rejects_a_malformed_partition_value(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_history_fleet_config(tmp_path)
    watermark_store = FileWatermarkStore(str(tmp_path / "watermarks.json"))
    jobs = BronzeJobs(jobs_path, Datasources(datasources_path), watermark_store=watermark_store)

    with pytest.raises(ValueError):
        redo_partition(jobs, "events_history", "not-a-date")
