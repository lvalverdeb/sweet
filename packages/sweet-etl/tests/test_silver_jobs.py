from pathlib import Path

import pytest
from sweet_etl import Datasources, SilverJobs


def _write_config(tmp_path: Path) -> tuple[Path, Path]:
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    datasources_path = tmp_path / "datasources.yaml"
    datasources_path.write_text(
        "filesystems:\n  etl:\n    fs_type: file\n    fs_path: " + str(data_dir) + "\n"
    )

    jobs_path = tmp_path / "silver_jobs.yaml"
    jobs_path.write_text(
        "jobs:\n"
        "  products:\n"
        "    source:\n"
        "      filesystem_profile: etl\n"
        "      path: bronze/products\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: silver/products\n"
        "  nested_products:\n"
        "    source:\n"
        "      filesystem_profile: etl\n"
        "      path: bronze/products\n"
        "    destination:\n"
        "      filesystem_profile: etl\n"
        "      path: support/silver/products\n"
    )
    return datasources_path, jobs_path


def test_job_resolves_configured_fields(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs = SilverJobs(jobs_path, Datasources(datasources_path))

    job = jobs.job("products")

    assert job.source.filesystem_profile == "etl"
    assert job.source.path == "bronze/products"
    assert job.destination.filesystem_profile == "etl"
    assert job.destination.path == "silver/products"


def test_unknown_job_raises_key_error(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs = SilverJobs(jobs_path, Datasources(datasources_path))

    with pytest.raises(KeyError, match="other"):
        jobs.job("other")


def test_malformed_job_names_the_job(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs_path.write_text("jobs:\n  broken:\n    source:\n      filesystem_profile: etl\n")

    with pytest.raises(ValueError, match="jobs.broken"):
        SilverJobs(jobs_path, Datasources(datasources_path))


def test_source_reader_kwargs_joins_filesystem_storage_path_and_job_path(
    tmp_path: Path,
) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs = SilverJobs(jobs_path, Datasources(datasources_path))

    source = jobs.source_reader_kwargs("products")

    assert source["parquet_storage_path"] == str(tmp_path / "data" / "bronze" / "products")


def test_silver_destination_supports_nested_path(tmp_path: Path) -> None:
    datasources_path, jobs_path = _write_config(tmp_path)
    jobs = SilverJobs(jobs_path, Datasources(datasources_path))

    destination = jobs.silver_destination("nested_products")

    assert destination["parquet_storage_path"] == str(
        tmp_path / "data" / "support" / "silver" / "products"
    )
