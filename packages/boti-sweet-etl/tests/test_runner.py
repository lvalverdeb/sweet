from unittest.mock import MagicMock

import boti_sweet_etl.runner as runner_module
import pytest
from boti_sweet_etl import run_with_dask_session


def test_run_with_dask_session_opens_session_and_writes_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = MagicMock()
    session.__enter__.return_value = session
    session.__exit__.return_value = False
    session_factory = MagicMock(return_value=session)
    monkeypatch.setattr(runner_module, "dask_session_from_env_prefix", session_factory)

    pipeline = MagicMock()
    pipeline.__enter__.return_value = pipeline
    pipeline.__exit__.return_value = False
    pipeline.write.return_value = "result"

    result = run_with_dask_session(
        lambda: pipeline, env_prefix="DASK_", env_file=None, overwrite=True
    )

    session_factory.assert_called_once_with("DASK_", env_file=None)
    pipeline.write.assert_called_once_with(overwrite=True)
    assert result == "result"
