"""Generic settings loading: YAML defaults layered under prefixed env/.env overrides.

Reuses `boti.core.settings.load_dotenv_values` for dotenv parsing (which applies
`boti.core.security.validate_environment_bindings`), and follows the same
env-prefix convention as `boti.core.settings.load_prefixed_model` — this module
exists because that function validates directly against the model and has no
way to seed it from a YAML file first.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from boti.core.project import ProjectService
from boti.core.settings import load_dotenv_values
from pydantic import BaseModel, TypeAdapter, ValidationError

CONFIG_DIR_ENV_VAR = "SWEET_CONFIG_DIR"


def default_config_dir() -> Path:
    """A deployment's config directory: `SWEET_CONFIG_DIR` if set (explicit
    override, e.g. a sandbox or a container mount outside the project tree),
    else `<detected project root>/config` via
    `boti.core.project.ProjectService.detect_project_root()` — not a bare
    `"config"` relative to whatever the current working directory happens to
    be.
    """
    override = os.environ.get(CONFIG_DIR_ENV_VAR)
    if override:
        return Path(override)
    return ProjectService.detect_project_root() / "config"


def load_yaml_defaults(yaml_file: Path | str) -> dict[str, Any]:
    """Read a YAML mapping file, or return `{}` if it does not exist."""
    path = Path(yaml_file)
    if not path.is_file():
        return {}

    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a YAML mapping at the top level.")
    return data


def load_settings[TModel: BaseModel](
    model_cls: type[TModel],
    *,
    prefix: str,
    yaml_file: Path | str | None = None,
    env_file: Path | str | None = None,
) -> TModel:
    """Build `model_cls` from, in increasing priority: field defaults, `yaml_file`,
    `{prefix}{FIELD}` values from `env_file`, then the same from `os.environ`.
    """
    payload: dict[str, Any] = {}
    if yaml_file is not None:
        payload.update(load_yaml_defaults(yaml_file))

    merged_bindings: dict[str, str] = {}
    if env_file is not None and Path(env_file).is_file():
        merged_bindings.update(load_dotenv_values(Path(env_file)))
    merged_bindings.update(
        {key: value for key, value in os.environ.items() if isinstance(value, str)}
    )

    for field_name, field in model_cls.model_fields.items():
        env_key = f"{prefix}{field_name.upper()}"
        raw_value = merged_bindings.get(env_key)
        if raw_value is None or raw_value == "":
            continue

        adapter: TypeAdapter[Any] = TypeAdapter(field.annotation)
        try:
            payload[field_name] = adapter.validate_python(raw_value)
        except ValidationError:
            payload[field_name] = adapter.validate_json(raw_value)

    return model_cls.model_validate(payload)
