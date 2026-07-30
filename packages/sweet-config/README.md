# sweet-config

Generic typed-settings loading for the Boti suite: a YAML file of defaults,
overridden by `{PREFIX}FIELD` environment variables — the same env-prefix
convention as `boti.core.settings.load_prefixed_model` — sourced from a
`.env` file and then the process environment. Dotenv parsing and validation
are delegated to `boti.core.settings.load_dotenv_values`.

```python
from pydantic import BaseModel
from sweet_config import load_settings


class MySettings(BaseModel):
    debug: bool = False


settings = load_settings(
    MySettings,
    prefix="MY_APP_",
    yaml_file="config/my_app.yaml",
    env_file="config/.env",
)
```

`MY_APP_DEBUG=true` (in `.env` or the environment) overrides `debug` from
`config/my_app.yaml`, which in turn overrides the field default.
