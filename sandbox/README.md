# sandbox

A stand-in for a client deployment of `boti-sweet`, for exercising suite
behavior while `boti-sweet-etl` and future optional packages (e.g. a `bi`
package) are still being built out.

```
sandbox/
├── config/
│   ├── settings.yaml   # a pretend deployment's committed defaults
│   └── .env            # a pretend deployment's local overrides
└── run.py              # points BOTI_SWEET_CONFIG_DIR here and reports what a
                         # deployment would see: resolved settings + installed
                         # optional packages
```

Run it:

```bash
uv sync                 # boti-sweet-dummy is always present (workspace dev group)
uv run python sandbox/run.py

uv sync --extra etl      # simulate a client that also needs ETL
uv run python sandbox/run.py

uv sync                  # back to skeleton-only
uv run python sandbox/run.py
```

`boti-sweet-dummy` (`packages/boti-sweet-dummy/`) is a no-op package that only
exists to make optional-package discovery visible without needing a real,
heavyweight package installed — it registers itself the same way
`boti-sweet-etl` does, via the `boti_sweet.packages` entry-point group. Edit
`sandbox/config/settings.yaml` / `.env` to see settings precedence (YAML <
.env < environment variable) for yourself.
