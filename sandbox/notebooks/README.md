# sandbox/notebooks

Jupyter notebooks for interactively using what `sweet_etl.Datasources`
builds — currently `datacubes.ipynb`, covering `Datasources.datacube()` /
`.data_helper()`.

## Setup

Needs the `etl` extra and the `notebook` dev dependency (both already in the
workspace's dependency groups):

```bash
uv sync --extra etl   # or --all-extras
```

Register the workspace's own venv as a Jupyter kernel once:

```bash
uv run python -m ipykernel install --user --name sweet --display-name "sweet"
```

Then launch and select the "sweet" kernel:

```bash
uv run jupyter notebook sandbox/notebooks/
```

## Notes

- `datacubes.ipynb`'s first section is self-contained (a temporary sqlite
  database, stdlib driver, no real credentials) so it always runs, including
  in CI or a fresh clone. The last section demonstrates the real deployment
  path — copy `sandbox/config/datasources.yaml.example` to
  `sandbox/config/datasources.yaml` (gitignored) first.
- Before committing a notebook you've run against real credentials, make
  sure no cell displays actual secret values or real business data — this
  one only ever prints non-secret fields (`query_only`, `pool_size`, ...) or
  masked `SecretStr` reprs, and catches/prints exceptions rather than raw
  query results when pointed at a real connection.
