# boti-sweet-bi

Stub optional package for BI. No `boti-bi` runtime exists yet in the Boti
ecosystem for this package to wrap (the way `boti-sweet-etl` wraps
`boti_data`/`boti_dask`) — for now it only registers into the
`boti_sweet.packages` entry-point group, so the suite's optional-package
wiring (extras, registry discovery, `sandbox/`) is exercised end-to-end
today, and gives future BI work a package to land in.

```toml
[project.entry-points."boti_sweet.packages"]
bi = "boti_sweet_bi"
```

Install with:

```bash
uv sync --extra bi
```

When a real BI dependency exists, follow `boti-sweet-etl`'s shape: depend on
it directly, re-export/wrap its primitives here rather than reimplementing
them, and replace `describe()` with real functionality.
