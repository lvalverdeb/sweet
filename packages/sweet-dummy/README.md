# sweet-dummy

Minimal reference optional package. It has no real behavior and no
dependencies beyond the standard library — it exists to demonstrate, and let
you exercise, the contract a real optional package (`sweet-etl`, a
future `sweet-bi`, ...) implements to plug into the suite:

```toml
[project.entry-points."sweet.packages"]
dummy = "sweet_dummy"
```

That's the whole contract: register a module (or an attribute in it) under
the `sweet.packages` entry-point group, and
`sweet.registry.installed_packages()` will discover it.

This package is a workspace dev-only dependency (see the root
`pyproject.toml`'s `dev` dependency group) — it is never something a client
deployment installs. See `sandbox/` for a walkthrough that uses it.
