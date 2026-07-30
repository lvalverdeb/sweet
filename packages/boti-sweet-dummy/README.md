# boti-sweet-dummy

Minimal reference optional package. It has no real behavior and no
dependencies beyond the standard library — it exists to demonstrate, and let
you exercise, the contract a real optional package (`boti-sweet-etl`, a
future `boti-sweet-bi`, ...) implements to plug into the suite:

```toml
[project.entry-points."boti_sweet.packages"]
dummy = "boti_sweet_dummy"
```

That's the whole contract: register a module (or an attribute in it) under
the `boti_sweet.packages` entry-point group, and
`boti_sweet.registry.installed_packages()` will discover it.

This package is a workspace dev-only dependency (see the root
`pyproject.toml`'s `dev` dependency group) — it is never something a client
deployment installs. See `sandbox/` for a walkthrough that uses it.
