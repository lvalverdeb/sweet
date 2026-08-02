# Upstream wishlist — boti / boti-data / boti-dask

Bugs and enhancements found while building `sweet-etl`'s `BronzeCube`/
`BronzeJobs` and running them against real infra (`sandbox/notebooks/
datacubes.ipynb`, `products_bronze.ipynb`). Each entry is grounded in an
actual reproduction from that work, not speculative — see "Evidence" for how
to reproduce.

Living document — add to it as new issues surface.

**Status (2026-07-31):** upgraded `boti-data` `1.3.2` → `1.3.3` → `1.3.4`
(`boti`/`boti-dask` unchanged — already at the latest version allowed by this
repo's `<2.0.0` constraints) and re-verified every item below against the
real install, not just symbol presence. **All 7 are now genuinely fixed and
confirmed working**, including #1 — its `1.3.3` fix introduced a worse,
silent-data-loss regression (see history below), fixed for real in `1.3.4`
and re-verified directly against real S3 data with no workaround. The
pre-delete workaround previously required in `products_bronze.ipynb`/
`datacubes.ipynb` has been removed from both — confirmed safe to remove.

## boti-data

### 1. [Bug] [RESOLVED, verified] `ParquetSink` overwrite silently destroyed data when the target path had a URI scheme prefix
Original report: `_write_with_staging()`'s single recursive `fs.mv(staging,
target, recursive=True)` 404s against prefix-only S3-compatible backends
whenever the target already exists, because `fsspec`'s `expand_path(dir,
recursive=True)` includes the bare directory prefix as a phantom "file" with
no real object behind it. That part is fixed — `1.3.3` replaced the single
recursive `mv()` with `_move_staged_files()`, moving each already-enumerated
staged file individually so `expand_path()` is never invoked. Confirmed the
underlying `expand_path()` phantom-entry behavior is still present in
`fsspec` (unpatched, as expected — not `fsspec`'s package to fix), but
`boti_data` no longer triggers it directly.

**However**, `_move_staged_files()` has a new bug that's strictly worse than
the one it replaced:

```python
def _move_staged_files(fs, staged_files, staging_path, target_root):
    for staged_file in staged_files:
        destination = staged_file.replace(staging_path, target_root, 1)
        ...
        fs.mv(staged_file, destination)
```

`staged_files` comes from `fs.glob(...)` in `ParquetSink._write()`, which
returns **protocol-less** keys (`dst-etl/foo.staging/part.0.parquet`). But
`staging_path`/`target_root` are derived from `target_path` as originally
passed to `ParquetSink` — and this repo's own `Datasources.filesystem(name).
storage_path` (used by every `sweet-etl` `BronzeCube`/`BronzeJobs` call) is
`s3://dst-etl` **with** the scheme prefix, per `boti.core.filesystem.
FilesystemConfig.storage_path`'s own documented behavior. When
`staging_path` is `s3://dst-etl/foo.staging` (prefixed) and `staged_file` is
`dst-etl/foo.staging/part.0.parquet` (not), `.replace()` finds no match and
returns the string **unchanged** — so `destination == staged_file`, and
`fs.mv(x, x)` moves a file onto itself. On `s3fs` (`mv` = copy-then-delete-
source), a same-path move deletes the only copy. Combined with
`_write_with_staging()`'s own preceding `_rm_recursive(fs, target_path)` of
the *old* target, the net effect is: **old data deleted, new data deleted,
nothing left, and `SinkWriteResult` reports success** (its `files` tuple
even still shows the un-rewritten `.staging` path — the same `.replace()`
silently no-op'd there too, which is itself a visible tell something's
wrong).

**Evidence:** reproduced twice, both times on disposable throwaway S3 keys
(never against real data after the first, accidental occurrence) — first by
accident, running the real `ProductCube` pipeline against `bronze_jobs.yaml`
(whose `destination` resolves through `Datasources.filesystem().
storage_path`, `s3://`-prefixed) with no pre-delete workaround, which
destroyed `s3://dst-etl/test-bronze/products` entirely (recovered by
rerunning the bronze job from source — no data was unrecoverable, since the
SQL source is the actual source of truth here, but this would be true data
loss for a sink whose source isn't cheaply re-derivable). Second,
deliberately isolated and confirmed via a minimal repro against a disposable
key (`_staging_repro_test2`) calling `_write_with_staging()` directly with
`target_path="s3://dst-etl/_staging_repro_test2"` — 100% reproducible,
both target and staging end up gone, `RESULT` shows the un-rewritten
`.staging` path. The same call with a protocol-less `target_path` (e.g.
`"dst-etl/_staging_repro_test"`, no `s3://`) works correctly — this is
specifically a `staging_path`/`staged_files` protocol-prefix mismatch, not a
general regression.

**Resolved in `1.3.4`:** `sinks_common.py` now has `_rebase_staged_path()`,
which normalizes both the staged file path and `staging_path`/`target_root`
through `fs._strip_protocol()` before the `.replace()`, used consistently in
both `_move_staged_files()` and `_write_with_staging()`'s own return-value
rewrite. Re-verified two ways: (1) the exact isolated repro from the `1.3.3`
report (`target_path="s3://.../_staging_repro_test3"`, pre-existing target)
now returns the correct rewritten path, target contains the new data, no
`.staging` leftover; (2) real end-to-end overwrites of both `localiser` and
`products` through our actual `BronzeJobs`/`BronzeCube` pipeline, with **no**
pre-delete workaround — both succeeded, both verified by row count against
the live S3 data afterward (1118 and 200 rows respectively, matching
expected).

**`sweet-etl` workaround removed** (`products_bronze.ipynb`,
`datacubes.ipynb`): no longer needed. Confirmed by removing it and rerunning
both notebooks clean against real S3.

---

### 2. [Bug/gap] [RESOLVED, verified] dask `_meta_nonempty` corruption via cross-column reassignment
Reassigning one dask DataFrame column via `dd.to_datetime()`/an unannotated
`map_partitions()` could silently corrupt an *unrelated* column's synthetic
meta into a raw sentinel object, only surfacing later as an opaque pyarrow
`ArrowInvalid` at write time.

**Resolved:** `sinks_common.py` now has `_validate_meta_matches_real_dtypes()`,
called from `prepare_partitioned_frame()` on every `ParquetSink`/`CsvSink`/
`JsonlSink` write. Re-reproduced the exact corruption pattern in isolation
(bool-cast a column, confirm correct meta, reassign an *unrelated* column via
`dd.to_datetime()`, watch the bool column's meta become `object`) — the dask
corruption itself still happens (expected; that's dask's own behavior, not
boti-data's to fix), but the new guard now catches it before any write
commits, with a clear, actionable error naming the exact column and cause
instead of the old cryptic pyarrow failure three layers removed from the
real trigger.

---

### 3. [Gap] [RESOLVED, verified] `uuid.UUID` object columns broke parquet writes with an opaque error
`uuid.UUID`-typed columns (pandas `object` dtype) used to fail pyarrow schema
inference with a low-level message giving no hint that stringifying fixes it.

**Resolved:** verified with a real `uuid.uuid4()` column written via the new
`write_parquet()` convenience (local filesystem) — succeeds cleanly, and the
column comes back as `string` dtype on read-back. No manual `.astype(str)`
needed anymore for this case.

---

### 4. [DX gap] [RESOLVED, verified] `worker_connection_env_var` security warning used `warnings.warn()`, not the structured logger
**Resolved:** `sql_config.py` now calls
`Logger.default_logger(logger_name=cls.__name__).warning(...)` instead of
`warnings.warn(..., UserWarning)`. Confirmed live — the warning now appears
as a structured `[WARNING][WorkerSqlConfig]` log line through this
repo's own logging setup rather than a plain, filterable Python warning.

---

### 5. [Ergonomics] [RESOLVED, verified] `ParquetSink` required explicit `close()` for a one-shot write
**Resolved:** `boti_data.write_parquet()`/`awrite_parquet()` module-level
convenience functions now exist, handling construction + write + close in
one call. Verified working for a local-filesystem destination.

---

### 6. [Safety gap] [RESOLVED, verified] no lightweight schema/row-count discovery path
**Resolved:** `DataGateway.describe()`/`adescribe()` (+ `DataHelper`
delegates) now exist, returning a `TableDescription(table, columns,
row_count, row_count_is_exact)`. Verified against a real (if small) sqlite
table — correctly reflects columns with zero rows loaded and returns an exact
row count for a table under the default cap.

## boti-dask

### 7. [Gap] [RESOLVED — via boti-data #2, no separate change] no built-in guard against the dask meta-corruption class of bug
**Resolved:** no separate `boti_dask` change was needed or made — the
`_validate_meta_matches_real_dtypes()` guard added for boti-data #2 catches
this corruption class at every sink write regardless of which dask operation
upstream caused it, confirmed by the same isolated repro used for #2. A
standalone `boti_dask.validate_meta(ddf)` would still be worth requesting
separately for a dask pipeline that never goes through a boti-data sink at
all — not needed for anything in `sweet-etl` today.

## Side effect of #2's fix: caught a real bug in our own code

The `1.3.3`/`1.3.4` upgrade also brought in a newer pandas (`3.0.5`), under
which `pd.to_datetime()` now returns `datetime64[us, UTC]` for
`fh_ultimo_movimiento`, not `datetime64[ns]` as before. `ProductCube`'s own
`meta=(col, "datetime64[ns]")` declaration had been silently wrong all along
— #2's new `_validate_meta_matches_real_dtypes()` guard caught it immediately
on the first post-upgrade run, with a clear error naming the exact column and
mismatch, instead of the coercion silently misbehaving. Fixed in both
notebooks (`meta=(col, "datetime64[us, UTC]")`, and `ProductSilverCube`'s
derived `sla_end_calc_date` the same way) — a concrete example of #2's fix
catching a real, pre-existing bug of ours, not just the synthetic repro.

## boti (core)

Nothing broken surfaced this session. One thing worth noting as a positive,
not a wishlist item: `ParquetDataResource`'s local-filesystem sandbox-root
allowlist (rejecting a path outside the project dir / system tempdir)
worked exactly as intended when tested against an arbitrary `/tmp` path.
