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

## Findings from 2026-08-01, building `sweet-etl`'s `SilverCube`/`GoldCube` and running the latter against real infra (`sandbox/notebooks/dask_laziness_ctf.ipynb`, `gold_materialization.ipynb`) — all three fixed in `boti-data` 1.3.8 (2026-08-02)

### 8. [Bug] [RESOLVED, verified] `TypeCaster.transform()` raises `TypeError` on any dask frame
`boti_data.enrichment.transformers.TypeCaster.transform()` always calls
`df.astype(applicable, errors="ignore")`. `dd.DataFrame.astype()` doesn't
accept an `errors=` kwarg at all — so any `transformers` list containing a
`TypeCaster` fails immediately on a `dd.DataFrame`, unconditionally,
regardless of what columns are being cast. `RowFilter`, `DerivedColumn`,
and `Deduplicator` (the other three `boti_data.enrichment.transformers`
classes) were checked the same way and all three work correctly on dask —
`Deduplicator`'s cross-partition case was specifically verified (a
duplicate key split across two partitions was still correctly collapsed to
one row), so this isn't a "the whole module doesn't support dask" issue,
it's narrowly `TypeCaster`.

**Evidence:** reproduced directly —

```python
>>> await TypeCaster({"amount": "float64"}).transform(a_real_dd_DataFrame)
TypeError: FrameBase.astype() got an unexpected keyword argument 'errors'
```

— against a real `dask.dataframe.DataFrame` built via `dd.from_pandas`.
`boti_data`'s own `DatacubeFrame`/`FrameResult` type unions already include
`dd.DataFrame`, so this is a real gap against the package's own stated
frame-type contract, not a case of using an unsupported type.

**Impact on `sweet-etl`:** `SilverCube` (`silver.py`) now accepts
`pd.DataFrame | dd.DataFrame` rather than pre-emptively rejecting dask (an
earlier, overly-broad guard that also blocked the three transformers that
work fine).

**Resolved:** `boti-data` 1.3.8's `TypeCaster.transform()` now branches on
`isinstance(df, dd.DataFrame)` — the dask path calls `df.astype(applicable)`
(no `errors=`, since dask has no per-column partial-cast concept: a
column's dtype must be uniform across every partition), the pandas path is
unchanged. `DataFrameTransformer`'s protocol type hints were also corrected
to `pd.DataFrame | dd.DataFrame` to match what was already true for the
other three transformers. Verified in `sweet-etl` itself: the
`return_type="pandas"` requirement is gone from `silver.py`'s docstring,
and the test that used to prove the break
(`test_typecaster_breaks_on_dask_return_type`) was replaced with
`test_typecaster_now_works_on_dask_return_type`, which passes a
`TypeCaster`-using cube straight through `return_type="dask"` and confirms
the cast actually took effect.

---

### 9. [Bug] [RESOLVED, verified] period-load filters can't compare a tz-aware `Timestamp` parquet column against `boti_data`'s own filter values
`boti_data.gateway.normalization.prepare_period_filters()` unconditionally
truncates `start`/`end` to `pd.to_datetime(x).date()` — a bare
`datetime.date` — before building the `__gte`/`__lte` filter dict, no
matter what dtype the target column actually is. When that column is a
genuine tz-aware `Timestamp` in a parquet file being read lazily (dask),
PyArrow's filter-pushdown has no comparison kernel for `date32` against a
tz-aware `timestamp` column, so the load raises
`ArrowNotImplementedError`/`ArrowInvalid`. Filtering the *identical* column
with an actual `pd.Timestamp` value (same date, correct type) succeeds.

**Evidence:** reproduced directly against a real bronze parquet snapshot of
a real table (`asm_tracking_productos`, tz-aware `last_activity_dt`):

```python
>>> await historical.aload(filters={"last_activity_dt__gte": pd.Timestamp("2023-06-01", tz="UTC")}, return_type="dask")
# succeeds
>>> await historical.aload(filters={"last_activity_dt__gte": datetime.date(2023, 6, 1)}, return_type="dask")
ArrowInvalid: Cannot compare Timestamp with datetime.date. Use ts == pd.Timestamp(date) or ts.date() == date instead.
```

Since `HybridDataset.load()`/`.aload()` (and therefore any `GoldCube`) go
through `prepare_period_filters()` internally for every period-based load,
any deployment using a tz-aware timestamp column as `date_field` hits this
unconditionally — there's no caller-facing option to make
`prepare_period_filters()` preserve the original precision/tz instead of
truncating to a bare date.

**Impact on `sweet-etl`:** previously worked around by writing the
historical (parquet) branch's date column as a plain `datetime.date`
(`date32`, which *does* have a kernel against `datetime.date` filters)
rather than the tz-aware dtype the source column actually has — that
workaround is gone now (see below).

**Resolved:** fixed at the parquet-filter-coercion layer, not in
`prepare_period_filters()` itself — `prepare_period_filters()` is
deliberately backend-agnostic (also used by SQL backends, where a bare
date is fine) and has no schema access, whereas `boti_data.parquet.
schema_filters.coerce_temporal_filters()` already had schema access and
already handled the mirror-image case (date value vs. string-typed
column, from #3's era). `boti-data` 1.3.8 adds a symmetric path there: a
bare `datetime.date` filter value is now promoted to a `pd.Timestamp`
matching the target column's actual tz whenever that column is a genuine
PyArrow timestamp type. Re-verified live against the exact real table this
gap was originally found against (`asm_tracking_productos`,
`last_activity_dt`) — both the `.dt.date` truncation workaround in
`gold_materialization.ipynb`'s real-infra variant and the corresponding
note in `gold.py`'s module docstring are gone; the notebook was re-executed
end-to-end against the real `replica` connection and matches (13,518 rows,
100% join match rate, `.compute()` and `ParquetSink.write()` agreeing) —
same shape of result as before the fix, now with the real tz-aware dtype
kept intact throughout instead of truncated.

---

### 10. [Gap] [RESOLVED, verified] `_validate_meta_matches_real_dtypes()` (wishlist #2) doesn't catch a related dask meta-corruption pattern: all-null/edge-case columns surviving a `HybridDataset` historical+live concat
Item #2 above (resolved in `1.3.4`) guards against meta/real dtype
*mismatches* by sampling one real row per partition and comparing dtypes.
It does **not** catch a related but distinct failure: when
`HybridDataset`'s dask-level concat combines two structurally different
lazy branches (here: a historical parquet reader with a source table's
full raw column set, most columns entirely null in a given slice, vs. a
live SQL branch), `dd.DataFrame._meta_nonempty`'s own synthesized
placeholder value for at least one column becomes a raw Python sentinel
object that PyArrow's schema inference can't recognize — surfacing as a
raw `pyarrow.lib.ArrowInvalid: Could not convert <object object at 0x...>
with type object: did not recognize Python value type when inferring an
Arrow data type` at `ParquetSink.write()` time, with no `boti_data`-level
diagnostic pointing at the cause the way #2's guard does for its own
pattern. `.compute()` on the exact same lazy frame succeeds and returns a
complete, correct `pd.DataFrame` throughout — the corruption is specific to
`_meta_nonempty`, not the real data, same as #2, but a case #2's guard
doesn't reach.

**Evidence:** reproduced directly against real data — `GoldCube.
save_to_parquet()` on a `HybridDataset` built from a real 89-column,
mostly-null-in-slice source table failed with the raw pyarrow error above;
the traceback shows no `_validate_meta_matches_real_dtypes()`/`ValueError`
anywhere in the stack, confirming the existing guard didn't fire before the
raw pyarrow failure. The identical pipeline projected down to 4 needed
columns (`columns=[...]`) succeeded cleanly.

**Impact on `sweet-etl`:** not treated as a bug to route around in
`sweet-etl` itself — projecting a gold/mart cube's load down to the
columns it actually needs remains correct practice for that layer
regardless of this fix, and this cube already did that, so the gap never
manifested here in the first place.

**Resolved:** root-caused via `dask`'s own source, not guessed —
`dask/dataframe/utils.py`'s `_scalar_from_dtype()` maps the `"O"` (object)
dtype kind to a bare `_object = object()` sentinel specifically
`if PANDAS_GE_300`. So on pandas>=3.0, *any* genuine object-dtype dask
column hits this whenever `frame._meta_nonempty` needs a sample and the
meta itself has 0 real rows (the normal case) — broader than the original
report's framing (a single-branch struct/nested-typed parquet column
reproduces it too, no concat required). `boti-data` 1.3.8 adds
`_validate_meta_object_columns_are_arrow_convertible()` in
`pipelines/sinks_common.py`, checked in `ParquetSink.write()` specifically
(not the shared prep function `CsvSink`/`JsonlSink` also use — arrow schema
inference is a PyArrow-only concern, and `CsvSink.write()` on the exact
same struct-column frame that PyArrow rejects was confirmed to succeed
fine through pandas' own `to_csv`). It tests each object-dtype column's
`_meta_nonempty` sample against `pa.Table.from_pandas()` and raises a
named-column `ValueError` instead of letting the raw `ArrowInvalid` escape.
Not reported as fully "fixed" in the sense of eliminating the underlying
wide-null-column problem — this cube already avoided it by projecting to 4
columns, and that remains the right practice — but the failure mode is now
a clear, actionable error rather than an opaque one whenever it does occur
elsewhere.

## boti (core)

Nothing broken surfaced this session. One thing worth noting as a positive,
not a wishlist item: `ParquetDataResource`'s local-filesystem sandbox-root
allowlist (rejecting a path outside the project dir / system tempdir)
worked exactly as intended when tested against an arbitrary `/tmp` path.
