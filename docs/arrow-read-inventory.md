# Arrow read materialization inventory — gpze

2026-10-04, Rust working tree after `28454fd`. This records concrete boundaries,
not a claim of universal zero-copy or a throughput benchmark. Core source leases
and native planning remain the contracts in [rust-contract.md](rust-contract.md).

## Data paths and disposition

| Boundary | Before / retained behavior | Change or justification |
| --- | --- | --- |
| Log RPC/file → IPC batch | Parse/decompress wire data and resolve write-time schema | Format/compression can allocate. No second protocol decoder in connector |
| Log scanner → bounded/streaming reader | Individual batches with offsets; reader queues and clips ranges | Clipping uses Arrow slices. Native DataFusion source does not concatenate these batches |
| Initial-schema log projection/pruning | Requested projection at source; whole-batch pruning | Exact residual SQL filters remain necessary; do not duplicate them in client |
| Evolved-schema log alignment | Decode with write-time schema, align target fields; absent fields become NULL | Existing arrays are reused where compatible; new null/type-normalization data is required where representation changes |
| Log preview `LimitBatchScanner` | Singleton batch fast path; multiple batches concatenate to satisfy its single-result API | This concat is not the DataFusion source path; removing it changes preview API semantics |
| KV wire records → current logical row | FixedSchemaDecoder aligns original field IDs; previously converted whole rows at first access | Projection now selects physical source fields. Traversed unselected fields advance through checked wire boundaries without conversion |
| KV logical row → Arrow | Previously built all columns then projected | Build only unique requested fields; reorder/duplicate with native Arrow reference projection |
| KV preview limit | Previously built all rows then sliced last `limit` | Select suffix record ranges before converting/building; server framing/schema-ID discovery remains unchanged |
| KV COUNT projection | Previously decoded full value columns and stripped them in connector | KV batch readers accept zero-column projection; preserve row count explicitly and build no Arrow value columns |
| Source admission/output | Original backing storage charged; Arrow custom owners preserve source leases | Headers/owners allocate metadata, but payload/null/offset/child buffers keep pointers and geometry |
| DataFusion exact filters, casts, sort/aggregate results | May materialize different output arrays | Native SQL semantics, not gratuitous connector copies; their output accounting belongs to operators |

Relevant client paths under `clients/rust/crates/fluss/src/`:
`client/table/{batch_scanner,reader,kv_scanner}.rs`,
`client/table/scanner/{batches,builder,polling}.rs`,
`row/{fixed_schema_decoder,row_decoder}.rs`,
`row/compacted/compacted_row_reader.rs`, `record/arrow.rs` and
`client/table/read_context_resolver.rs`. Connector paths:
`crates/fluss-datafusion/src/{scan,kv_scan,resources,filter,log_table,kv_table}.rs`.

## KV implementation decisions

- Use the existing compacted deserializer and its checked scalar/byte readers.
  Float/double fields are fixed-width; integral/date/time fields use their actual
  variable encoding; timestamp/decimal widths follow format/precision. Do not
  infer byte widths from Arrow types or introduce a connector-side wire parser.
- Selected source fields are derived through the existing source→current schema
  field-ID mapping. Missing current fields remain NULL. Arrow builder projection
  then maps the aligned current row to requested fields, including reorder.
- The internal logical row still has original positional slots for compatibility;
  unselected slots need no converted values. This and schemas/index masks/builders
  are metadata allocations, not eliminated by the optimization.
- Requested duplicate columns materialize once and share the resulting ArrayRef.
  No `take`/gather of values is needed merely to duplicate or reorder columns.
- Projection does not validate logical contents of unrequested values. For example,
  an unselected string is not UTF-8 converted. Record/schema framing is still
  checked; traversed variable field lengths must fit the payload. A query selecting
  that string still receives its original conversion error. Row-only COUNT does
  not deserialize value contents, analogous to counting records rather than
  checking every field's logical validity.
- Empty projections are admitted for primary-key batch reads, not log/changelog
  readers. COUNT still receives snapshot pages/record headers; no server count RPC,
  global snapshot or filter/limit pushdown has been added.
- Empty Arrow schemas need explicit row counts. The shared row builder preserves
  its existing nonempty-schema behavior; only empty schemas need that count.
- The connector's post-decode empty projection workaround is removed. The client
  now returns the requested schema directly. EXPLAIN distinguishes KV
  `row_count_only` from logs' retained `full_rows_for_count` fallback.

## Evidence

- Client `batch_scanner` tests: selected values, duplicate/reordered ArrayRef
  sharing, invalid indices, empty COUNT output with three rows/zero Arrow bytes,
  limits, and old/new schemas with selected absent fields.
- A deliberately invalid UTF-8 value in an unselected column fails full conversion
  but allows the selected integer through the projected decoder. Truncating that
  field's wire payload still fails projected traversal. This tests avoided work,
  not merely a smaller final result (which the old post-projection already had).
- Compacted decoder tests cover skipping fixed-width float/double versus variable
  integers, alongside primitive/null/nested existing round trips.
- Core lease test now compares pointers, offsets and lengths recursively, including
  validity bitmaps and nested child buffers, across slicing/projection/retention.
- Real bounded log/KV SQL checks COUNT source batches: rows remain nonzero, columns
  are zero and `arrow_decoded_bytes` is zero. SQL output COUNT remains correct.
- Real KV snapshot/schema-evolution and four write SQL integrations cover the
  changed decoder under SELECT, DELETE and MERGE. Existing Arrow encoding/schema
  round-trip tests ensure row-builder changes do not disturb writes.

Functional evidence uses DEBUG and eight jobs. This is structural evidence of
reduced materialization, not a measured throughput/latency improvement.

## Remaining limits

Network bytes, raw page buffers, decompression/unfinished-poll peaks and format
conversion for selected values are not eliminated. Admission occurs after decode;
`w8ap` and subsequent resource profiles verify hard bounds/pressure. Compatible
Arrow views do not imply every allocation is charged to the source pool.

Log COUNT retains its current full-read fallback; a client log row-count-only
contract needs separate offset/framing/schema/pruning validation, not a shortcut
based on unchecked header counts. Log schema normalization and preview concat
are explicit retained materializations. No custom mmap, alternate decoder/runtime,
Python/FFI builds or a global allocation registry is added here.
