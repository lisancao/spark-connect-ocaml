# API coverage

This page says what the client implements and what it doesn't, so there are no
surprises. [Protocol coverage](protocol-coverage.md) is the generated, message-by-message
version: every relation, expression, command, catalog call, analyze request and config
operation in the protocol, which function builds it, and whether a test exercises it.
Everything the client implements is covered by a test.

## Tested Spark versions

The integration suite runs against live servers:

| Spark | Result |
|---|---|
| 4.1.3 | passes |
| 4.2.0 | passes |
| 4.3.0-rc1 | passes |

The protos are vendored from Spark 4.2.0.

## Supported

| Area | Covered |
|---|---|
| Session | connect (TLS, bearer tokens, custom headers), `sql` with named and positional parameters, `range`, `table`, `create_dataframe`, version, interrupts, error details, close |
| DataFrame | projection, filtering, joins of every type including self-joins, set operations, grouping with `rollup`, `cube` and pivot, sorting, limit and offset, deduplication, sampling, NA handling, `describe` and `summary`, `unpivot`, `transpose`, hints, repartitioning, `to_schema`, persistence, temp and global views |
| Actions | `collect`, `collect_columns`, `count`, `head`, `first`, `take`, `tail`, `is_empty`, `show`, `show_string`, `html_string` |
| Analysis | `schema`, `columns`, `dtypes`, `print_schema`, `explain` (every mode), `input_files`, `is_local`, `is_streaming`, `same_semantics`, `semantic_hash` |
| Columns | the full expression algebra, casts, `CASE WHEN`, nested field access and update, sort orders with null ordering, window specifications, lambdas for higher-order functions, `*`, `t.*`, regex column selection |
| Functions | the common Spark SQL functions by name in `F`, and any other through `Column.fn` |
| Data sources | readers and writers for Parquet, CSV, JSON, ORC, text, JDBC and tables, any server-side format through `format`, and DataSource v2 writers |
| Catalog | catalogs, databases, tables, columns and functions; existence checks; temp view drops; cache management; refresh and partition recovery |
| Config | `get`, `set`, `unset`, `get_all`, `is_modifiable` |
| Results | Arrow IPC decoding into rows or columns, every Spark type including decimals, intervals and nested types |
| Errors | Spark's error class, SQLSTATE, exception chain and error id on every failure |

## Not supported

- **User-defined functions.** A UDF in Spark Connect is code that runs on the server
  (pickled Python, or a JVM closure), and there is no OCaml runtime on a Spark executor.
  The SQL function library and higher-order functions with lambdas cover a lot of what
  UDFs are used for.
- **Reattachable execution** (`ReattachExecute` / `ReleaseExecute`). The client uses
  plain `ExecutePlan`, which Spark 4 fully supports, but a dropped connection during a
  long query won't resume.
- **Structured Streaming, MLlib and Declarative Pipelines commands.** The messages are
  generated and reachable through `Proto`, but there is no high-level API yet.
- **Artifact upload** (`AddArtifacts`).
- **Dictionary-encoded and compressed Arrow batches.** Spark doesn't send them by
  default; the reader raises if it meets one.
- **Statistics relations** such as `crosstab`, `cov`, `corr`, `approx_quantile` and
  `freq_items`; SQL equivalents work through `Session.sql`.

Anything the high-level API doesn't cover is reachable through `Proto`, the generated
protobuf types: `Dataframe.of_relation` accepts a raw relation.
