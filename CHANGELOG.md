# Changelog

## 0.1.0 (unreleased)

Initial release.

- gRPC transport over HTTP/2 (h2), plaintext and TLS, bearer tokens and custom headers.
- Protobuf bindings generated from the Spark 4.2.0 Spark Connect protos.
- Pure OCaml Arrow IPC reader (results) and writer (`create_dataframe`).
- `Session`: SQL with named and positional parameters, `range`, `table`, readers, configuration, catalog, interrupts, error details.
- `Dataframe`: projections, filters, joins, set operations, grouping and pivots, sorting, sampling, writers (v1 and v2), temp views, schema and plan analysis, persistence.
- `Column` and `F`: expression builders, operators, window specs, lambdas for higher-order functions.
- `sparkc`: command line SQL runner with table, CSV and JSON output.

Fixes found by the expanded integration suite:

- The gRPC client connected to only the first address a hostname resolved to, so
  `localhost` failed on dual-stack hosts where `::1` is returned first; every
  address is now tried in turn.
- `Dataframe.na_fill` sent whole numbers as 32-bit integer literals, which the server's fill rejects; they are now sent as longs, as PySpark does.
- Sort orders without an explicit null ordering were sent as unspecified, so the server put nulls last in ascending sorts; they now follow Spark's defaults (nulls first ascending, last descending).
- `Dataframe.show` now always ends with a newline, so consecutive tables don't run together.

Testing and docs:

- Integration tests for every implemented message type; the suite passes against Spark 4.1.3, 4.2.0 and 4.3.0-rc1.
- `scripts/coverage_matrix.py` generates `docs/protocol-coverage.md` (protocol coverage plus per-version results).
- A documentation site (MkDocs Material) with guides for every part of the API, and the odoc API reference.
- Runnable examples: quickstart, SQL and views, window functions, reading and writing, word count.
- `examples/nyc_taxi`: a batch job over a month of NYC taxi trips, with a Docker compose cluster to run it on.
- `examples/options_desk`: market data through a Python data source, analytics in Spark, Black-Scholes pricing in OCaml (vendored from option-pricing-model-comparison, MIT), a metric view risk layer, and Delta `MERGE` and time travel.

CI and releases:

- CI: build and unit tests on OCaml 5.0 through 5.3, formatting and the API docs on every push and pull request, and the integration suite against Spark 4.1.3 and 4.2.0.
- Docs are built and published to GitHub Pages from `main`.
- Pushing a `v*` tag builds, tests and publishes a GitHub Release with the source archive.
