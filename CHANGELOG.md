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
