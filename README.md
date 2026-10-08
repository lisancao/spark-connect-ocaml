# spark-connect for OCaml

A client for [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html),
the gRPC protocol that lets a thin client build DataFrame plans and run them on a
remote Apache Spark server. Written in OCaml end to end: HTTP/2 via `h2`,
protobuf via `ocaml-protoc`, and an Arrow IPC reader and writer included in the
package, so there is no C dependency on libarrow or on a JVM.

Works against Spark 4.x servers, including Databricks compute that exposes a
Spark Connect endpoint. Tested against Spark 4.2.0.

```ocaml
open Lwt.Infix
open Spark_connect

let () =
  Lwt_main.run begin
    Session.connect "sc://localhost:15002" >>= fun spark ->
    Session.sql spark "SELECT id, id * id AS sq FROM range(10)" >>= fun df ->
    let big = Dataframe.filter df Column.O.(Column.col "sq" > Column.int 20) in
    Dataframe.show big >>= fun () ->
    Dataframe.collect big >>= fun rows ->
    List.iter (fun r -> print_endline (Row.to_string r)) rows;
    Session.close spark
  end
```

## Install

Requires OCaml 5.0 or newer and opam.

```sh
opam pin add spark-connect https://github.com/lisanatashacao/spark-connect-ocaml.git
```

Or from a checkout:

```sh
opam install . --deps-only --with-test
dune build
dune install
```

This also installs `sparkc`, a small SQL runner:

```sh
sparkc -r sc://localhost:15002 -e "SELECT current_timestamp(), version()"
sparkc --json -n 5 -e "SELECT * FROM my_table"
echo "SHOW TABLES;" | sparkc --csv
sparkc            # interactive; statements end with ';'
```

`SPARK_REMOTE` sets the default URL, as in PySpark.

## Connecting

The connection string follows the Spark Connect spec:
`sc://host[:port]/;param=value;...`. Supported parameters: `token` (sent as a
bearer `authorization` header and switching TLS on), `use_ssl`, `user_id`,
`session_id`, `user_agent`, and any `x-*` parameter, which becomes a request
header. Port defaults to 15002.

```ocaml
(* Databricks *)
Session.connect "sc://dbc-abc.cloud.databricks.com:443/;token=dapi...;x-databricks-cluster-id=0123-456789-abcdef"

(* explicit arguments override the string *)
Session.connect ~token:(Sys.getenv "DATABRICKS_TOKEN") ~user_id:"lisa" "sc://dbc-abc.cloud.databricks.com:443"
```

TLS certificate validation uses the system trust store through `ca-certs`.

## What's in the box

| Module | Purpose |
|---|---|
| `Session` | connect, `sql` (with `:named` and `?` parameters), `range`, `table`, `read` (parquet, csv, json, orc, text, jdbc, any format), `create_dataframe`, `Conf`, `Catalog`, interrupts, `version`, `fetch_error_details`, `close` |
| `Dataframe` | `select`, `filter`, `with_column(s)`, `drop`, rename, `join` (all join types, `join_using`, self-joins via `Dataframe.col`), set operations, `group_by` / `rollup` / `cube` / pivot, `sort`, `limit`, `offset`, `sample`, `distinct`, NA handling, `describe`, `summary`, `unpivot`, `transpose`, hints, repartitioning, `persist` |
| actions | `collect` (rows), `collect_columns` (columnar), `count`, `head`, `take`, `tail`, `show`, `show_string`, `html_string`, `schema`, `columns`, `dtypes`, `print_schema`, `explain`, `input_files`, `is_local`, `same_semantics` |
| writers | `Dataframe.write` (format, mode, options, partition/bucket/cluster by, `save`, `save_as_table`, `insert_into`) and `write_to` (DataSource v2 modes), temp and global views |
| `Column` | `col`, literals, `expr`, `fn` for any SQL function, arithmetic and comparison operators (module `O`), `alias`, `cast`, sort orders with null ordering, `when_`/`otherwise`, `isin`, `like`, `get_item`, `get_field`, `with_field`, `over` with `Window` specs, `lambda` |
| `F` | the common Spark functions by name: aggregates, strings, math, dates, collections, higher-order functions, JSON, window functions |
| `Value` / `Row` | decoded results: ints, floats, strings, binary, decimals as strings, dates, times, timestamps with unit and zone, intervals, lists, structs, maps |
| `Arrow` / `Arrow_writer` | the Arrow IPC stream codec, usable on its own |
| `Error` | `Spark_error` with gRPC code, message, Spark error class, SQLSTATE, JVM exception classes, and an error id for `fetch_error_details` |
| `Proto` | the generated protobuf types, for anything the high-level API does not cover (`Dataframe.of_relation` accepts a raw relation) |

Everything is Lwt-based. All DataFrame transforms are pure plan construction;
only actions talk to the server.

## Not covered yet

- Reattachable execution (`ReattachExecute` / `ReleaseExecute`). Plain
  `ExecutePlan` is used, which Spark 4 servers fully support; very long queries
  over flaky networks will not resume after a dropped connection.
- Dictionary-encoded and compressed Arrow batches (Spark does not send these by
  default; the reader raises `Arrow.Unsupported` if it meets one).
- User-defined functions, Structured Streaming, MLlib and pipeline commands.
  The protos are generated, so the raw messages are available through `Proto`.
- Artifact upload (`AddArtifacts`).

## Design notes

- `src/grpc_client.ml` is a minimal gRPC client on top of `h2`: length-prefixed
  framing, trailers for status, `grpc-status-details-bin` decoding for Spark's
  rich errors. Unary and server-streaming calls only, which is all the
  protocol needs.
- The protos are vendored from the Spark repository (`proto/README.md`) and
  compiled at build time. Two small ocaml-protoc code generation quirks are
  patched by `src/proto/fixup` after generation.
- `src/arrow` is a self-contained Arrow IPC implementation: a FlatBuffers
  reader and builder, the streaming framing, and column decoders for the types
  Spark produces. `Arrow_writer` output validates with `pyarrow`'s full
  validation and is what `Session.create_dataframe` sends as a `LocalRelation`.
- Plan ids are assigned to every relation so that `Dataframe.col df "x"`
  resolves against a specific side of a self-join, as in the official clients.

## Tests

```sh
dune test                      # Arrow codec and unit tests, no server needed
```

Integration tests need a server. Spark's official image works out of the box:

```sh
docker run -d --name spark-connect -p 15002:15002 apache/spark:4.2.0-scala2.13-java21-python3-ubuntu \
  /opt/spark/bin/spark-submit --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --master 'local[2]' --conf spark.connect.grpc.binding.port=15002
SPARK_REMOTE=sc://localhost:15002 dune test
```

The Arrow fixtures in `test/fixtures` were produced with `pyarrow` and cover
every Arrow type the reader handles, plus multi-batch, legacy framing, empty,
dictionary and compressed streams.

## License

Apache License 2.0. The vendored protobuf files are copyright the Apache
Software Foundation, under the same license.
