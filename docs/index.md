# Spark Connect for OCaml

Run Apache Spark from OCaml.

This library connects to a [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
server, lets you build DataFrames and run SQL from ordinary OCaml, and leaves the
heavy lifting on the cluster. It is pure OCaml — HTTP/2 via `h2`, protobuf via
`ocaml-protoc`, and its own Arrow IPC reader and writer — so there is no JVM and
no libarrow to install.

It speaks the same protocol as PySpark and the other Spark Connect clients, so it
works against any Spark 4.x server that exposes an endpoint, including from a
laptop or an application process.

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

## Why

- **No JVM or C dependencies.** The whole client is OCaml, including the Arrow
  codec, so it builds with opam and runs anywhere OCaml 5 runs.
- **The real transform surface.** Projections, filters, joins, grouping, pivots,
  windows, set operations, NA handling, readers and writers — not just SQL.
- **Lwt throughout.** Every call is a promise; DataFrame transforms are pure plan
  construction, and only actions talk to the server.

## Where to go next

- [Installation](installation.md) — add the package and start a server.
- [Quickstart](quickstart.md) — connect and run your first job.
- [Protocol coverage](protocol-coverage.md) — exactly which messages are implemented and
  which tests exercise them.
- [API (odoc)](https://lisancao.github.io/spark-connect-ocaml/api/) — the
  generated module reference.
