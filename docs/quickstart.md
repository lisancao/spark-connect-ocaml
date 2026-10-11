# Quickstart

This page assumes the library is installed and a Spark Connect server is running
(see [Installation](installation.md)). Every snippet below runs inside a session:

```ocaml
open Lwt.Infix
open Spark_connect

let () =
  Lwt_main.run
    ( Session.connect "sc://localhost:15002" >>= fun spark ->
      (* snippets go here *)
      Session.close spark )
```

The API is [Lwt](https://github.com/ocsigen/lwt)-based. Building a DataFrame is pure
plan construction and returns immediately; only actions (`collect`, `count`, `show`,
writes, ...) talk to the server and return a promise.

## Connecting

`Session.connect` takes a Spark Connect connection string:

```ocaml
Session.connect "sc://localhost:15002" >>= fun spark ->
Session.version spark >>= fun v ->
Printf.printf "connected to Spark %s\n" v;
...
```

Connection strings, tokens, TLS and headers are covered in
[Configuration and connection](configuration.md).

## Your first DataFrame

```ocaml
let open Column.O in
let df =
  Session.range spark 10L |> fun d ->
  Dataframe.select d [ Column.col "id"; Column.col "id" * Column.int 2 @= "doubled" ] |> fun d ->
  Dataframe.filter d (Column.col "id" % Column.int 2 = Column.int 0)
in
Dataframe.show df >>= fun () ->
```

`Column.O` brings the operators into scope (`+ - * / %`, comparisons, `&&`, `||`, and
`@=` for aliasing); open it locally, because it shadows OCaml's own operators.

## From OCaml values

```ocaml
let people =
  Session.create_dataframe spark ~columns:[ "name"; "age" ]
    [ [ Value.String "ann"; Value.Int 34 ]; [ Value.String "bob"; Value.Int 27 ] ]
in
```

The rows go to the server as an Arrow batch inside the plan, and the server infers
the Spark types (`Value.Int` becomes `bigint`, `Value.Float` becomes `double`, and
so on). For explicit types, pass a DDL schema:
`Session.create_dataframe_ddl spark ~ddl:"name string, age int" ~columns:[ "name"; "age" ] rows`.

## Getting results back

```ocaml
Dataframe.collect people >>= fun rows ->
List.iter (fun r -> print_endline (Row.to_string r)) rows;
Dataframe.count people >>= fun n ->
Printf.printf "%Ld rows\n" n;
```

`Row.field r "name"` returns a `Value.t`; [Results and types](types.md) has the
accessors.

## SQL

```ocaml
Dataframe.create_or_replace_temp_view people "people" >>= fun () ->
Session.sql spark ~args:[ ("min", Value.Int 30) ] "SELECT name FROM people WHERE age >= :min"
>>= Dataframe.show
```

## Next

- [DataFrames](dataframes.md) and [Columns and functions](columns-and-functions.md)
- [Examples](examples.md): runnable programs, from a quickstart to a batch job on a cluster
