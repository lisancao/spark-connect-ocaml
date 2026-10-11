# Data sources

Files are read and written by the **server**, so paths are paths on the server (or on
storage the server can reach), not on the machine running your OCaml program.
Snippets assume `open Spark_connect`, `open Lwt.Infix` and a session named `spark`.

## Reading

`Session.read` returns a reader; configure it and then load:

```ocaml
let trips = Session.Reader.parquet (Session.read spark) [ "/data/trips.parquet" ] in

let zones =
  Session.read spark |> fun r ->
  Session.Reader.option r "header" "true" |> fun r ->
  Session.Reader.option r "inferSchema" "true" |> fun r ->
  Session.Reader.csv r [ "/data/zones.csv" ]
in
```

| Function | Reads |
|---|---|
| `Reader.parquet`, `csv`, `json`, `orc`, `text` | files in that format (a list of paths) |
| `Reader.jdbc r ~url ~table` | a JDBC table |
| `Reader.table r "name"` | a table, with reader options |
| `Reader.format r "name"` then `Reader.load ?path r` | any data source the server knows |
| `Reader.schema r ddl`, `option`, `options` | schema and options |

`format` works for any source the server has loaded, which includes table formats such
as Delta (when the server is configured with Delta's extension and catalog) and Python
data sources installed on the server as `pyspark_*` packages:

```ocaml
Session.read spark |> fun r ->
Session.Reader.format r "fortunes" |> fun r ->
Session.Reader.option r "count" "4" |> fun r -> Session.Reader.load r
```

## Writing (v1)

`Dataframe.write` returns a writer:

```ocaml
Dataframe.write df |> fun w ->
Dataframe.Writer.mode w `Overwrite |> fun w ->
Dataframe.Writer.partition_by w [ "kind" ] |> fun w ->
Dataframe.Writer.parquet w "/tmp/events"
```

| Function | Effect |
|---|---|
| `Writer.mode` | `` `Append ``, `` `Overwrite ``, `` `Error_if_exists ``, `` `Ignore `` |
| `Writer.format`, `option`, `options` | data source and options |
| `Writer.partition_by`, `bucket_by n cols`, `sort_by`, `cluster_by` | layout |
| `Writer.parquet`, `csv`, `json`, `orc`, `text` | write files at a path |
| `Writer.save w path` | write with the configured format |
| `Writer.save_as_table w "name"`, `insert_into w "name"` | write to a table |

## Writing (v2)

`Dataframe.write_to df "table"` returns a DataSource v2 writer for catalogs that
support it:

```ocaml
let w = Dataframe.write_to df "db.orders" in
Dataframe.Writer_v2.using w "delta" |> fun w ->
Dataframe.Writer_v2.partitioned_by w [ Column.col "region" ] |> fun w ->
Dataframe.Writer_v2.create_or_replace w
```

`Writer_v2` has `create`, `replace`, `create_or_replace`, `append`, `overwrite cond`
and `overwrite_partitions`, plus `using`, `option`, `table_property` and
`partitioned_by`.
