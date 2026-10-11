# Reading and writing

## Reading

`Session.read` starts a reader; the format functions add paths:

```ocaml
let df = Session.Reader.parquet (Session.read spark) [ "s3a://bucket/data" ]
let csv = Session.Reader.csv (Session.read spark) [ "data/*.csv" ]
```

Available formats: `parquet`, `csv`, `json`, `orc`, `text`, `jdbc`, plus
`load`/`load_paths` with an explicit `format` for anything else.

```ocaml
let jdbc = Session.Reader.jdbc (Session.read spark) ~url:"jdbc:..." ~table:"public.t"
```

Reader options and schema:

```ocaml
let r =
  Session.read spark
  |> Session.Reader.format "csv"
  |> Session.Reader.option "header" "true"
  |> Session.Reader.option "inferSchema" "true"
in
Session.Reader.load_paths r [ "data.csv" ]
```

`Session.table ?options spark name` reads a catalog table directly.

## Writing

```ocaml
let w = Dataframe.write df |> Dataframe.Writer.format "parquet" |> Dataframe.Writer.mode "overwrite" in
Dataframe.Writer.save w "s3a://bucket/out"

Dataframe.write df
|> Dataframe.Writer.format "delta"
|> Dataframe.Writer.save_as_table "cat.db.table"
```

Writer options: `format`, `mode` (`"append"`, `"overwrite"`, `"error"`,
`"ignore"`), `option`/`options`, `partition_by`, `bucket_by`, `cluster_by`, and
the terminal `save`, `save_as_table`, `insert_into`.

Data Source v2 writers use `Dataframe.write_to table`:

```ocaml
Dataframe.write_to df "cat.db.table"
|> Dataframe.Writer_v2.option "mergeSchema" "true"
|> Dataframe.Writer_v2.create_or_replace
```

## Local DataFrames

Build a DataFrame from OCaml values without a round trip:

```ocaml
let df = Session.create_dataframe spark ~columns:[ "id"; "name" ]
  [ [ Value.Int 1; Value.String "a" ]; [ Value.Int 2; Value.String "b" ] ]

let typed = Session.create_dataframe_ddl spark ~ddl:"id INT, name STRING"
  ~columns:[ "id"; "name" ]
  [ [ Value.Int 1; Value.String "a" ] ]
```

`create_dataframe` infers types from the values; the DDL form fixes the Spark
schema. Both are encoded as an Arrow `LocalRelation` by `Arrow_writer`.
