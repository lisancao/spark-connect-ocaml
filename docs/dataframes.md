# DataFrames

A `Dataframe.t` is a lazy plan. Transform functions return a new DataFrame and
never touch the server; only *actions* run the plan.

```ocaml
open Lwt.Infix
open Spark_connect

let () =
  Lwt_main.run begin
    Session.connect "sc://localhost:15002" >>= fun spark ->
    Session.range spark 100L >>= fun df ->
    Dataframe.filter df Column.O.(Column.col "id" > Column.int 50)
    >>= Dataframe.collect
    >|= List.iter (fun r -> print_endline (Row.to_string r))
  end
```

## Selecting and reshaping

| Function | Purpose |
|---|---|
| `select`, `select_names`, `select_expr` | project columns or expressions |
| `with_column`, `with_columns` | add or replace columns |
| `with_column_renamed`, `with_columns_renamed` | rename columns |
| `drop`, `drop_cols`, `to_df` | drop columns, or rename all at once |
| `alias` (`as_`) | name a subquery |
| `distinct`, `drop_duplicates` | deduplicate |
| `unpivot`, `transpose` | reshape |
| `na_drop`, `na_fill` | handle nulls |

```ocaml
let enriched =
  df
  |> Dataframe.with_column "sq" Column.O.(Column.col "id" * Column.col "id")
  |> Dataframe.with_columns [ ("doubled", Column.O.(Column.col "id" * Column.int 2)) ]
```

## Filtering

`filter` takes a `Column.t`; `filter_expr` takes a SQL expression string.

```ocaml
Dataframe.filter df Column.O.(Column.col "id" > Column.int 50)
Dataframe.filter_expr df "id > 50 AND id % 2 = 0"
```

## Sorting, limiting, sampling

```ocaml
Dataframe.sort df [ Column.desc (Column.col "id") ]        (* global sort *)
Dataframe.limit df 10
Dataframe.offset df 10
Dataframe.sample df ~with_replacement:false 0.1
```

Null ordering is explicit: `Column.asc_nulls_first`, `asc_nulls_last`,
`desc_nulls_first`, `desc_nulls_last`; bare `asc`/`desc` follow Spark's defaults.

## Joins

```ocaml
let joined =
  Dataframe.join ~how:`Inner orders customers
    ~on:Column.O.(Dataframe.col orders "customer_id" == Dataframe.col customers "id")
```

`join` takes all Spark join types (`how`), `join_using` joins on column names,
`cross_join` is the cartesian product. `Dataframe.col df "x"` binds a column to a
specific side of a self-join, the same way the official clients do.

## Grouping and aggregation

```ocaml
let g = Dataframe.group_by df [ Column.col "k" ] in
Dataframe.Grouped.sum g [ "v" ]            (* or .count, .avg, .max, .min *)
Dataframe.Grouped.agg g [ Column.alias (F.count_all ()) "n" ]
```

`rollup`, `cube` and `Grouped.pivot` are available too. `Dataframe.agg df aggs`
is the no-grouping form.

## Set operations

`union` / `union_all`, `union_by_name`, `intersect`, `intersect_all`, `except`,
`except_all`.

## Partitioning and hints

`repartition`, `coalesce`, `repartition_by`, `hint`, and `persist` / `cache` /
`unpersist`.

## Actions

| Function | Returns |
|---|---|
| `collect` | `Row.t list` |
| `collect_columns` | columnar result |
| `count` | `int64` |
| `head`, `take`, `tail`, `first` | rows |
| `show`, `show_string`, `html_string` | printed table / string |
| `schema`, `columns`, `dtypes`, `print_schema` | metadata |
| `explain` | a plan or tree-string |
| `is_local`, `is_streaming`, `input_files`, `same_semantics` | analysis |

`same_semantics` checks whether another DataFrame resolves to the same plan.
