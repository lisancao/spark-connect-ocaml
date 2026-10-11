# DataFrames

A `Dataframe.t` is a plan, not data. Every transformation wraps the current plan in a
new relation and returns immediately; nothing reaches the server until an action runs.
Snippets assume `open Spark_connect`, `open Lwt.Infix` and a session named `spark`.

## Creating DataFrames

| Function | Produces |
|---|---|
| `Session.range ?start ?step ?num_partitions spark n` | a single `id` column, `start` to `n - 1` |
| `Session.table spark "db.name"` | a table or view |
| `Session.sql spark query` | the result of a SQL query (see [SQL](sql.md)) |
| `Session.create_dataframe spark ~columns rows` | rows of `Value.t`, sent as Arrow |
| `Session.create_dataframe_ddl spark ~ddl ~columns rows` | the same, with a DDL schema |
| `Session.Reader.*` | files and tables (see [Data sources](data-sources.md)) |

## Transformations

```ocaml
let open Column.O in
let df = Session.range spark 100L in
let df = Dataframe.with_column df "bucket" (Column.col "id" % Column.int 10) in
let df = Dataframe.filter df (Column.col "id" > Column.int 5) in
let df = Dataframe.select df [ Column.col "id"; Column.col "bucket" ] in
```

| Area | Functions |
|---|---|
| Projection | `select`, `select_names`, `select_expr` (SQL expressions), `with_column`, `with_columns`, `with_column_renamed`, `with_columns_renamed`, `drop`, `drop_cols`, `to_df`, `alias` |
| Filtering | `filter` / `where`, `filter_expr` (a SQL predicate string) |
| Ordering and limits | `sort` / `order_by`, `sort_within_partitions`, `limit`, `offset` |
| Deduplication | `distinct`, `drop_duplicates` (all columns or a subset) |
| Set operations | `union`, `union_all`, `union_by_name ?allow_missing_columns`, `intersect`, `intersect_all`, `except`, `except_all` |
| Reshaping | `unpivot ~ids ?values ~variable_column_name ~value_column_name`, `transpose ?index_column` |
| Missing values | `na_drop ?cols ?min_non_nulls`, `na_fill ?cols values` |
| Statistics | `describe cols`, `summary stats` (e.g. `[ "min"; "max"; "50%" ]`) |
| Sampling | `sample ?with_replacement ?seed df fraction` |
| Partitioning | `repartition`, `coalesce`, `repartition_by ?num_partitions cols`, `hint df "broadcast" []` |
| Schema | `to_schema df struct_type` reconciles columns to a target schema |

## Joins

```ocaml
let joined =
  Dataframe.join ~how:`Left_outer orders customers
    ~on:Column.O.(Column.col "customer_id" = Column.col "id")
in
let same_name = Dataframe.join_using orders customers [ "id" ] in
let every_pair = Dataframe.cross_join a b in
```

`~how` is one of `` `Inner `` (the default), `` `Left_outer ``, `` `Right_outer ``,
`` `Full_outer ``, `` `Left_semi ``, `` `Left_anti `` and `` `Cross ``.

For a self-join, alias both sides and bind columns to a side with `Dataframe.col`,
which tags the column with that DataFrame's plan id so the server resolves it against
the right input even when names collide:

```ocaml
let a = Dataframe.alias people "a" and b = Dataframe.alias people "b" in
Dataframe.join a b ~on:Column.O.(Dataframe.col a "id" = Dataframe.col b "id" + Column.int 1)
```

## Grouping and aggregation

```ocaml
let open Column.O in
Dataframe.group_by sales [ Column.col "region" ] |> fun g ->
Dataframe.Grouped.agg g
  [ F.count_all () @= "orders"; F.sum (Column.col "amount") @= "revenue"; F.avg (Column.col "amount") @= "avg" ]
```

- `Dataframe.group_by`, `group_by_names`, `rollup` and `cube` return a `Grouped.t`.
- `Grouped.agg` takes aggregate columns; `Grouped.count`, `sum`, `avg`, `mean`, `max`
  and `min` are shortcuts.
- `Grouped.pivot` pivots on a column, optionally with an explicit value list.
- `Dataframe.agg df aggs` aggregates the whole DataFrame without grouping.

## Actions

| Action | Returns |
|---|---|
| `collect` | `Row.t list` |
| `collect_columns` | column names and one `Value.t array` per column (columnar, cheaper for large results) |
| `count` | `int64` |
| `head`, `first` | `Row.t option` |
| `take df n`, `tail df n` | `Row.t list` |
| `is_empty` | `bool` |
| `show ?n ?truncate ?vertical`, `show_string`, `html_string` | the server-formatted table (printed, or as a string) |

## Inspecting a plan

| Function | Returns |
|---|---|
| `schema`, `columns`, `dtypes` | the server-resolved schema |
| `print_schema`, `tree_string` | the schema as a tree |
| `explain ?mode`, `explain_string ?mode` | the plan; `` `Simple ``, `` `Extended ``, `` `Codegen ``, `` `Cost `` or `` `Formatted `` |
| `input_files` | files a file-based relation reads |
| `is_local`, `is_streaming`, `same_semantics`, `semantic_hash` | plan properties |

`explain` shows the plan the *server* built; the client has no optimizer.

## Persistence and views

- `persist ?level` (default `` `Memory_and_disk ``), `cache`, `unpersist`
- `create_or_replace_temp_view`, `create_temp_view`, and the `global` variants

Writers (`Dataframe.write`, `Dataframe.write_to`) are on the [Data sources](data-sources.md) page.
