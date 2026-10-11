# Columns and functions

Columns are expressions the server resolves; the client never checks a name, so a typo
fails when an action runs, not when the column is built.

## Building columns

| Function | Builds |
|---|---|
| `Column.col "name"` | a column reference (`"t.name"` qualifies it) |
| `Column.int`, `Column.float`, `Column.string`, `Column.lit (Value.t)` | literals |
| `Column.expr "sql"` | any SQL expression, parsed by the server |
| `Column.star`, `Column.star_of "t.*"` | `*` and `t.*` |
| `Column.regex pattern` | columns whose names match a backtick-quoted regex, as in `` df.colRegex `` |
| `Column.fn "name" args` | any SQL function by name |

## Operators

`Column.O` provides `+ - * / %`, `= <> < <= > >=`, `&&`, `||`, `not`, and `@=` for
`alias`. Open it locally, since it shadows OCaml's own operators:

```ocaml
let open Column.O in
Column.col "price" * Column.col "qty" > Column.int 100 && not (Column.is_null (Column.col "region"))
```

## Common column functions

| Function | Notes |
|---|---|
| `alias`, `cast`, `try_cast` | `cast c "double"` takes a type string |
| `is_null`, `isin`, `between ~low ~high`, `like` | predicates |
| `when_` / `add_when` / `otherwise` | `CASE WHEN`: `otherwise (when_ cond v) default` |
| `get_item`, `get_field`, `with_field`, `drop_fields` | arrays, maps and structs |
| `asc`, `desc`, `asc_nulls_first`, `desc_nulls_last`, ... | sort orders; plain columns sort ascending with nulls first, as in Spark |
| `over col window` | apply a window specification |

## Functions (`F`)

`F` names the common Spark SQL functions:

- aggregates: `count`, `count_all`, `count_distinct`, `sum`, `avg`, `min`, `max`,
  `first`, `last`, `collect_list`, `collect_set`, `approx_count_distinct`,
  `stddev`, `variance`, `percentile_approx`, `max_by`, `min_by`
- strings: `upper`, `lower`, `trim`, `concat`, `concat_ws`, `substring`, `split`,
  `regexp_replace`, `regexp_extract`, `lpad`, `rpad`, `format_string`, ...
- math: `abs`, `sqrt`, `pow`, `exp`, `log`, `round ?scale`, `floor`, `ceil`, `rand`, ...
- dates and times: `current_date`, `to_date ?fmt`, `to_timestamp`, `date_format`,
  `year`, `month`, `hour`, `date_add`, `datediff`, `date_trunc`, `unix_timestamp`, ...
- collections: `array`, `struct_`, `map`, `explode`, `size`, `array_contains`,
  `element_at`, `flatten`, `sequence`, ...
- higher-order: `transform`, `filter`, `exists`, `aggregate`, with OCaml lambdas
- JSON: `to_json`, `from_json`, `get_json_object`
- window: `row_number`, `rank`, `dense_rank`, `percent_rank`, `ntile`, `lag`, `lead`, ...

Anything not listed is one call away: `Column.fn "levenshtein" [ a; b ]`.

## Window functions

```ocaml
let w =
  Column.Window.partition_by [ Column.col "region" ] |> fun w ->
  Column.Window.with_order_by w [ Column.col "month" ] |> fun w ->
  Column.Window.rows_between w Column.Window.unbounded_preceding Column.Window.current_row
in
Dataframe.with_column sales "running_total" (Column.over (F.sum (Column.col "amount")) w)
```

`rows_between` and `range_between` take `unbounded_preceding`, `current_row`,
`unbounded_following`, or `Column.Window.Value c`.

## Higher-order functions

```ocaml
F.transform (Column.col "prices") (fun p -> Column.O.(p * Column.float 1.1))
```

The lambda is built into the plan as a Spark lambda expression and runs on the server;
no OCaml code is shipped.
