# Columns and functions

## Building columns

```ocaml
Column.col "id"                       (* a column reference *)
Column.expr "id * 2"                  (* any SQL expression *)
Column.int 3                          (* literals: int, int64, float, string, bool, date, null *)
Column.lit (Value.String "x")
Column.fn "sqrt" [ Column.col "id" ]  (* any SQL function by name *)
Column.typed_null (Datatype.something ())
```

## Operators

The `Column.O` module has the usual operators so expressions read naturally:

```ocaml
Column.O.(Column.col "a" + Column.col "b")
Column.O.(Column.col "sq" > Column.int 20)
Column.O.(Column.col "a" == Column.col "b" && Column.col "c" <> Column.null)
```

Comparison, arithmetic, boolean and null operators are all there. Because a
column is a normal value, prefix operators from `Column` are available too
(`Column.add`, `Column.eq`, `Column.is_null`, `Column.like`, and so on).

## Casting and aliasing

```ocaml
Column.alias (Column.col "id") "idx"
Column.cast (Column.col "id") "STRING"
Column.try_cast (Column.col "s") "INT"
```

## Windows

```ocaml
let w = Window.(partition_by [ Column.col "borough" ] |> order_by [ Column.desc (Column.col "trips") ]) in
Column.alias (Column.over (F.row_number ()) w) "rank"
```

`Column.over` takes a `Window` spec; window functions come from `F`.

## The `F` module

`F` exposes the common Spark functions by name — aggregates (`sum`, `avg`,
`count`, `count_all`, `min`, `max`, `stddev`, `percentile_approx`), strings
(`upper`, `lower`, `concat_ws`, `substring`, `regexp_replace`, `split`), math
(`abs`, `sqrt`, `pow`, `round`, `log`), dates, collections, higher-order
functions, JSON and window functions.

Anything not covered is one call away:

```ocaml
Column.fn "my_function" [ Column.col "a"; Column.string "b" ]
```

Pass `~distinct:true` for distinct aggregates such as `F.count_distinct`.

## Lambdas

Higher-order functions take a lambda column:

```ocaml
Column.fn "transform" [ Column.col "arr"; Column.lambda [ "x" ] Column.O.(Column.col "x" * Column.col "x") ]
```
