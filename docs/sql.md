# SQL

`Session.sql` runs a statement on the server and returns a promise of a `Dataframe.t`.
The statement executes eagerly, so DDL and DML take effect when the promise resolves;
for a query, the result is a DataFrame you can keep transforming.

```ocaml
Session.sql spark "CREATE OR REPLACE TEMP VIEW t AS SELECT 7 AS seven" >>= fun _ ->
Session.sql spark "SELECT seven * 6 AS answer FROM t" >>= Dataframe.show
```

## Parameters

Pass values as parameters rather than formatting them into the string. Named
parameters use `:name`, positional ones use `?`:

```ocaml
Session.sql spark ~args:[ ("dept", Value.String "eng") ]
  "SELECT name, salary FROM people WHERE dept = :dept ORDER BY salary DESC"
>>= Dataframe.show >>= fun () ->
Session.sql spark ~pos_args:[ Value.Int 90 ] "SELECT count(*) AS above FROM people WHERE salary > ?"
>>= Dataframe.show
```

The values travel as typed literals and the server binds them.

## Views

Register a DataFrame for SQL with `Dataframe.create_or_replace_temp_view` (or
`create_temp_view`, or the `global` variants, which live in `global_temp`):

```ocaml
Dataframe.create_or_replace_temp_view people "people" >>= fun () ->
Session.sql spark "SELECT dept, avg(salary) FROM people GROUP BY dept" >>= Dataframe.show
```

Temp views belong to the session. Under Spark Connect a DataFrame built on a view reads
whatever the view points to when it *runs*, so give views unique names rather than
re-pointing one name.

## SQL inside the DataFrame API

`Column.expr "upper(name) || '!'"` and `Dataframe.select_expr df [ "id + 1 AS next" ]`
embed SQL expressions; `Dataframe.filter_expr df "salary > 90"` takes a SQL predicate.
