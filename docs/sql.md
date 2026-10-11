# SQL

`Session.sql` runs a query string. Queries return a lazy DataFrame; DDL and DML
statements execute eagerly and return an empty DataFrame.

```ocaml
Session.sql spark "SELECT id, id * 2 AS twice FROM range(5)"
>>= Dataframe.show
```

## Parameters

Named parameters use `:name`, positional parameters use `?`:

```ocaml
Session.sql ~args:[ ("min", Value.Int 3) ] spark
  "SELECT * FROM range(10) WHERE id >= :min"

Session.sql ~pos_args:[ Value.Int 3; Value.Int 7 ] spark
  "SELECT * FROM range(10) WHERE id BETWEEN ? AND ?"
```

## Views

```ocaml
Dataframe.create_temp_view df "my_view"
Dataframe.create_or_replace_temp_view df "my_view"
Dataframe.create_global_temp_view df "my_view"
Dataframe.create_or_replace_global_temp_view df "my_view"
```

After creating a view, read it back with `Session.sql` or `Session.table`:

```ocaml
Session.sql spark "SELECT count(*) FROM my_view"
```

## Time zone

Timestamps render in the session time zone. Pin it when comparing values so
results do not depend on the server's locale:

```ocaml
Session.Conf.set spark "spark.sql.session.timeZone" "UTC"
```
