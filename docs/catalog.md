# Catalog

`Session.Catalog` mirrors Spark's catalog API. Listing functions return a `Dataframe.t`
you can filter and collect; the rest return plain values.

```ocaml
let module C = Session.Catalog in
C.current_database spark >>= fun db ->
Dataframe.show (C.list_tables spark) >>= fun () ->
C.table_exists spark "orders" >>= fun exists ->
```

| Function | Returns |
|---|---|
| `current_catalog`, `set_current_catalog` | string, unit |
| `current_database`, `set_current_database` | string, unit |
| `list_catalogs ?pattern`, `list_databases ?pattern` | DataFrame |
| `list_tables ?db_name ?pattern`, `list_functions ?db_name ?pattern` | DataFrame |
| `list_columns ?db_name table` | DataFrame |
| `database_exists`, `table_exists ?db_name`, `function_exists ?db_name` | bool |
| `drop_temp_view`, `drop_global_temp_view` | bool (whether it existed) |
| `cache_table`, `uncache_table`, `is_cached`, `clear_cache` | unit, bool |
| `refresh_table`, `refresh_by_path`, `recover_partitions` | unit |

Catalog calls are relations under the hood: each one executes on the server like a
small query.
