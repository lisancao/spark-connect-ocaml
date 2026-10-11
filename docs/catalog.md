# Catalog and configuration

## Configuration

`Session.Conf` reads and writes Spark runtime configuration:

```ocaml
Session.Conf.set spark "spark.sql.session.timeZone" "UTC"
Session.Conf.get spark "spark.sql.session.timeZone"
Session.Conf.get_with_default spark "spark.sql.shuffle.partitions" "200"
Session.Conf.get_all spark
Session.Conf.unset spark "some.key"
Session.Conf.is_modifiable spark "some.key"
```

## Catalog

`Session.Catalog` covers the catalog operations in the protocol:

```ocaml
Session.Catalog.current_catalog spark
Session.Catalog.set_current_catalog spark "main"
Session.Catalog.list_catalogs spark
Session.Catalog.current_database spark
Session.Catalog.list_databases spark
Session.Catalog.list_tables ~db_name:"default" spark
Session.Catalog.list_columns spark "my_table"
Session.Catalog.table_exists spark "my_table"
```

Also available: `list_functions`, `function_exists`, `database_exists`,
`drop_temp_view`, `drop_global_temp_view`, `is_cached`, `cache_table`,
`uncache_table`, `clear_cache`, `refresh_table`, `refresh_by_path` and
`recover_partitions`.

## Interrupts

```ocaml
Session.interrupt_all spark
Session.interrupt_tag spark tag
Session.interrupt_operation spark operation_id
```
