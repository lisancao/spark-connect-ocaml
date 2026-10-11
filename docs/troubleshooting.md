# Troubleshooting

## Reading errors

Failures from the server raise `Spark_error`, with the details Spark sends:

```ocaml
Lwt.catch
  (fun () -> Session.sql spark "SELECT * FROM missing" >>= Dataframe.collect >|= ignore)
  (function
    | Spark_error e ->
        Printf.printf "%s (%s)\n" (Option.value ~default:"-" e.error_class) (Option.value ~default:"-" e.sql_state);
        Session.fetch_error_details spark e >|= ignore
    | e -> Lwt.fail e)
```

`e.error_class` (for example `TABLE_OR_VIEW_NOT_FOUND`), `e.sql_state`,
`e.exception_classes` and `e.message` cover most cases;
`Session.fetch_error_details` asks the server for the full details by error id.

## Common problems

**Connection refused or hangs.** Check the server is running and listening where you
think: `sparkc -r sc://host:15002 -e "SELECT version()"`. With a token, TLS is on;
add `;use_ssl=false` only for servers that really are plaintext.

**Am I talking to the server I think I am?** Print `Session.version` first. Nothing in
`sc://localhost:15002` says which Spark answers, and a stale server on the same port
gives convincing, misleading errors.

**An error appears at `show` or `collect`, not where the mistake is.** The client
never checks names; the server analyzes the plan only when an action runs. Look at the
column names in the plan you built.

**Timestamps differ between machines.** Timestamps render in the session time zone.
Set `spark.sql.session.timeZone` explicitly when comparing output.

**`UNBOUND_SQL_PARAMETER` or `NoSuchMethodError` on a server with Iceberg or Delta.**
Table-format runtimes are built per Spark minor version, and a mismatched build fails
at query time in ways that look like client bugs. Reproduce from PySpark against the
same server; if it fails there too, fix the server's runtime version.

**A setting doesn't take effect.** Startup-only settings (packages, SQL extensions,
catalog implementations) belong on the server's launch command, not in
`Session.Conf.set`.

**Paths not found when reading or writing.** Paths resolve on the server, not on the
machine running your program.
