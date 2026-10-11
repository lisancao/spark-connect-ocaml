# Configuration and connection

## Connection strings

`Session.connect` takes a Spark Connect connection string, with the same grammar as
the official clients:

```
sc://host[:port][/;param=value;param=value...]
```

The port defaults to 15002. Parameters follow `/;` and are separated by `;`:

| Parameter | Meaning |
|---|---|
| `token` | bearer token, sent as an `authorization` header; turns TLS on |
| `use_ssl` | `true` or `false` to force TLS on or off |
| `user_id` | the Spark user id |
| `session_id` | reuse a specific session id (a UUID) |
| `user_agent` | the client type reported to the server |
| `x-*` | any parameter starting with `x-` becomes a request header |

`SPARK_REMOTE` is the conventional environment variable for the URL; the examples and
`sparkc` read it.

```ocaml
(* local server, no TLS *)
Session.connect "sc://localhost:15002"

(* token implies TLS *)
Session.connect "sc://spark.example.com:443/;token=abc123;user_id=alice"

(* explicit arguments override the string *)
Session.connect ~token:(Sys.getenv "DATABRICKS_TOKEN") ~user_id:"alice" "sc://dbc-abc.cloud.databricks.com:443"
```

`Session.connect` also takes `?use_ssl`, `?session_id` and `?headers`. TLS certificates
are validated against the system trust store through `ca-certs`.

## The session

| Function | Does |
|---|---|
| `Session.version` | the server's Spark version |
| `Session.session_id` | the client session id |
| `Session.close` | release the session on the server |
| `Session.interrupt_all`, `interrupt_tag`, `interrupt_operation` | cancel running work |
| `Session.fetch_error_details` | the server-side details for a `Spark_error` (see [Troubleshooting](troubleshooting.md)) |

## Runtime configuration

`Session.Conf` reads and writes session configuration on the server:

```ocaml
Session.Conf.set spark "spark.sql.shuffle.partitions" "8" >>= fun () ->
Session.Conf.get spark "spark.sql.shuffle.partitions" >>= fun v ->
```

| Function | Returns |
|---|---|
| `set`, `unset` | unit |
| `get` | string (raises if unset) |
| `get_opt`, `get_with_default` | string option, string |
| `get_all ?prefix` | `(key, value)` pairs, with the prefix stripped |
| `is_modifiable` | bool |

Startup-only settings (packages, `spark.sql.extensions`, catalog implementations)
belong to the server's launch command; setting them from a client has no effect.

## sparkc

`sparkc` is a SQL runner installed with the package:

```sh
sparkc -r sc://localhost:15002 -e "SELECT current_timestamp(), version()"
sparkc --json -n 5 -e "SELECT * FROM my_table"
echo "SHOW TABLES;" | sparkc --csv
sparkc            # interactive; statements end with ';'
```

| Flag | Meaning |
|---|---|
| `-r URL` | connection string (default `$SPARK_REMOTE`, then `sc://localhost:15002`) |
| `-e SQL`, `-f FILE` | statements to run (repeatable) |
| `-n N`, `--truncate N` | rows to show, cell width |
| `--table`, `--csv`, `--json` | output format |
| `-t` | print elapsed time per statement |
