# Errors

Server errors raise `Spark_connect.Spark_error`, carrying the decoded gRPC status
and the Spark error metadata:

```ocaml
match error with
| Spark_error e ->
    Printf.printf "grpc=%s class=%s sqlstate=%s\n"
      (Error.grpc_code_name e.code)
      (Option.value ~default:"-" e.error_class)
      (Option.value ~default:"-" e.sql_state)
| _ -> raise error
```

The record fields are:

| Field | Meaning |
|---|---|
| `code` | gRPC status code |
| `message` | the server message |
| `error_class` | Spark error condition, e.g. `"DIVIDE_BY_ZERO"` |
| `exception_classes` | JVM exception classes, most specific first |
| `sql_state` | SQLSTATE |
| `error_id` | id for `fetch_error_details` |
| `metadata` | raw `ErrorInfo` metadata |

Full details, including message parameters and the server stack trace, are one
call away when the error has an id:

```ocaml
Session.fetch_error_details spark e
```

Transport problems raise `Spark_connect.Grpc_client.Transport_error` instead.
