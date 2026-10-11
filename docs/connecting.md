# Connecting

`Session.connect` takes a Spark Connect connection string:

```
sc://host[:port][/;param=value;...]
```

Supported parameters:

| Parameter | Meaning |
|---|---|
| `token` | sent as a bearer `authorization` header; implies TLS unless `use_ssl=false` |
| `use_ssl` | force TLS on or off |
| `user_id` | the Spark Connect user id |
| `session_id` | reuse a specific session id |
| `user_agent` | the client type reported to the server |
| `x-*` | any such parameter becomes a request header |

The port defaults to `15002`.

```ocaml
(* a bearer token plus an extra x-* request header *)
Session.connect "sc://spark.example.com:443/;token=abc;x-request-id=42"

(* explicit arguments override the string *)
Session.connect ~token:(Sys.getenv "SPARK_CONNECT_TOKEN") ~user_id:"lisa" "sc://spark.example.com:443"
```

## How connections are made

- TLS certificate validation uses the system trust store through `ca-certs`.
- A hostname that resolves to several addresses, such as `localhost` on a
  dual-stack host, is tried in turn until one connects.
- All DataFrame transforms are pure plan construction; a connection is only made,
  and reused, when an action runs.

## Closing

`Session.close` shuts the connection down; the process also closes it on exit.
