# Troubleshooting

## Connection refused on `localhost`

On a dual-stack host, `localhost` can resolve to `::1` first while the server is
published on IPv4 only. The client tries every resolved address in turn, so a
working IPv4 address is reached even if the first `connect` fails. If you still
cannot connect, use the literal address (`sc://127.0.0.1:15002`) and check that
the server is listening:

```sh
docker ps
docker logs spark-connect
```

## Timestamps differ between runs

Timestamps render in the session time zone. Pin it before comparing values:

```ocaml
Session.Conf.set spark "spark.sql.session.timeZone" "UTC"
```

## `Arrow.Unsupported`

The reader raises this for dictionary-encoded or compressed batches. Spark does
not send these by default; if a server is configured to, disable the encoding or
decode the batch on the server side.

## A long query does not resume after a dropped connection

The client uses plain `ExecutePlan`, which Spark 4 servers fully support, and does
not implement the reattachable `ReattachExecute` / `ReleaseExecute` flow. Very long
queries over flaky networks will not resume; retry the action instead.
