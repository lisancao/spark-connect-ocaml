# Quickstart

## 1. Start a Spark Connect server

The official Spark image works out of the box:

```sh
docker run -d --name spark-connect -p 127.0.0.1:15002:15002 \
  --entrypoint /opt/spark/sbin/start-connect-server.sh \
  apache/spark:4.2.0-scala2.13-java21-python3-ubuntu --wait
```

The server listens on `sc://localhost:15002`.

## 2. Add the dependency

```sh
opam pin add spark-connect https://github.com/lisancao/spark-connect-ocaml.git
```

## 3. Run a job

```ocaml
open Lwt.Infix
open Spark_connect

let () =
  Lwt_main.run begin
    Session.connect "sc://localhost:15002" >>= fun spark ->
    Session.range spark 10L >>= fun df ->
    let squares =
      Dataframe.with_column df "sq" Column.O.(Column.col "id" * Column.col "id")
    in
    Dataframe.filter squares Column.O.(Column.col "sq" > Column.int 20)
    >>= Dataframe.show
  end
```

`Dataframe.show` prints a table; `Dataframe.collect` returns `Row.t list` for
further processing in OCaml.

## Connect string

The argument to `Session.connect` is a Spark Connect connection string. See
[Connecting](connecting.md) for tokens, TLS and custom headers.
