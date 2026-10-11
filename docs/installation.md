# Installation

## Requirements

- OCaml 5.0 or newer and [opam](https://opam.ocaml.org/)
- A reachable Spark Connect server (Apache Spark 4.x)

## Install the library

The package isn't on the opam repository yet; pin it from GitHub:

```sh
opam pin add spark-connect https://github.com/lisancao/spark-connect-ocaml.git
```

Or build from a checkout:

```sh
git clone https://github.com/lisancao/spark-connect-ocaml.git
cd spark-connect-ocaml
opam install . --deps-only --with-test
dune build
dune install
```

Then depend on it from your `dune` file:

```lisp
(executable
 (name main)
 (libraries spark-connect lwt lwt.unix))
```

Installing the package also installs `sparkc`, a small SQL runner (see
[Configuration and connection](configuration.md#sparkc)).

## Start a Spark Connect server

Spark 4.0 and later ship the Connect server in the standard download. With a local
distribution (Java 17 or newer):

```sh
curl -L https://archive.apache.org/dist/spark/spark-4.2.0/spark-4.2.0-bin-hadoop3.tgz -o spark.tgz
tar xzf spark.tgz && cd spark-4.2.0-bin-hadoop3
./sbin/start-connect-server.sh
```

Or with Docker, using the official image:

```sh
docker run -d --name spark-connect -p 127.0.0.1:15002:15002 \
  --entrypoint /opt/spark/sbin/start-connect-server.sh \
  apache/spark:4.2.0-scala2.13-java21-python3-ubuntu --wait
```

Either way the server listens on `sc://localhost:15002`. The server listens on every
network interface by default; the Docker command above publishes the port on
localhost only, and with a local distribution you can add
`--conf spark.connect.grpc.binding.address=127.0.0.1`.

Check the connection with `sparkc`:

```sh
sparkc -r sc://localhost:15002 -e "SELECT version()"
```
