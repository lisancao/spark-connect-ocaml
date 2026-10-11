# Installation

## Requirements

- OCaml 5.0 or newer, with opam.
- A Spark 4.x Connect server to talk to (see [Quickstart](quickstart.md) for a
  one-command Docker server).

## From GitHub

```sh
opam pin add spark-connect https://github.com/lisancao/spark-connect-ocaml.git
```

## From a checkout

```sh
opam install . --deps-only --with-test
dune build
dune install
```

## The `sparkc` runner

The package also installs `sparkc`, a small SQL runner useful for poking at a
server without writing OCaml:

```sh
sparkc -r sc://localhost:15002 -e "SELECT current_timestamp(), version()"
sparkc --json -n 5 -e "SELECT * FROM my_table"
echo "SHOW TABLES;" | sparkc --csv
sparkc            # interactive; statements end with ';'
```

`SPARK_REMOTE` sets the default URL, as in PySpark.
