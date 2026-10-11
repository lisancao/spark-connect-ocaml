# Contributing

Bug reports and pull requests are welcome at
[lisancao/spark-connect-ocaml](https://github.com/lisancao/spark-connect-ocaml).

## Build and test

```sh
opam install . --deps-only --with-test
dune build
dune test              # Arrow codec and unit tests, no server needed
```

Integration tests need a server; see the [Quickstart](quickstart.md) for the
Docker command, then:

```sh
SPARK_REMOTE=sc://localhost:15002 dune test
```

## Formatting

Formatting is checked in CI with ocamlformat 0.29.0:

```sh
dune build @fmt
```

## Generated docs

This site is MkDocs Material for the guides plus odoc for the API reference. The
protocol-coverage page ([Protocol coverage](COVERAGE.md)) is generated, not
hand-written:

```sh
dune build
python3 scripts/coverage_matrix.py --remote 4.2.0=sc://localhost:15002
```

## CI

[`ci.yml`](https://github.com/lisancao/spark-connect-ocaml/blob/main/.github/workflows/ci.yml)
builds and tests on every OCaml version from 5.0 to 5.3 and checks formatting.
[`integration.yml`](https://github.com/lisancao/spark-connect-ocaml/blob/main/.github/workflows/integration.yml)
runs the integration suite against Spark 4.1.3 and 4.2.0 servers started with
Docker.
