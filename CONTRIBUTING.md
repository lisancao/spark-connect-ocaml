# Contributing

Thanks for helping. The full guide, including running the integration tests against
a Spark Connect server, regenerating the coverage matrix and building the docs, is at
[lisancao.github.io/spark-connect-ocaml/contributing](https://lisancao.github.io/spark-connect-ocaml/contributing/)
(source: [docs/contributing.md](docs/contributing.md)).

The short version:

```sh
opam install . --deps-only --with-test --with-doc
dune build
dune fmt
dune test                                        # no server needed
SPARK_REMOTE=sc://localhost:15002 dune test      # plus the integration suite
```

New API should come with an integration test that checks concrete values, and
`docs/protocol-coverage.md` should be regenerated with `scripts/coverage_matrix.py`.
