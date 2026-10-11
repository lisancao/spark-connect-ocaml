# Contributing

Issues and pull requests are welcome at
[lisancao/spark-connect-ocaml](https://github.com/lisancao/spark-connect-ocaml).

## Development setup

```sh
opam install . --deps-only --with-test --with-doc
dune build
dune test                 # unit and Arrow tests; no server needed
```

## Integration tests

The integration suite runs when `SPARK_REMOTE` is set:

```sh
docker run -d --name spark-connect -p 127.0.0.1:15002:15002 \
  --entrypoint /opt/spark/sbin/start-connect-server.sh \
  apache/spark:4.2.0-scala2.13-java21-python3-ubuntu --wait
SPARK_REMOTE=sc://localhost:15002 dune test
```

New API should come with an integration test that checks concrete values.

## Coverage matrix

`docs/protocol-coverage.md` is generated; regenerate it after changing the API or the
tests, with one `--remote` per server you want in the version table:

```sh
dune build
python3 scripts/coverage_matrix.py --remote 4.2.0=sc://localhost:15002
```

## Formatting

```sh
dune fmt
```

## Updating the protos

```sh
scripts/update-protos.sh v4.2.0
```

copies the Spark Connect protos from a Spark tag and applies the rewrites
`ocaml-protoc` needs (see `proto/README.md`).

## Docs

The site is built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/),
and the API reference with odoc:

```sh
pip install mkdocs-material
mkdocs serve              # http://127.0.0.1:8000
dune build @doc           # _build/default/_doc/_html
```

## CI

`.github/workflows/ci.yml` builds and runs the unit tests on OCaml 5.0 through 5.3,
checks formatting, and builds the API docs. `.github/workflows/integration.yml` runs the
integration suite against the official Spark 4.1.3 and 4.2.0 images. Both run on every
push to `main`, every pull request, and on demand. `.github/workflows/coverage.yml`
regenerates `docs/protocol-coverage.md` against live servers when run by hand, and
`.github/workflows/docs.yml` publishes the site to GitHub Pages when the docs, the API
or the examples change on `main`.

## Releasing

1. Set the version in `src/session.ml` (the client type the server sees, for example
   `spark-connect-ocaml/0.1.0`) and give it a section in `CHANGELOG.md`.
2. Tag and push: `git tag v0.1.0 && git push origin v0.1.0`.
3. `.github/workflows/release.yml` builds and tests the tag and publishes a GitHub
   Release with the source archive.
4. Submit the package to [opam-repository](https://github.com/ocaml/opam-repository),
   for example with `opam publish`.
