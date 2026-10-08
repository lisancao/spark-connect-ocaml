# Vendored protobuf definitions

- `spark/connect/*.proto`: the Spark Connect API, copied from the Apache Spark
  repository at the tag recorded in `spark/connect/VERSION`. Apache License 2.0,
  copyright the Apache Software Foundation. Refresh with
  `scripts/update-protos.sh <tag>`; that script also applies the two cosmetic
  rewrites ocaml-protoc needs (double-quoted strings, no `reserved "name"`).
- `google/protobuf/any.proto`, `google/protobuf/timestamp.proto`,
  `google/rpc/status.proto`, `google/rpc/error_details.proto`: minimal
  hand-written equivalents of the well-known types the Spark protos and gRPC
  error details depend on. Wire-compatible with the originals.

The OCaml modules are generated at build time by `ocaml-protoc` (see
`src/proto/dune`) and post-processed by `src/proto/fixup` to work around two
ocaml-protoc 4.1 code generation issues (a forward reference in a recursive
default, and key/payload order for unit-typed oneof cases).
