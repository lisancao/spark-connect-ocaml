# Arrow IPC

The Arrow codec is a standalone part of the package: `Arrow` (the reader) and
`Arrow_writer` (the writer) are usable on their own, with no server.

## Reading

```ocaml
let schema, batches = Spark_connect.Arrow.read_stream bytes in
```

`Arrow_ipc` decodes the IPC streaming framing, FlatBuffers metadata and the
column types Spark produces (including nested lists, structs and maps). It
reports `Arrow.Unsupported` for the few encodings it does not handle —
dictionary-encoded and compressed batches, which Spark does not send by default.

## Writing

```ocaml
let bytes = Spark_connect.Arrow_writer.encode ~columns:[ "id"; "name" ] rows in
```

`Arrow_writer.encode` turns OCaml values into an Arrow IPC stream. Its output
validates with `pyarrow`'s full validation, and it is what
`Session.create_dataframe` sends as a `LocalRelation`.

## Tests

The fixtures under `test/fixtures` were produced with `pyarrow` and cover every
Arrow type the reader handles, plus multi-batch, legacy framing, empty,
dictionary and compressed streams.
