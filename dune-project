(lang dune 3.0)
(name spark-connect)
(generate_opam_files true)
(source (github lisanatashacao/spark-connect-ocaml))
(authors "Lisa Cao")
(maintainers "Lisa Cao")
(license Apache-2.0)

(package
 (name spark-connect)
 (synopsis "Spark Connect client for OCaml")
 (description
  "A pure OCaml client for Apache Spark's Spark Connect protocol: gRPC over h2, protobuf via ocaml-protoc, and a built-in Arrow IPC reader/writer. Build DataFrame plans in OCaml and run them on any Spark 4.x Connect server.")
 (depends
  (ocaml (>= 5.0))
  dune
  (pbrt (>= 4.0))
  (ocaml-protoc (>= 4.0))
  (h2 (>= 0.13))
  (h2-lwt-unix (>= 0.13))
  (lwt (>= 5.0))
  uri
  bigstringaf
  (tls-lwt (>= 1.0))
  ca-certs
  domain-name
  (alcotest :with-test)))
