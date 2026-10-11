# Security

Please report security issues privately through GitHub's
[security advisories](https://github.com/lisancao/spark-connect-ocaml/security/advisories/new)
rather than in a public issue.

Vulnerabilities in Apache Spark itself, including the Spark Connect server, should go
to the Apache Spark project: <https://spark.apache.org/security.html>.

Notes for users:

- Bearer tokens in connection strings turn TLS on; certificates are validated against
  the system trust store.
- A Spark Connect server listens on every network interface by default. Bind it to
  localhost (`spark.connect.grpc.binding.address=127.0.0.1`) or put it behind
  authentication when the network isn't trusted.
