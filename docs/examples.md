# Examples

Runnable programs live in
[`examples/`](https://github.com/lisancao/spark-connect-ocaml/tree/main/examples).
Each connects to the server named by `SPARK_REMOTE` (default `sc://localhost:15002`):

```sh
SPARK_REMOTE=sc://localhost:15002 dune exec examples/quickstart.exe
```

| Program | Shows |
|---|---|
| `quickstart` | range, derived column, grouped aggregation, `show` |
| `sql_and_views` | temp views, SQL with named and positional parameters, SQL results as DataFrames |
| `window_functions` | rank within a partition and a running total |
| `read_write` | partitioned Parquet on the server's filesystem, read back, `save_as_table` |
| `word_count` | `regexp_replace`, `split`, `explode`, group and count |
| `smoke` | a minimal end-to-end check, including error handling |
| `local_df` | `create_dataframe` with every Arrow type the writer supports |
| `nyc_taxi` | a batch job over a month of NYC taxi trips on a standalone cluster |
| `options_desk` | an options desk: FRED data through a Python data source, realized volatility in Spark, Black-Scholes pricing in OCaml, a metric view risk layer, and Delta `MERGE` and time travel |

## Quickstart

```ocaml
--8<-- "examples/quickstart.ml"
```

```
+------+---+------+
|bucket|  n| total|
+------+---+------+
|     0|333|166833|
|     1|333|166167|
|     2|333|166500|
+------+---+------+
```

## Window functions

```
+------+-----+------+----+-------------+
|region|month|amount|rank|running_total|
+------+-----+------+----+-------------+
|  east|    1|   100|   2|          100|
|  east|    2|   140|   1|          240|
|  east|    3|    90|   3|          330|
|  west|    1|    70|   2|           70|
|  west|    2|   120|   1|          190|
+------+-----+------+----+-------------+
```

## NYC taxi on a cluster

[`examples/nyc_taxi`](https://github.com/lisancao/spark-connect-ocaml/tree/main/examples/nyc_taxi)
reads a month of NYC yellow-cab trips (about 3 million rows) and the zone lookup, cleans
and derives columns, broadcast-joins the zones, aggregates per zone, ranks zones per
borough with a window function, and writes Parquet. Its README brings up a standalone
cluster (a master and two workers) with Docker and shows a full run.

## Options desk

[`examples/options_desk`](https://github.com/lisancao/spark-connect-ocaml/tree/main/examples/options_desk)
reads ten years of index closes from FRED through a Python data source, computes realized
volatility and drawdowns in Spark, prices 30-day options with an OCaml Black-Scholes
library, defines the desk's risk measures as a metric view, and merges the next day's
fills into a Delta book with time travel for audit. Its README brings up the cluster and
shows a full run.
