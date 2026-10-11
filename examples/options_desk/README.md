# Options desk

A small options desk run through Spark Connect from OCaml. It asks one question:
should 30-day index options be priced off realized volatility or off the VIX?

1. **Ingest.** Daily S&P 500, Nasdaq Composite and Dow Jones closes come from
   [FRED](https://fred.stlouisfed.org/) through a Python data source
   ([`pyspark_fred/`](pyspark_fred/__init__.py)) installed on every node, and land in a
   Delta table.
2. **Analytics in Spark.** Daily log returns, 21-day realized volatility, drawdowns, and
   realized volatility against the VIX, all as window functions built in OCaml.
3. **Pricing in OCaml.** 30-day at-the-money options priced with the closed-form
   Black-Scholes module from Case Ollila's
   [option-pricing-model-comparison](https://github.com/crollila/option-pricing-model-comparison)
   (MIT, vendored unchanged in [`option_pricer/`](option_pricer/)). The priced book goes
   back to Spark as a Delta table.
4. **Risk layer.** A metric view (Spark 4.2+) defines premium, dollar delta, vega and
   theta once; the program queries it with `measure()` from the DataFrame API.
5. **Next day.** Fills are merged into the book with `MERGE`, and yesterday's book is
   read back with Delta time travel.

Every step that can be cross-checked prints PASS or FAIL: put-call parity for each
index, the metric view against the same aggregation on the book table, and time travel
against the book before the merge.

## Run it

The cluster is a standalone Spark 4.2 (one master, two workers) behind a Spark Connect
server, with Delta Lake loaded and PyArrow installed for the Python data source. The
nodes need internet access to reach FRED.

```sh
docker compose -f examples/options_desk/compose.yaml up -d --build
SPARK_REMOTE=sc://localhost:15002 dune exec examples/options_desk/options_desk.exe
docker compose -f examples/options_desk/compose.yaml down -v
```

`CONNECT_PORT`, `UI_PORT` and `MASTER_UI_PORT` change the published ports if 15002, 4040
or 18080 are taken. `DESK_SINCE` sets the start date (default `2016-10-10`; FRED carries
ten years of S&P and Dow Jones history).

## Data

The series are fetched from FRED (Federal Reserve Bank of St. Louis) when the job runs;
nothing is stored in this repository. The index series are copyrighted by their owners
(S&P Dow Jones Indices, Nasdaq and Cboe); see each series' notes on FRED before reusing
the data.

## Known issue

`CREATE OR REPLACE VIEW ... WITH METRICS` fails with
`TABLE_OR_VIEW_ALREADY_EXISTS` when the metric view exists (Spark 4.2.0 and 4.3.0-rc1,
session catalog), so the program drops the view before creating it.

## A run

On the cluster above, published on port 15802 for this run:

```
Spark 4.2.0 at sc://localhost:15802

== 1. Ingest: FRED through the Python data source, into Delta
+---------+--------+----------+----------+
|   series|count(1)| min(date)| max(date)|
+---------+--------+----------+----------+
|     DJIA|    2514|2016-10-10|2026-10-09|
|NASDAQCOM|    2515|2016-10-10|2026-10-09|
|    SP500|    2514|2016-10-10|2026-10-09|
+---------+--------+----------+----------+

== 2. Analytics in Spark: returns, realized volatility, drawdowns
+---------+----------+--------+--------+------------+----------------+-------------+
|   series|      date|    spot|rv21_pct|avg_rv21_pct|max_drawdown_pct|worst_day_pct|
+---------+----------+--------+--------+------------+----------------+-------------+
|     DJIA|2026-10-09|51654.95|    10.2|        14.3|           -37.1|        -13.8|
|NASDAQCOM|2026-10-09|27366.17|    14.2|        19.4|           -36.4|        -13.1|
|    SP500|2026-10-09| 7811.54|    10.0|        15.0|           -33.9|        -12.8|
+---------+----------+--------+--------+------------+----------------+-------------+
+----+------------------------------+----------------------+
|days|avg_implied_minus_realized_pts|pct_days_implied_above|
+----+------------------------------+----------------------+
|2511|                           3.7|                  86.1|
+----+------------------------------+----------------------+

== 3. Pricing in OCaml: 30-day options with option_pricer's Black-Scholes
  risk-free (3m T-bill) 4.23%, VIX 15.4
  index      kind contracts     strike    vol  premium   delta   vega/pt theta/day
  SP500      call        20    7811.54  10.0%   103.40   0.554      8.85     -1.96
  SP500      put        -10    7811.54  10.0%    76.28  -0.446      8.85     -1.06
  NASDAQCOM  call         5   27366.17  14.2%   492.76   0.542     31.13     -9.03
  DJIA       put          8   51654.95  10.2%   516.15  -0.447     58.56     -7.22
  PASS  put-call parity, SP500: gap 4.55e-13
  PASS  put-call parity, NASDAQCOM: gap 0.00e+00
  PASS  put-call parity, DJIA: gap 0.00e+00
  SP500 30-day ATM call: 103.40 at realized vol (10.0%), 151.41 at VIX (15.4%)
  PASS  priced book written to Delta: 4 positions

== 4. Risk layer: a metric view over the book
+----------+-----------+------------+---------------+-----------------+
|index_name|premium_usd|dollar_delta|vega_usd_per_pt|theta_usd_per_day|
+----------+-----------+------------+---------------+-----------------+
|      DJIA|     412920|   -18467411|          46844|            -5775|
| NASDAQCOM|     246379|     7417769|          15563|            -4514|
|     SP500|     130507|    12138611|           8853|            -2867|
+----------+-----------+------------+---------------+-----------------+
  PASS  metric view = direct aggregation on the book

== 5. Next day: merge fills, audit yesterday's book by time travel
+-------+--------------------+
|version|           operation|
+-------+--------------------+
|      0|CREATE OR REPLACE...|
|      1|               MERGE|
+-------+--------------------+
  yesterday: [DJIA, put, 8] [NASDAQCOM, call, 5] [SP500, call, 20] [SP500, put, -10]
  today:     [DJIA, put, 8] [SP500, call, 25] [SP500, put, -10]
  PASS  time travel returns yesterday's book: 4 positions yesterday, 3 today

all checks passed
```
