# NYC taxi job

A batch job written with this client, run on a real Spark cluster. It reads a
month of NYC yellow-cab trips (January 2024, about 3 million rows) and the
taxi-zone lookup, then:

1. drops bad records (non-positive distance or fare, pickups outside January,
   trips shorter than a minute or longer than four hours);
2. derives trip duration, tip percentage and pickup date;
3. joins the pickup zone and borough (the lookup is broadcast);
4. aggregates trips, average fare, tip and duration, and revenue per zone;
5. ranks zones within each borough with a window function and keeps the top
   three;
6. builds a daily trend;
7. writes both results as Parquet and reads them back.

The source is [`../nyc_taxi.ml`](../nyc_taxi.ml).

## Run it

Get the data, from the [NYC TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):

```sh
mkdir -p data && chmod 777 data   # the Spark containers write data/out as uid 185
curl -L -o data/yellow_tripdata_2024-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
curl -L -o data/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Start a standalone cluster (one master, two workers with two cores each) with a
Spark Connect server in front of it. `CONNECT_PORT`, `UI_PORT` and
`MASTER_UI_PORT` change the published ports if 15002, 4040 or 18080 are taken.

```sh
TAXI_DIR=$PWD/data docker compose -f examples/nyc_taxi/compose.yaml up -d
```

Run the job. `TAXI_DATA` is the data directory as the server sees it:

```sh
TAXI_DATA=/data SPARK_REMOTE=sc://localhost:15002 dune exec examples/nyc_taxi.exe
```

Stop the cluster with `docker compose -f examples/nyc_taxi/compose.yaml down`.

## A run

On the cluster above (Spark 4.2.0, published on port 15302 for this run),
with the timings from a warm second run:

```
Spark 4.2.0 at sc://localhost:15302

steps
  count raw trips                        0.17s
  count clean trips                      0.27s
  write top_zones (parquet)              1.71s
  write daily (parquet)                  0.55s
  read results back                      0.20s

trips: 2964624 raw, 2859106 kept after cleaning (3.6% dropped)
wrote 19 top-zone rows and 31 daily rows to /data/out

Top 3 pickup zones per borough
+-------------+----------------------------+------+--------+-----------+-----------+--------+----+
|      Borough|                        Zone| trips|avg_fare|avg_tip_pct|avg_minutes| revenue|rank|
+-------------+----------------------------+------+--------+-----------+-----------+--------+----+
|        Bronx|      Mott Haven/Port Morris|   428|   31.11|        2.0|       28.1|   14713|   1|
|        Bronx|                  Co-Op City|   362|    40.3|        0.3|       46.6|   15943|   2|
|        Bronx|East Concourse/Concourse ...|   299|    29.1|        0.6|       39.0|    9514|   3|
|     Brooklyn| Downtown Brooklyn/MetroTech|  1279|   26.39|       13.0|       22.5|   42063|   1|
|     Brooklyn|            Brooklyn Heights|  1161|   26.25|       18.1|       23.3|   38323|   2|
|     Brooklyn|                 Fort Greene|  1045|   23.02|       10.4|       22.9|   29061|   3|
|          EWR|              Newark Airport|    17|   77.01|       12.0|       21.6|    1617|   1|
|    Manhattan|              Midtown Center|139827|   15.48|       21.6|       13.9| 3347113|   1|
|    Manhattan|       Upper East Side South|139825|   12.37|       22.8|       11.0| 2765222|   2|
|    Manhattan|       Upper East Side North|133598|   12.85|       22.6|       11.2| 2706117|   3|
|          N/A|              Outside of NYC|   494|   69.28|        6.9|       36.0|   39479|   1|
|       Queens|                 JFK Airport|137003|   63.05|       14.2|       37.5|11118984|   1|
|       Queens|           LaGuardia Airport| 87452|   42.33|       21.3|       27.2| 5805634|   2|
|       Queens|               East Elmhurst| 11728|   41.81|       20.8|       27.1|  760187|   3|
|Staten Island|     Arrochar/Fort Wadsworth|     7|   59.24|        7.3|       18.2|     501|   1|
|Staten Island|    South Beach/Dongan Hills|     4|   23.74|        0.0|       22.3|     143|   2|
|Staten Island| Heartland Village/Todt Hill|     3|  100.62|        1.8|       64.4|     349|   3|
|Staten Island|   Saint George/New Brighton|     3|   39.33|        0.0|       27.3|     139|   3|
|      Unknown|                         N/A|  9478|    18.6|       20.4|       14.9|  255604|   1|
+-------------+----------------------------+------+--------+-----------+-----------+--------+----+
Busiest and quietest days
+-----------+------+-------+
|pickup_date| trips|revenue|
+-----------+------+-------+
| 2024-01-27|106823|2676394|
| 2024-01-18|106651|2896187|
| 2024-01-25|106649|2958598|
+-----------+------+-------+
only showing top 3 rows
+-----------+-----+-------+
|pickup_date|trips|revenue|
+-----------+-----+-------+
| 2024-01-07|64877|1857652|
| 2024-01-02|72665|2241775|
| 2024-01-15|74458|2129177|
+-----------+-----+-------+
only showing top 3 rows
```

The Spark application UI (port 4040, or its REST API at
`/api/v1/applications/<app>/executors`) shows the work spread across both
workers' executors, with every job tagged with the client's Spark Connect
session id.
