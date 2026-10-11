"""A Spark Python data source for FRED daily series.

Installed on the server's Python path, it is discovered by Spark from the
``pyspark_`` package prefix and read by format name from any Spark Connect client:

    spark.read.format("fred").option("series", "SP500").load()

Each series becomes rows of (series, date, value); FRED's empty values (market
holidays) are skipped.
"""
import csv
import datetime
import io
import urllib.request

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"


class DefaultSource(DataSource):
    @classmethod
    def name(cls):
        return "fred"

    def schema(self):
        return "series string, date date, value double"

    def reader(self, schema):
        series = [s.strip() for s in self.options.get("series", "").split(",") if s.strip()]
        if not series:
            raise ValueError("the fred data source needs option('series', 'ID[,ID...]')")
        return FredReader(series)


class FredReader(DataSourceReader):
    def __init__(self, series):
        self.series = series

    def partitions(self):
        # one partition per series, so series download in parallel on the executors
        return [InputPartition(s) for s in self.series]

    def read(self, partition):
        series = partition.value
        with urllib.request.urlopen(FRED_CSV.format(series), timeout=60) as resp:
            text = resp.read().decode("utf-8")
        for row in csv.DictReader(io.StringIO(text)):
            value = row.get(series, "")
            if value not in ("", "."):
                yield (series, datetime.date.fromisoformat(row["observation_date"]), float(value))
