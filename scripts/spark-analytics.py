"""Answer the project questions from the cleaned stop events in HDFS.

Reads /sl/stop_events (written by spark-preprocessing.py) and writes one small
table per question, both to HDFS and as CSV for the dashboard:

    lines       which lines have the highest average delay
    stations    which stations are delayed most often, and which are busiest
    hourly      during which hours delays are worst, weekday vs weekend
    day_types   weekday vs weekend, per transport mode
    histogram   how delays are distributed, in one-minute bins
    daily       one row per service day, to spot odd days

    /sl/analytics/<table>/part-*.parquet
    data/analytics/<table>.csv

Usage:
    python scripts/spark-analytics.py
    python scripts/spark-analytics.py --base /some/local/copy/of/sl
"""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = "hdfs://localhost:9000/sl"
ROOT = Path(__file__).resolve().parent.parent
EXPORT = ROOT / "data" / "analytics"

TIMEZONE = "Europe/Stockholm"

# A line or station is only ranked with enough stops behind its numbers,
# counted per day so the bar stays the same however many days are loaded.
MIN_LINE_STOPS_PER_DAY = 100
MIN_STATION_STOPS_PER_DAY = 50


def spark_session():
    return (SparkSession.builder
            .appName("sl-analytics")
            .config("spark.driver.memory", "6g")
            .config("spark.sql.session.timeZone", TIMEZONE)
            # Spark does not read hdfs-site.xml, so tell it about the single node
            .config("spark.hadoop.dfs.replication", "1")
            .getOrCreate())


def delay_stats():
    """The same measures for every grouping, so the tables can be compared.
    Left unrounded so rankings use exact values, save() rounds them."""
    return [
        F.count("*").alias("stops"),
        F.avg("delay_seconds").alias("avg_delay_s"),
        F.median("delay_seconds").alias("median_delay_s"),
        F.percentile("delay_seconds", 0.9).alias("p90_delay_s"),
        F.sum(F.col("is_late").cast("int")).alias("late_stops"),
        (F.avg(F.col("is_late").cast("int")) * 100).alias("late_pct"),
    ]


def latest(*columns):
    """
    Names and coordinates can change between timetables (a stop moved 70 m),
    take them from the newest day instead of splitting the group
    """
    return [F.max_by(column, "service_date").alias(column) for column in columns]


def with_all(events, column):
    """The events once as they are and once more with `column` set to 'all'."""
    return events.unionByName(events.withColumn(column, F.lit("all")))


def ranked(table, enough, order, name):
    """Rank the rows that have enough stops, leave the rest unranked."""
    window = Window.partitionBy(enough).orderBy(*order)
    return table.withColumn(name, F.when(enough, F.row_number().over(window)))


def lines(events, days):
    # line names repeat across modes (14 is a metro line and a ferry), so group by route_id
    table = events.groupBy("route_id").agg(
        *latest("transport_mode", "route_short_name", "route_desc"),
        F.countDistinct("trip_id", "service_date").alias("trips"), *delay_stats())
    enough = F.col("stops") >= MIN_LINE_STOPS_PER_DAY * days
    return ranked(table, enough, [F.desc("avg_delay_s"), F.desc("stops"), "route_id"], "rank_avg_delay")


def stations(events, days):
    # station names repeat too (Slussen is five stations), so group by station_id
    table = events.groupBy("station_id").agg(
        *latest("station_name", "station_lat", "station_lon"),
        F.array_join(F.array_sort(F.collect_set("transport_mode")), ", ").alias("modes"),
        F.countDistinct("route_id").alias("lines"),
        *delay_stats())
    table = table.withColumn("stops_per_day", F.col("stops") / days)
    enough = F.col("stops") >= MIN_STATION_STOPS_PER_DAY * days
    table = ranked(table, enough, [F.desc("late_pct"), F.desc("stops"), "station_id"], "rank_late_pct")
    return ranked(table, F.lit(True), [F.desc("stops"), "station_id"], "rank_busiest")


def hourly(events):
    return with_all(events, "day_type").groupBy("day_type", "hour_of_day").agg(*delay_stats())


def day_types(events):
    return with_all(events, "transport_mode").groupBy("transport_mode", "day_type").agg(
        F.countDistinct("service_date").alias("days"), *delay_stats()
    ).withColumn("stops_per_day", F.col("stops") / F.col("days"))


def histogram(events):
    return events.groupBy(
        "transport_mode", "day_type",
        F.floor(F.col("delay_seconds") / 60).cast("int").alias("delay_minute"),
    ).agg(F.count("*").alias("stops"))


def daily(events):
    return events.groupBy("service_date", "day_of_week", "day_type").agg(*delay_stats())


def save(table, name, base):
    """Small results: one Parquet file in HDFS and one CSV for the dashboard."""
    for column in ["avg_delay_s", "median_delay_s", "p90_delay_s", "late_pct", "stops_per_day"]:
        if column in table.columns:
            table = table.withColumn(column, F.round(column, 1))
    table.coalesce(1).write.mode("overwrite").parquet(f"{base}/analytics/{name}")
    EXPORT.mkdir(parents=True, exist_ok=True)
    rows = table.toPandas()
    # unranked rows are empty, keep the ranks whole numbers instead of 1.0
    for column in [c for c in rows.columns if c.startswith("rank_")]:
        rows[column] = rows[column].astype("Int64")
    rows.to_csv(EXPORT / f"{name}.csv", index=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default=HDFS, help=f"folder holding stop_events/ (default {HDFS})")
    args = parser.parse_args()

    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.parquet(f"{args.base}/stop_events").cache()
    days = events.select("service_date").distinct().count()
    print(f"\n{events.count():,} stop events over {days} days")

    results = {
        "lines": lines(events, days).orderBy(F.asc_nulls_last("rank_avg_delay"), F.desc("stops")),
        "stations": stations(events, days).orderBy(F.asc_nulls_last("rank_late_pct"), "rank_busiest"),
        "hourly": hourly(events).orderBy("day_type", "hour_of_day"),
        "day_types": day_types(events).orderBy("transport_mode", "day_type"),
        "histogram": histogram(events).orderBy("transport_mode", "day_type", "delay_minute"),
        "daily": daily(events).orderBy("service_date"),
    }
    for name, table in results.items():
        save(table, name, args.base)
    print(f"written to {args.base}/analytics/ and {EXPORT.relative_to(ROOT)}/")

    tables = {name: spark.read.parquet(f"{args.base}/analytics/{name}") for name in results}
    print("\nLines with the highest average delay")
    tables["lines"].where("rank_avg_delay <= 10").orderBy("rank_avg_delay").select(
        "rank_avg_delay", "transport_mode", "route_short_name", "route_desc",
        "stops", "avg_delay_s", "median_delay_s", "late_pct").show(truncate=False)
    print("Stations most often 3+ minutes late")
    tables["stations"].where("rank_late_pct <= 10").orderBy("rank_late_pct").select(
        "rank_late_pct", "station_name", "modes", "stops", "late_pct", "avg_delay_s").show(truncate=False)
    print("Busiest stations")
    tables["stations"].where("rank_busiest <= 10").orderBy("rank_busiest").select(
        "rank_busiest", "station_name", "modes", "lines", "stops_per_day", "late_pct").show(truncate=False)
    print("Delay by hour of day (scheduled, Stockholm time)")
    tables["hourly"].groupBy("hour_of_day").pivot("day_type", ["weekday", "weekend", "all"]) \
        .agg(F.first("avg_delay_s")).orderBy("hour_of_day").show(24)
    print("Weekday vs weekend")
    tables["day_types"].orderBy("transport_mode", "day_type").select(
        "transport_mode", "day_type", "days", "stops_per_day", "avg_delay_s",
        "median_delay_s", "p90_delay_s", "late_pct").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
