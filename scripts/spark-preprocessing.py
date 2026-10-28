"""Clean the trip updates in HDFS and join them with the timetable.

Reads what hdfs-ingression.py put into HDFS and writes one table with one row
per stop a vehicle really made, ready for the analytics step:

    /sl/stop_events/service_date=2026-09-15/part-*.parquet

Steps:
  1. keep the newest row per (trip, stop, service day) across all hour folders,
     since the ingestion only kept the newest one inside each hour
  2. drop stops that were still a prediction the last time SL sent them
  3. pick the delay (departure at the first stop, arrival everywhere else)
     and drop missing values and impossible delays
  4. join with trips, routes, stops and stop_times of the timetable that was
     valid on the service day, since trip ids change between timetables
  5. add the scheduled local hour, weekday/weekend, transport mode and station

Usage:
    python scripts/spark-preprocessing.py
    python scripts/spark-preprocessing.py --base /some/local/copy/of/sl
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = "hdfs://localhost:9000/sl"

# The service day, hour folders and scheduled times are all Stockholm time.
TIMEZONE = "Europe/Stockholm"

# Anything outside this range is a feed error rather than a real delay.
MIN_DELAY = -15 * 60
MAX_DELAY = 2 * 3600

# A stop counts as late from 3 minutes on.
LATE = 3 * 60

TRANSPORT_MODES = {100: "train", 401: "metro", 700: "bus", 900: "tram", 1000: "ferry"}


def spark_session():
    return (SparkSession.builder
            .appName("sl-preprocessing")
            .config("spark.driver.memory", "6g")
            .config("spark.sql.session.timeZone", TIMEZONE)
            # Spark does not read hdfs-site.xml, so tell it about the single node
            .config("spark.hadoop.dfs.replication", "1")
            .getOrCreate())


def read_gtfs(spark, base, table, columns):
    """One timetable table, with the date of the timetable it came from."""
    return (spark.read.option("basePath", f"{base}/gtfs/{table}")
            .parquet(f"{base}/gtfs/{table}")
            .select(*columns, F.col("date").alias("feed_date")))


def latest_per_stop(trip_updates):
    """
    The same stop shows up in several hour folders while the trip is running,
    the newest one is what really happened
    """
    newest_first = (Window.partitionBy("trip_id", "stop_sequence", "trip_start_date")
                    .orderBy(F.desc("date"), F.desc("hour"), F.desc("snapshot_time")))
    return (trip_updates
            .withColumn("rank", F.row_number().over(newest_first))
            .where(F.col("rank") == 1)
            .drop("rank"))


def clean(stops_seen, first_stops):
    """Keep the stops that happened, with one plausible delay each."""
    # the folders are named after the Stockholm hour the snapshots were taken in
    last_seen = F.unix_timestamp(F.format_string("%s %02d:00:00", F.col("date").cast("string"),
                                                 F.col("hour"))) + 3600

    # at the first stop vehicles arrive early and wait, so only the departure counts
    first = F.col("first_sequence") == F.col("stop_sequence")
    delay = F.when(first, F.coalesce("departure_delay", "arrival_delay")) \
             .otherwise(F.coalesce("arrival_delay", "departure_delay"))
    time = F.when(first, F.coalesce("departure_time", "arrival_time")) \
            .otherwise(F.coalesce("arrival_time", "departure_time"))

    return (stops_seen
            .withColumn("feed_date", F.to_date("trip_start_date", "yyyyMMdd"))
            .join(first_stops, ["trip_id", "feed_date"], "left")
            .withColumn("delay", delay)
            .withColumn("actual_unix", time)
            .where(F.col("delay").isNotNull())
            .where(F.col("actual_unix") <= last_seen)
            .where(F.col("delay").between(MIN_DELAY, MAX_DELAY)))


def gtfs_seconds(column):
    """'25:10:00' -> seconds after the start of the service day (can pass 24h)."""
    parts = F.split(column, ":")
    return parts[0].cast("int") * 3600 + parts[1].cast("int") * 60 + parts[2].cast("int")


def enrich(events, trips, routes, stops, stop_times):
    modes = F.create_map(*[F.lit(x) for pair in TRANSPORT_MODES.items() for x in pair])
    stations = stops.select(
        F.col("stop_id").alias("station_id"), "feed_date",
        F.col("stop_name").alias("station_name"),
        F.col("stop_lat").alias("station_lat"), F.col("stop_lon").alias("station_lon"))

    # GTFS times count from noon minus 12h, which is midnight except on DST days
    service_day_start = F.unix_timestamp(F.concat("trip_start_date", F.lit(" 12:00:00")),
                                         "yyyyMMdd HH:mm:ss") - 12 * 3600

    joined = (events
              .join(trips, ["trip_id", "feed_date"])
              .join(routes, ["route_id", "feed_date"])
              .join(stops, ["stop_id", "feed_date"])
              .withColumn("station_id", F.coalesce(F.nullif("parent_station", F.lit("")), "stop_id"))
              .join(stations, ["station_id", "feed_date"], "left")
              .join(stop_times, ["trip_id", "stop_sequence", "feed_date"], "left"))

    scheduled = F.coalesce(
        service_day_start + gtfs_seconds(F.when(F.col("first_sequence") == F.col("stop_sequence"),
                                                F.col("scheduled_departure"))
                                         .otherwise(F.col("scheduled_arrival"))),
        F.col("actual_unix") - F.col("delay"))

    return (joined
            .withColumn("scheduled_time", F.timestamp_seconds(scheduled))
            .withColumn("actual_time", F.timestamp_seconds("actual_unix"))
            .select(
                F.col("feed_date").alias("service_date"),
                "trip_id", "stop_sequence",
                "route_id", "route_short_name", "route_desc", "route_type",
                F.coalesce(modes[F.col("route_type")], F.lit("other")).alias("transport_mode"),
                "direction_id", "agency_id",
                "stop_id", "stop_name", "platform_code",
                "station_id", F.coalesce("station_name", "stop_name").alias("station_name"),
                F.coalesce("station_lat", "stop_lat").alias("station_lat"),
                F.coalesce("station_lon", "stop_lon").alias("station_lon"),
                "scheduled_time", "actual_time",
                F.col("delay").alias("delay_seconds"),
                (F.col("delay") >= LATE).alias("is_late"),
                F.hour("scheduled_time").alias("hour_of_day"),
                F.date_format("feed_date", "EEEE").alias("day_of_week"),
                F.when(F.dayofweek("feed_date").isin(1, 7), "weekend")
                 .otherwise("weekday").alias("day_type")))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default=HDFS, help=f"folder holding gtfs/ and tripupdates/ (default {HDFS})")
    parser.add_argument("--out", help="where to write the result (default <base>/stop_events)")
    args = parser.parse_args()
    out = args.out or f"{args.base}/stop_events"

    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    trip_updates = spark.read.option("basePath", f"{args.base}/tripupdates") \
                        .parquet(f"{args.base}/tripupdates")
    trips = read_gtfs(spark, args.base, "trips", ["trip_id", "route_id", "direction_id"])
    routes = read_gtfs(spark, args.base, "routes",
                       ["route_id", "route_short_name", "route_desc", "route_type", "agency_id"])
    stops = read_gtfs(spark, args.base, "stops",
                      ["stop_id", "stop_name", "stop_lat", "stop_lon", "parent_station", "platform_code"])
    stop_times = read_gtfs(spark, args.base, "stop_times", [
        "trip_id", "stop_sequence",
        F.col("arrival_time").alias("scheduled_arrival"),
        F.col("departure_time").alias("scheduled_departure")])
    first_stops = stop_times.groupBy("trip_id", "feed_date") \
                            .agg(F.min("stop_sequence").alias("first_sequence"))

    stops_seen = latest_per_stop(trip_updates).cache()
    events = clean(stops_seen, first_stops).cache()
    result = enrich(events, trips, routes, stops, stop_times)

    result.write.mode("overwrite").partitionBy("service_date").parquet(out)

    written = spark.read.parquet(out)
    print(f"\nsnapshot rows            {trip_updates.count():>12,}")
    print(f"distinct trip stops      {stops_seen.count():>12,}")
    print(f"after cleaning           {events.count():>12,}")
    print(f"joined with timetable    {written.count():>12,}  -> {out}")
    written.groupBy("service_date", "day_type").agg(
        F.count("*").alias("stops"),
        F.round(F.avg("delay_seconds"), 1).alias("avg_delay_s"),
        F.round(F.avg(F.col("is_late").cast("int")) * 100, 1).alias("late_%"),
    ).orderBy("service_date").show(100)

    spark.stop()


if __name__ == "__main__":
    main()
