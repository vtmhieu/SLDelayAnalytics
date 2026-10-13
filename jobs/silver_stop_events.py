"""Silver job: turn the real-time snapshots into one row per observed stop visit.

Every snapshot repeats the latest prediction for each upcoming stop of a trip,
and a stop stays in the feed for ~10 minutes after the vehicle has passed it.
So for each stop visit (trip_id, start_date, stop_sequence) the last report is
what actually happened. A visit counts as observed when its arrival (or
departure) time is not later than the last snapshot that reported it; the
others are trips that left the feed early and only have predictions.

Observed visits are joined with the static GTFS version of their service date
(trip_ids change between timetable versions) to get line, mode and station.

Run:
    python jobs/silver_stop_events.py                                    # local Spark, data/
    docker compose -f docker/docker-compose.yml exec jupyter \\
        spark-submit jobs/silver_stop_events.py                          # cluster, HDFS
"""

from pyspark.sql import functions as F

from common import (DELAY_THRESHOLD_S, MAX_ABS_DELAY_S, MODES, PERIOD_END, PERIOD_START,
                    path, spark_session)

CSV_OPTIONS = {"header": True, "inferSchema": False}


def read_static(spark, name):
    # feed_date comes from the folder name feed_date=<date>
    return (spark.read.options(**CSV_OPTIONS, pathGlobFilter=name)
            .csv(path("bronze", "static"))
            .withColumn("feed_date", F.col("feed_date").cast("date")))


def stop_visits(updates):
    """Last report of every stop visit."""
    last = F.max_by(F.struct("trip_sched_rel", "stop_id", "stop_sched_rel", "arr_delay", "arr_time",
                             "dep_delay", "dep_time", "vehicle_id"), "snapshot_ts")
    return (updates
            .groupBy("trip_id", "start_date", "stop_sequence")
            .agg(last.alias("last"),
                 F.max("snapshot_ts").alias("last_seen_ts"),
                 F.count("*").alias("n_reports"))
            .select("trip_id", "start_date", "stop_sequence", "last.*", "last_seen_ts", "n_reports")
            .withColumn("event_time", F.coalesce("arr_time", "dep_time"))
            .withColumn("delay_s", F.coalesce("arr_delay", "dep_delay"))
            .withColumn("service_date", F.to_date("start_date", "yyyyMMdd")))


def main():
    spark = spark_session("silver_stop_events")
    spark.sparkContext.setLogLevel("WARN")

    updates = spark.read.parquet(path("bronze", "tripupdates"))
    visits = stop_visits(updates).cache()

    in_period = F.col("service_date").between(F.lit(PERIOD_START), F.lit(PERIOD_END))
    observed = F.coalesce(F.col("event_time") <= F.col("last_seen_ts"), F.lit(False))
    # not an added/canceled trip or a skipped stop
    valid = F.coalesce((F.col("trip_sched_rel") == 0) & (F.col("stop_sched_rel") == 0), F.lit(False))
    plausible = F.abs("delay_s") <= MAX_ABS_DELAY_S

    events = visits.where(in_period & observed & valid & F.col("delay_s").isNotNull() & plausible)

    trips = read_static(spark, "trips.txt").select("feed_date", "trip_id", "route_id", "direction_id")
    routes = read_static(spark, "routes.txt").select(
        "feed_date", "route_id", "agency_id", "route_type",
        F.col("route_short_name").alias("line"), F.col("route_long_name").alias("line_name"))
    stops = read_static(spark, "stops.txt")
    platforms = stops.select("feed_date", "stop_id", F.coalesce("parent_station", "stop_id").alias("station_id"))
    stations = stops.select("feed_date", F.col("stop_id").alias("station_id"),
                            F.col("stop_name").alias("station_name"),
                            F.col("stop_lat").cast("double").alias("lat"),
                            F.col("stop_lon").cast("double").alias("lon"))
    mode = F.create_map(*[F.lit(x) for kv in MODES.items() for x in kv])

    local_ts = F.from_utc_timestamp(F.timestamp_seconds("event_time"), "Europe/Stockholm")
    joined = (events
              .withColumn("feed_date", F.col("service_date"))
              .join(trips, ["feed_date", "trip_id"], "left")
              .join(routes, ["feed_date", "route_id"], "left")
              .join(platforms, ["feed_date", "stop_id"], "left")
              .join(stations, ["feed_date", "station_id"], "left")
              .withColumn("mode", F.coalesce(mode[F.col("route_type")], F.lit("Other")))
              .withColumn("event_ts_local", local_ts)
              .withColumn("hour_local", F.hour(local_ts))
              # dayofweek: 1 = Sunday ... 7 = Saturday
              .withColumn("day_type", F.when(F.dayofweek("service_date").isin(1, 7), "weekend")
                          .otherwise("weekday"))
              .withColumn("is_delayed", F.col("delay_s") > DELAY_THRESHOLD_S)
              .cache())

    stop_events = joined.where(F.col("route_id").isNotNull() & F.col("station_id").isNotNull()).select(
        "service_date", "day_type", "trip_id", "direction_id", "route_id", "line", "line_name", "mode",
        "agency_id", "stop_sequence", "stop_id", "station_id", "station_name", "lat", "lon",
        "event_ts_local", "hour_local", "delay_s", "is_delayed", "n_reports")
    (stop_events.repartition("service_date")
     .write.mode("overwrite").partitionBy("service_date").parquet(path("silver", "stop_events")))

    # Row counts of every step, for the report and to catch silent data loss.
    period_visits = visits.where(in_period)
    qa = [
        ("bronze stop updates", updates.count()),
        ("stop visits (all service dates)", visits.count()),
        ("stop visits in period", period_visits.count()),
        ("  not observed (left the feed early)", period_visits.where(~observed).count()),
        ("  added/canceled trip or skipped stop", period_visits.where(observed & ~valid).count()),
        ("  no delay value", period_visits.where(observed & valid & F.col("delay_s").isNull()).count()),
        ("  |delay| > 2 h", period_visits.where(observed & valid & F.col("delay_s").isNotNull()
                                                & ~plausible).count()),
        ("observed events", joined.count()),
        ("  trip_id not in static GTFS", joined.where(F.col("route_id").isNull()).count()),
        ("  stop_id not in static GTFS", joined.where(F.col("station_id").isNull()).count()),
        ("silver stop events", spark.read.parquet(path("silver", "stop_events")).count()),
    ]
    qa_df = spark.createDataFrame(qa, "step string, n_rows long").coalesce(1)
    qa_df.write.mode("overwrite").parquet(path("silver", "qa"))
    qa_df.show(truncate=False)


if __name__ == "__main__":
    main()
