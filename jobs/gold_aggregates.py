"""Gold job: aggregate the silver stop events into the tables the dashboard shows.

    by_line       Q1  delay per line (mode, line)
    by_station    Q2  delay frequency per station, with coordinates for the map
    by_hour       Q3  delay per local hour of day
    by_day_type   Q4  weekday vs weekend
    by_date           delay per service date (trend / sanity check)
    histogram         delay distribution in 30 s bins
    qa                copy of silver/qa (pipeline row counts)

Every table has the same metrics (see metrics()) and a "mode" column. Except
by_line, the tables also have rows with mode = "All", and the tables split by
day_type also have day_type = "All", so the dashboard never has to combine
medians or percentiles itself.

Run:
    python jobs/gold_aggregates.py                                       # local Spark, data/
    docker compose -f docker/docker-compose.yml exec jupyter \\
        spark-submit jobs/gold_aggregates.py                             # cluster, HDFS
"""

from pyspark.sql import functions as F

from common import path, spark_session

HIST_BIN_S = 30
HIST_MIN_S, HIST_MAX_S = -5 * 60, 20 * 60


def metrics():
    delayed = F.col("is_delayed").cast("long")
    return [
        F.count("*").alias("n_events"),
        F.sum(delayed).alias("n_delayed"),
        F.round(100 * F.avg(delayed), 2).alias("pct_delayed"),
        F.round(F.avg("delay_s"), 1).alias("mean_delay_s"),
        F.percentile_approx("delay_s", 0.5).alias("median_delay_s"),
        F.percentile_approx("delay_s", 0.9).alias("p90_delay_s"),
    ]


def latest(column):
    """Value from the newest timetable version: names and coordinates change slightly between versions."""
    return F.max_by(column, "service_date").alias(column)


def with_all(df, column):
    """Duplicate every row with column = "All", so grouping by it also gives the total."""
    return df.unionByName(df.withColumn(column, F.lit("All")))


def write(df, name):
    df.coalesce(1).write.mode("overwrite").parquet(path("gold", name))
    print(f"gold/{name}: {df.count():,} rows")


def main():
    spark = spark_session("gold_aggregates")
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.parquet(path("silver", "stop_events")).cache()
    by_mode = with_all(events, "mode")
    by_mode_day = with_all(by_mode, "day_type")

    write(events.groupBy("mode", "line")
          .agg(latest("line_name"),
               F.countDistinct("service_date", "trip_id").alias("n_trips"),
               *metrics())
          .orderBy("mode", F.desc("mean_delay_s")), "by_line")

    write(by_mode.groupBy("mode", "station_id")
          .agg(latest("station_name"), latest("lat"), latest("lon"),
               *metrics()), "by_station")

    write(by_mode_day.groupBy("mode", "day_type", "hour_local").agg(*metrics())
          .orderBy("mode", "day_type", "hour_local"), "by_hour")

    write(by_mode_day.groupBy("mode", "day_type")
          .agg(F.countDistinct("service_date").alias("n_service_days"), *metrics())
          .withColumn("events_per_day", F.round(F.col("n_events") / F.col("n_service_days")))
          .orderBy("mode", "day_type"), "by_day_type")

    write(by_mode.groupBy("mode", "service_date", "day_type").agg(*metrics())
          .orderBy("mode", "service_date"), "by_date")

    clipped = F.least(F.greatest("delay_s", F.lit(HIST_MIN_S)), F.lit(HIST_MAX_S))
    write(by_mode_day.withColumn("bin_s", F.floor(clipped / HIST_BIN_S) * HIST_BIN_S)
          .groupBy("mode", "day_type", "bin_s").agg(F.count("*").alias("n_events"))
          .orderBy("mode", "day_type", "bin_s"), "histogram")

    write(spark.read.parquet(path("silver", "qa")), "qa")


if __name__ == "__main__":
    main()
