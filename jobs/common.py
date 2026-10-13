"""Settings and helpers shared by the Spark jobs.

The jobs read and write under one base location with the same layout everywhere:

    <SL_DATA>/bronze/tripupdates/file_day=<date>/   real-time stop updates (scripts/rt_to_parquet.py)
    <SL_DATA>/bronze/static/feed_date=<date>/       static GTFS .txt files (scripts/decompresser.py)
    <SL_DATA>/silver/stop_events/                   one row per observed stop visit
    <SL_DATA>/silver/qa/                            row counts and join rates
    <SL_DATA>/gold/<table>/                         aggregates for the dashboard

SL_DATA is hdfs://namenode:8020/sl inside the Docker cluster (set in
docker/docker-compose.yml). Without it the jobs use the local data/ folder and
a local Spark, which is handy for testing on one day.
"""

import datetime as dt
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent
DATA = os.environ.get("SL_DATA", (ROOT / "data").as_uri())

# Analysis period: service dates (Stockholm local time), 10 weekdays + 4 weekend days.
PERIOD_START = dt.date(2026, 9, 13)
PERIOD_END = dt.date(2026, 9, 26)

# An arrival counts as delayed when it is more than 3 minutes late.
DELAY_THRESHOLD_S = 180
# Delays beyond +-2 hours are feed errors, not real delays.
MAX_ABS_DELAY_S = 2 * 60 * 60

# GTFS route_type (extended types used by SL) -> mode shown in the results.
MODES = {"100": "Commuter rail", "401": "Metro", "700": "Bus", "900": "Tram", "1000": "Ferry"}


def path(*parts):
    return "/".join([DATA, *parts])


def spark_session(app_name):
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # all timestamps are handled in UTC; local time is derived explicitly
        .config("spark.sql.session.timeZone", "UTC")
    )
    if "SPARK_MASTER" in os.environ:
        # one executor per worker, using the worker's memory (docker/docker-compose.yml)
        builder = builder.config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
    else:
        # local Spark: run the Python workers with this interpreter, not whatever python3 is on PATH
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        builder = builder.config("spark.driver.memory", "8g")
    return builder.getOrCreate()
