"""Convert the decompressed SL data to Parquet and put it into HDFS.

Two datasets are written:

    /sl/gtfs/<table>/date=2026-09-07/<table>.parquet              the timetable
    /sl/tripupdates/date=2026-09-07/hour=16/tripupdates.parquet   the delays

Trip updates contains the delays every 14 seconds. We decided to only keep the last
entry as the ground truth of what happened to each train

Usage:
    python scripts/hdfs-ingression.py                 # everything already in data/decompressed
    python scripts/hdfs-ingression.py --day-by-day    # straight from data/raw, one date at a time
"""

import argparse
import shutil
import subprocess
from multiprocessing import Pool
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from google.transit import gtfs_realtime_pb2

import decompresser

ROOT = Path(__file__).resolve().parent.parent
STATIC_FOLDER = ROOT / "data" / "decompressed" / "koda" / "static"
TRIP_UPDATES_FOLDER = ROOT / "data" / "decompressed" / "koda" / "rt" / "TripUpdates"
STAGING = ROOT / "data" / "parquet"

WORKERS = 8


GTFS_TABLES = ["stops", "routes", "trips", "stop_times", "calendar"]

ID_AS_TEXT = pacsv.ConvertOptions(column_types={
    name: pa.string() for name in
    ["trip_id", "stop_id", "route_id", "service_id", "shape_id", "parent_station", "agency_id"]
})

# One row per trip and stop
TRIP_UPDATE_SCHEMA = pa.schema([
    ("trip_id", pa.string()),
    ("stop_id", pa.string()),
    ("stop_sequence", pa.int32()),
    ("arrival_delay", pa.int32()),
    ("departure_delay", pa.int32()),
    ("arrival_time", pa.int64()),
    ("departure_time", pa.int64()),
    ("trip_start_date", pa.string()),
    ("snapshot_time", pa.int64()),
])


def hdfs(*arguments):
    """Process that starts the HDFS"""
    subprocess.run(["hdfs", "dfs", *arguments], check=True)


def write(rows, folder, name):
    folder.mkdir(parents=True, exist_ok=True)
    pq.write_table(rows, folder / name)


def upload(local_folder, hdfs_folder):
    """Put one date folder into HDFS, then drop the local copy.
    """
    hdfs("-mkdir", "-p", hdfs_folder)
    hdfs("-put", "-f", str(local_folder), hdfs_folder)
    shutil.rmtree(local_folder)


def convert_static(folder):
    """
    One date to one parquet (one date has half a GB of data
    """
    date = folder.name.replace("GTFS-SL-", "")
    for table in GTFS_TABLES:
        rows = pacsv.read_csv(folder / f"{table}.txt", convert_options=ID_AS_TEXT)
        local = STAGING / "gtfs" / table / f"date={date}"
        write(rows, local, f"{table}.parquet")
        upload(local, f"/sl/gtfs/{table}")
    print(f"static   {date}")


def event(stop_time_event):
    """
    The delay and the time of an arrival or a departure.
    """
    delay = stop_time_event.delay if stop_time_event.HasField("delay") else None
    time = stop_time_event.time if stop_time_event.HasField("time") else None
    return delay, time


def convert_hour(folder):
    """
    Every snapshot of one hour -> one Parquet file
    """
    year, month, day, hour = folder.parts[-4:]
    latest = {}

    for snapshot in sorted(folder.glob("*.pb")):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(snapshot.read_bytes())
        for entity in feed.entity:
            update = entity.trip_update
            for stop in update.stop_time_update:
                arrival_delay, arrival_time = event(stop.arrival)
                departure_delay, departure_time = event(stop.departure)
                # a later snapshot of the same stop replaces the earlier one.
                # trip ids are reused on other days, so the start date is part of the key
                latest[(update.trip.trip_id, update.trip.start_date, stop.stop_sequence)] = (
                    update.trip.trip_id,
                    stop.stop_id,
                    stop.stop_sequence,
                    arrival_delay,
                    departure_delay,
                    arrival_time,
                    departure_time,
                    update.trip.start_date,
                    update.timestamp,
                )

    if not latest:
        return f"{year}-{month}-{day} {hour}h: no trips"

    columns = dict(zip(TRIP_UPDATE_SCHEMA.names, zip(*latest.values())))
    rows = pa.table(columns, schema=TRIP_UPDATE_SCHEMA)
    write(rows, STAGING / "tripupdates" / f"date={year}-{month}-{day}" / f"hour={hour}",
          "tripupdates.parquet")
    return f"{year}-{month}-{day} {hour}h: {len(latest)} rows"


def convert_trip_updates(day):
    """One day of snapshots -> one Parquet file per hour, uploaded together."""
    # get the date and hours for the trip updates folders
    date = day.name.replace("sl-TripUpdates-", "")
    hours = sorted(hour for hour in day.glob("sl/TripUpdates/*/*/*/*") if hour.is_dir())
    with Pool(WORKERS) as pool:
        for message in pool.imap(convert_hour, hours):
            print(message)
    upload(STAGING / "tripupdates" / f"date={date}", "/sl/tripupdates")
    print(f"uploaded {date}")


def day_by_day():
    """
    Decompress, ingest and delete one date at a time. A day of trip updates is
    about 4.5 GB unpacked, so the whole month at once does not fit on a laptop.
    Returns the archives that could not be decompressed.
    """
    jobs = [
        (decompresser.STATIC_FOLDER, "GTFS-SL-", ".zip", decompresser.open_zip, convert_static),
        (decompresser.TRIP_UPDATES_FOLDER, "sl-TripUpdates-", ".7z", decompresser.open_7z, convert_trip_updates),
    ]
    dates = sorted({archive.name.removeprefix(prefix).removesuffix(suffix)
                    for folder, prefix, suffix, _, _ in jobs
                    for archive in folder.glob(f"{prefix}*{suffix}")})
    failed = []
    for date in dates:
        for folder, prefix, suffix, open_archive, convert in jobs:
            archive = folder / f"{prefix}{date}{suffix}"
            if not archive.exists():
                continue
            if not decompresser.extract(archive, open_archive):
                failed.append(archive.name)
                continue
            unpacked = Path(str(folder).replace("raw", "decompressed")) / f"{prefix}{date}"
            convert(unpacked)
            shutil.rmtree(unpacked)
    return failed


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--day-by-day", action="store_true",
                        help="decompress, ingest and delete one date at a time, straight from data/raw")
    args = parser.parse_args()

    failed = []
    if args.day_by_day:
        failed = day_by_day()
    else:
        # get all the data from the static folder and put in parquet
        for folder in sorted(STATIC_FOLDER.iterdir()):
            convert_static(folder)
        for day in sorted(TRIP_UPDATES_FOLDER.iterdir()):
            convert_trip_updates(day)

    shutil.rmtree(STAGING, ignore_errors=True)  # only empty folders are left by now

    print("\nIn HDFS:")
    hdfs("-du", "-h", "/sl")

    if failed:
        print(f"\n{len(failed)} archive(s) could not be decompressed, re-run to retry:")
        for name in failed:
            print(f"  {name}")


if __name__ == "__main__":
    main()
