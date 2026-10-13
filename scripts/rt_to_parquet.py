"""Convert GTFS-RT TripUpdates archives (.7z of protobuf snapshots) to Parquet.

For each day archive in data/raw/koda/rt/TripUpdates it:
  1. unpacks the archive into a temporary folder (~5 GB per day),
  2. parses every snapshot (.pb) with gtfs-realtime-bindings, skipping files
     that repeat an earlier snapshot (the feed changes every ~20 s but was
     saved every ~14 s, so ~30% of the files are byte-identical copies),
  3. writes one row per stop_time_update to
     data/bronze/tripupdates/file_day=<date>/part-NNNN.parquet,
  4. deletes the temporary folder.

Every snapshot repeats the predictions for all upcoming stops, so this bronze
table is large and full of duplicates on purpose; the Spark silver job reduces
it to one event per stop visit.

Days whose output already exists are skipped, so the script can be re-run.

Usage:
    python scripts/rt_to_parquet.py                      # whole period, 4 days in parallel
    python scripts/rt_to_parquet.py --days 2026-09-15    # one day
    python scripts/rt_to_parquet.py --workers 2
"""

import argparse
import datetime as dt
import shutil
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.transit import gtfs_realtime_pb2

from decompresser import open_7z

ROOT = Path(__file__).resolve().parent.parent
TRIP_UPDATES_FOLDER = ROOT / "data" / "raw" / "koda" / "rt" / "TripUpdates"
OUT_FOLDER = ROOT / "data" / "bronze" / "tripupdates"
TMP_FOLDER = ROOT / "data" / "tmp"

# Archive and snapshot file names are Stockholm local time, even though the
# snapshot names end in "Z": sl-tripupdates-2026-09-15T00-00-03Z.pb has the feed
# header timestamp 2026-09-14 21:59:52 UTC. So the archive of day D covers local
# day D, and we use the header timestamp (real UTC), never the file name.
FIRST_DAY = dt.date(2026, 9, 13)
LAST_DAY = dt.date(2026, 9, 26)

ROWS_PER_FILE = 5_000_000

SCHEMA = pa.schema([
    ("snapshot_ts", pa.int64()),       # feed header timestamp, unix epoch (UTC)
    ("trip_id", pa.string()),
    ("start_date", pa.string()),       # service date, YYYYMMDD
    ("trip_sched_rel", pa.int8()),     # 0 SCHEDULED, 1 ADDED, 2 UNSCHEDULED, 3 CANCELED
    ("vehicle_id", pa.string()),
    ("stop_id", pa.string()),
    ("stop_sequence", pa.int32()),
    ("stop_sched_rel", pa.int8()),     # 0 SCHEDULED, 1 SKIPPED, 2 NO_DATA
    ("arr_delay", pa.int32()),         # seconds, null when absent
    ("arr_time", pa.int64()),          # unix epoch (UTC), null when absent
    ("arr_uncertainty", pa.int32()),
    ("dep_delay", pa.int32()),
    ("dep_time", pa.int64()),
])


def archive_day(archive):
    # sl-TripUpdates-2026-09-15.7z -> 2026-09-15
    return dt.date.fromisoformat(archive.stem[-10:])


class PartWriter:
    """Collects rows column by column and writes a Parquet file every ROWS_PER_FILE rows."""

    def __init__(self, folder):
        self.folder = folder
        self.parts = 0
        self.rows = 0
        self._reset()

    def _reset(self):
        self.cols = {name: [] for name in SCHEMA.names}

    def flush(self):
        n = len(self.cols["snapshot_ts"])
        if n == 0:
            return
        table = pa.table(self.cols, schema=SCHEMA)
        pq.write_table(table, self.folder / f"part-{self.parts:04d}.parquet", compression="zstd")
        self.parts += 1
        self.rows += n
        self._reset()


def event_fields(event):
    """(delay, time, uncertainty) of a StopTimeEvent, None for missing fields."""
    return (
        event.delay if event.HasField("delay") else None,
        event.time if event.HasField("time") else None,
        event.uncertainty if event.HasField("uncertainty") else None,
    )


NO_EVENT = (None, None, None)


def add_snapshot(path, writer, seen):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(path.read_bytes())
    ts = feed.header.timestamp
    if ts in seen:
        return
    seen.add(ts)
    c = writer.cols
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        trip_id, start_date, trip_rel = trip.trip_id, trip.start_date, trip.schedule_relationship
        vehicle_id = tu.vehicle.id if tu.HasField("vehicle") else None
        for stu in tu.stop_time_update:
            arr = event_fields(stu.arrival) if stu.HasField("arrival") else NO_EVENT
            dep = event_fields(stu.departure) if stu.HasField("departure") else NO_EVENT
            c["snapshot_ts"].append(ts)
            c["trip_id"].append(trip_id)
            c["start_date"].append(start_date)
            c["trip_sched_rel"].append(trip_rel)
            c["vehicle_id"].append(vehicle_id)
            c["stop_id"].append(stu.stop_id)
            c["stop_sequence"].append(stu.stop_sequence)
            c["stop_sched_rel"].append(stu.schedule_relationship)
            c["arr_delay"].append(arr[0])
            c["arr_time"].append(arr[1])
            c["arr_uncertainty"].append(arr[2])
            c["dep_delay"].append(dep[0])
            c["dep_time"].append(dep[1])
    if len(c["snapshot_ts"]) >= ROWS_PER_FILE:
        writer.flush()


def convert(archive):
    day = archive_day(archive)
    out_folder = OUT_FOLDER / f"file_day={day}"

    # only a non-empty folder means a previous run got through this archive.
    if out_folder.is_dir() and any(out_folder.iterdir()):
        return f"skipped: {out_folder.relative_to(ROOT)}"
    shutil.rmtree(out_folder, ignore_errors=True)

    started = time.time()
    tmp = TMP_FOLDER / archive.stem
    partial = out_folder.with_name(out_folder.name + ".part")
    shutil.rmtree(tmp, ignore_errors=True)
    shutil.rmtree(partial, ignore_errors=True)
    tmp.mkdir(parents=True)
    partial.mkdir(parents=True)

    try:
        with open_7z(archive) as opened:
            opened.extractall(tmp)
        unpacked = time.time() - started

        snapshots = sorted(tmp.rglob("*.pb"))
        writer = PartWriter(partial)
        seen = set()
        for path in snapshots:
            add_snapshot(path, writer, seen)
        writer.flush()
    except Exception as e:
        shutil.rmtree(partial, ignore_errors=True)
        return f"FAILED : {archive.name}: {type(e).__name__}: {e}"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    partial.rename(out_folder)
    size_mb = sum(f.stat().st_size for f in out_folder.iterdir()) / 1e6
    return (f"converted: {out_folder.relative_to(ROOT)}: {len(seen)} snapshots ({len(snapshots)} files), "
            f"{writer.rows:,} rows, {size_mb:.0f} MB "
            f"(unpack {unpacked:.0f}s, total {time.time() - started:.0f}s)")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", nargs="+", type=dt.date.fromisoformat,
                        help=f"archive days to convert (default: {FIRST_DAY} ... {LAST_DAY})")
    parser.add_argument("--workers", type=int, default=4,
                        help="days converted in parallel; each needs ~5 GB of temporary disk")
    args = parser.parse_args()

    archives = [a for a in sorted(TRIP_UPDATES_FOLDER.glob("*.7z"))
                if (archive_day(a) in args.days if args.days else FIRST_DAY <= archive_day(a) <= LAST_DAY)]
    if not archives:
        sys.exit("no archives to convert")

    with Pool(min(args.workers, len(archives))) as pool:
        results = list(pool.imap_unordered(convert, archives))
    for line in sorted(results):
        print(line)

    failed = [line for line in results if line.startswith("FAILED")]
    if failed:
        print(f"\n{len(failed)} archive(s) failed, re-run to retry.")
        sys.exit(1)
    print("\nAll archives converted.")


if __name__ == "__main__":
    main()
