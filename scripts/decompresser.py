'''
This module is responsable for decompressing the static GTFS archives inside
data/raw/koda/static. For each date of the analysis period it extracts the
timetable files the pipeline needs (csv formatted .txt files) into
data/bronze/static/feed_date=<date>/.

The real-time .7z archives are not decompressed here: a day is ~5 GB unpacked.
scripts/rt_to_parquet.py unpacks them one day at a time and converts them to
Parquet instead.
'''

import datetime as dt
import shutil
import sys
from pathlib import Path
from zipfile import ZipFile

import py7zr

ROOT = Path(__file__).resolve().parent.parent
STATIC_FOLDER = ROOT / "data" / "raw" / "koda" / "static"
OUT_FOLDER = ROOT / "data" / "bronze" / "static"

# Analysis period (service dates, Stockholm local time), see data/raw/koda/README.txt
PERIOD_START = dt.date(2026, 9, 13)
PERIOD_END = dt.date(2026, 9, 26)

# shapes.txt (~140 MB per version) and the other files are not used by the pipeline.
NEEDED_FILES = ["trips.txt", "routes.txt", "stops.txt", "stop_times.txt", "calendar_dates.txt"]


def open_zip(path):
    return ZipFile(path, "r")


def open_7z(path):
    return py7zr.SevenZipFile(path, "r")


def archive_date(archive):
    # GTFS-SL-2026-09-13.zip -> 2026-09-13
    return dt.date.fromisoformat(archive.stem[-10:])


def extract(archive, out_folder):
    # only a non-empty folder means a previous run got through this archive.
    if out_folder.is_dir() and any(out_folder.iterdir()):
        print(f"skipped: {out_folder.relative_to(ROOT)}")
        return True

    # an empty folder is the leftover of a run that failed; do the archive again.
    shutil.rmtree(out_folder, ignore_errors=True)

    partial = out_folder.with_name(out_folder.name + ".part")
    shutil.rmtree(partial, ignore_errors=True)
    partial.mkdir(parents=True)

    try:
        with open_zip(archive) as opened:
            opened.extractall(partial, members=NEEDED_FILES)
    except Exception as e:
        shutil.rmtree(partial, ignore_errors=True)
        print(f"FAILED : {archive.name}: {type(e).__name__}: {e}")
        return False

    partial.rename(out_folder)
    print(f"unzipped: {out_folder.relative_to(ROOT)}")
    return True


def main():
    failed = []
    for archive in sorted(STATIC_FOLDER.glob("*.zip")):
        day = archive_date(archive)
        if not PERIOD_START <= day <= PERIOD_END:
            continue
        if not extract(archive, OUT_FOLDER / f"feed_date={day}"):
            failed.append(archive.name)

    if failed:
        print(f"\n{len(failed)} archive(s) failed, re-run to retry:")
        for name in failed:
            print(f"  {name}")
        sys.exit(1)
    print("\nAll archives decompressed.")


if __name__ == "__main__":
    main()
