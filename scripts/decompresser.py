'''
This module is responsable for decompressing the files inside the data archive.
The decompressed files will be of type txt in csv format in the data/extracted
folder.
'''

import shutil
import sys
from pathlib import Path
from zipfile import ZipFile

import py7zr

ROOT = Path(__file__).resolve().parent.parent
SERVICE_ALERTS_FOLDER = ROOT / "data" / "raw" / "koda" / "rt" / "ServiceAlerts"
TRIP_UPDATES_FOLDER = ROOT / "data" / "raw" / "koda" / "rt" / "TripUpdates"
STATIC_FOLDER = ROOT / "data" / "raw" / "koda" / "static"


def open_zip(path):
    return ZipFile(path, "r")


def open_7z(path):
    return py7zr.SevenZipFile(path, "r")


def extract(archive, open_archive):
    decompressed_folder_parent = Path(str(archive.parent).replace("raw", "decompressed"))
    decompressed_folder_parent.mkdir(exist_ok=True, parents=True)

    decompressed_folder_path = decompressed_folder_parent / archive.with_suffix("").name

    # only a non-empty folder means a previous run got through this archive.
    if decompressed_folder_path.is_dir() and any(decompressed_folder_path.iterdir()):
        print(f"skipped: {decompressed_folder_path}")
        return True

    # an empty folder is the leftover of a run that failed; do the archive again.
    shutil.rmtree(decompressed_folder_path, ignore_errors=True)

    partial = decompressed_folder_path.with_name(decompressed_folder_path.name + ".part")
    shutil.rmtree(partial, ignore_errors=True)
    partial.mkdir(parents=True)

    try:
        with open_archive(archive) as opened:
            opened.extractall(partial)
    except Exception as e:
        shutil.rmtree(partial, ignore_errors=True)
        print(f"FAILED : {archive.name}: {type(e).__name__}: {e}")
        return False

    partial.rename(decompressed_folder_path)
    print(f"unzipped: {decompressed_folder_path}")
    return True


def main():
    failed = []
    for folder, pattern, open_archive in (
        (STATIC_FOLDER, "*.zip", open_zip),
        (TRIP_UPDATES_FOLDER, "*.7z", open_7z),
        # (SERVICE_ALERTS_FOLDER, "*.7z", open_7z),
    ):
        for archive in sorted(folder.glob(pattern)):
            if not extract(archive, open_archive):
                failed.append(archive.name)

    if failed:
        print(f"\n{len(failed)} archive(s) failed, re-run to retry:")
        for name in failed:
            print(f"  {name}")
        sys.exit(1)
    print("\nAll archives decompressed.")


if __name__ == "__main__":
    main()
