"""Download SL data from Trafiklab KoDa for a date range.

For each date it downloads:
  - GTFS-RT feeds (default: TripUpdates) as .7z archives -> data/raw/koda/rt/<feed>/
  - the static GTFS valid on that date as .zip          -> data/raw/koda/static/

Files that already exist are skipped, so the script can be re-run after a failure.

Usage:
    python scripts/download_koda.py --start 2026-09-07 --end 2026-10-04
    python scripts/download_koda.py --start 2026-09-07 --end 2026-09-07 --feeds TripUpdates ServiceAlerts
"""

import argparse
import datetime as dt
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = "https://api.koda.trafiklab.se/KoDa/api/v2"
OPERATOR = "sl"
ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "raw" / "koda"

# KoDa prepares archives on demand, so a request can take minutes.
REQUEST_TIMEOUT = 300
MAX_ATTEMPTS = 5
RETRY_WAIT = 60

# Magic bytes, used to make sure we saved an archive and not an error message.
SIGNATURES = {".7z": b"7z\xbc\xaf\x27\x1c", ".zip": b"PK\x03\x04"}


def load_env(path):
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def daterange(start, end):
    day = start
    while day <= end:
        yield day
        day += dt.timedelta(days=1)


def download(url, dest):
    """Download url to dest, retrying while KoDa is still preparing the file."""
    if dest.exists():
        print(f"  skip   {dest.relative_to(ROOT)} (exists)")
        return True

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    for attempt in range(1, MAX_ATTEMPTS + 1):
        started = time.time()
        try:
            with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status == 202:
                    # Archive is being prepared; ask again later.
                    print(f"  wait   {dest.name}: KoDa is preparing the file (attempt {attempt})")
                    time.sleep(RETRY_WAIT)
                    continue
                expected = resp.headers.get("Content-Length")
                with open(tmp, "wb") as f:
                    while chunk := resp.read(1 << 20):
                        f.write(chunk)
        except urllib.error.HTTPError as e:
            body = e.read(300).decode(errors="replace")
            print(f"  error  {dest.name}: HTTP {e.code} {body}")
            if e.code in (400, 401, 403, 404):
                return False  # bad key or no data for this date; retrying won't help
            time.sleep(RETRY_WAIT)
            continue
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            print(f"  error  {dest.name}: {e} (attempt {attempt})")
            time.sleep(RETRY_WAIT)
            continue

        # A dropped connection can end the response early without raising an error.
        if expected is not None and tmp.stat().st_size != int(expected):
            print(f"  error  {dest.name}: incomplete download "
                  f"({tmp.stat().st_size} of {expected} bytes, attempt {attempt})")
            tmp.unlink()
            time.sleep(RETRY_WAIT)
            continue

        with open(tmp, "rb") as f:
            head = f.read(8)
        if not head.startswith(SIGNATURES[dest.suffix]):
            print(f"  error  {dest.name}: response is not a {dest.suffix} archive: {head!r}")
            tmp.unlink()
            time.sleep(RETRY_WAIT)
            continue

        tmp.rename(dest)
        size_mb = dest.stat().st_size / 1e6
        print(f"  ok     {dest.relative_to(ROOT)} ({size_mb:.1f} MB, {time.time() - started:.0f}s)")
        return True

    print(f"  FAILED {dest.name} after {MAX_ATTEMPTS} attempts")
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", required=True, type=dt.date.fromisoformat, help="first date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=dt.date.fromisoformat, help="last date (inclusive), YYYY-MM-DD")
    parser.add_argument("--feeds", nargs="+", default=["TripUpdates"],
                        choices=["TripUpdates", "VehiclePositions", "ServiceAlerts"])
    parser.add_argument("--no-static", action="store_true", help="skip the static GTFS download")
    args = parser.parse_args()

    load_env(ROOT / ".env")
    key = os.environ.get("KODA_KEY")
    if not key:
        sys.exit("KODA_KEY is not set (put it in .env, see .env.example)")

    failed = []
    for day in daterange(args.start, args.end):
        print(f"{day} ({day:%a})")
        for feed in args.feeds:
            url = f"{BASE_URL}/gtfs-rt/{OPERATOR}/{feed}?date={day}&key={key}"
            dest = OUT_DIR / "rt" / feed / f"{OPERATOR}-{feed}-{day}.7z"
            if not download(url, dest):
                failed.append(dest.name)
        if not args.no_static:
            url = f"{BASE_URL}/gtfs-static/{OPERATOR}?date={day}&key={key}"
            dest = OUT_DIR / "static" / f"GTFS-{OPERATOR.upper()}-{day}.zip"
            if not download(url, dest):
                failed.append(dest.name)

    if failed:
        print(f"\n{len(failed)} file(s) failed, re-run the same command to retry:")
        for name in failed:
            print(f"  {name}")
        sys.exit(1)
    print("\nAll files downloaded.")


if __name__ == "__main__":
    main()
