# Stockholm Public Transport Delay Analytics Using Apache Spark

Group 67: Hieu Vu, Rong Jin, Serban

## Goals

We want to answer questions such as:

- Which transportation lines have the highest average delays?
- Which stations experience the most frequent delays?
- During which hours are delays most severe?
- How do delay patterns differ between weekdays and weekends?

## Tools

A distributed data processing pipeline:

- Storage: HDFS (Hadoop 3.4.1, in Docker)
- Processing: Apache Spark 4.1.3 (PySpark), standalone cluster in Docker
- Data format: GTFS / GTFS-Realtime converted to Parquet
- Visualization: Plotly Dash
- Development: Docker, Jupyter Notebook, Git, Python

## Methodology

The pipeline has 4 stages:

- **Data ingestion:** download SL's real-time and static GTFS data from Trafiklab KoDa, convert it to Parquet and upload it into HDFS.
- **Data preprocessing (Spark, silver):** reduce the real-time snapshots to one observed arrival per stop visit, clean them, and join routes, trips and stops of the matching timetable version.
- **Distributed analytics (Spark, gold):** delay per line and station, by hour of day, weekday vs weekend, busiest stations, delay distribution.
- **Visualization (Dash):** charts of delay distributions and hourly trends, and a map of stations with the largest delays.

```
data/raw/koda/*.7z, *.zip
  │  scripts/rt_to_parquet.py, scripts/decompresser.py      (local Python)
  ▼
data/bronze/  ──docker/hdfs.sh load──▶  hdfs:///sl/bronze
                                          │  jobs/silver_stop_events.py   (Spark)
                                          ▼
                                        hdfs:///sl/silver/stop_events
                                          │  jobs/gold_aggregates.py      (Spark)
                                          ▼
data/gold/  ◀──docker/hdfs.sh fetch-gold──  hdfs:///sl/gold
  │  dashboard/app.py                                        (Plotly Dash)
```

## Repository layout

| Path | What it is |
|---|---|
| `scripts/download_koda.py` | Downloads the real-time (`.7z`) and static (`.zip`) data from KoDa |
| `scripts/decompresser.py` | Extracts the static timetable files needed by the pipeline into `data/bronze/static/` |
| `scripts/rt_to_parquet.py` | Converts the real-time snapshots to Parquet in `data/bronze/tripupdates/` |
| `docker/` | HDFS + Spark cluster (`docker-compose.yml`, `hadoop/`, `Dockerfile.spark`) and `hdfs.sh` to move data in and out of HDFS |
| `jobs/common.py` | Shared settings: data paths, analysis period, 3-minute delay threshold, mode names |
| `jobs/silver_stop_events.py` | Spark job: snapshots → one row per observed stop visit, joined with the timetable |
| `jobs/gold_aggregates.py` | Spark job: aggregates for the four questions |
| `dashboard/app.py` | Plotly Dash app on the gold tables |
| `notebooks/exploration.ipynb` | Data exploration, the deduplication on a real trip, pipeline row counts |
| `draft1210.md` | Work log of how the pipeline was built and what was found (2026-10-12) |

---

## How to run the system

### Prerequisites

- Python 3.11+ (tested with 3.13)
- Java 17 (only for running Spark locally, step 4b)
- Docker Desktop with **at least 8 GB of memory** (Settings → Resources)
- ~25 GB free disk: raw data 1.6 GB, bronze 7.5 GB, a copy in HDFS 7.5 GB, plus ~5 GB per day being converted

### 1. Python environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Get the raw data

Either unzip the bundle a teammate shared (fastest):

```bash
mkdir -p data/raw && unzip data/export/SL-KoDa-2026-09-13_to_2026-09-26.zip -d data/raw/
# check it: (cd data/raw/koda && shasum -a 256 -c SHA256SUMS)
```

or download it yourself, see [Downloading the dataset](#downloading-the-dataset) below:

```bash
python scripts/download_koda.py --start 2026-09-13 --end 2026-09-26
```

You should end up with `data/raw/koda/rt/TripUpdates/sl-TripUpdates-<date>.7z` and `data/raw/koda/static/GTFS-SL-<date>.zip` for 2026-09-13 … 2026-09-26.

### 3. Convert to Parquet (bronze)

```bash
python scripts/decompresser.py     # static GTFS -> data/bronze/static/feed_date=<date>/*.txt     (~1 s)
python scripts/rt_to_parquet.py    # TripUpdates -> data/bronze/tripupdates/file_day=<date>/*.parquet (~20 min)
```

`rt_to_parquet.py` handles 4 days in parallel (`--workers N`); each needs ~5 GB of temporary disk in `data/tmp/`, deleted afterwards. Use `--days 2026-09-15` to convert a single day. Both scripts skip what already exists, so they can be re-run after a failure.

### 4a. Run on the Docker cluster (HDFS + Spark)

```bash
# start HDFS (namenode, datanode) and Spark (master, 2 workers, Jupyter driver)
docker compose -f docker/docker-compose.yml up -d --build

# copy bronze into HDFS (~2 min)
docker/hdfs.sh load

# Spark jobs (~4 min and ~1 min)
docker compose -f docker/docker-compose.yml exec jupyter spark-submit jobs/silver_stop_events.py
docker compose -f docker/docker-compose.yml exec jupyter spark-submit jobs/gold_aggregates.py

# copy the (small) gold tables back to data/gold/ for the dashboard
docker/hdfs.sh fetch-gold
```

The first `up --build` builds two images: `sl-hdfs` (downloads Hadoop 3.4.1, ~500 MB) and `sl-spark`. Wait until HDFS is ready before `hdfs.sh load`:
`docker compose -f docker/docker-compose.yml exec namenode hdfs dfsadmin -report | grep "Live datanodes"` should say `(1)`.

Web UIs while the cluster runs:

| UI | URL |
|---|---|
| HDFS namenode | http://localhost:9870 |
| Spark master (workers, applications) | http://localhost:8080 |
| Spark job (only while a job runs) | http://localhost:4040 |
| Jupyter Lab (Spark driver) | http://localhost:8888 |

`docker/hdfs.sh ls` lists what is in HDFS. Stop the cluster with `docker compose -f docker/docker-compose.yml down`; the HDFS data stays in Docker volumes (add `-v` to delete it).

### 4b. Or run the same jobs on a local Spark (no Docker)

Useful for testing. Without the `SL_DATA` / `SPARK_MASTER` environment variables the jobs read and write the local `data/` folder:

```bash
python jobs/silver_stop_events.py    # -> data/silver/  (~4 min)
python jobs/gold_aggregates.py       # -> data/gold/    (~1 min)
```

### 5. Dashboard

```bash
python dashboard/app.py              # then open http://127.0.0.1:8050
```

Tabs: Lines (Q1), Stations with map (Q2), Hour of day (Q3), Weekday vs weekend (Q4), Data quality. The mode, metric and minimum-sample filters at the top apply to every tab.

### 6. Notebook

`notebooks/exploration.ipynb` reads the local `data/` folder (bronze, silver, gold). Open it in Jupyter Lab at http://localhost:8888 or run it headless:

```bash
docker compose -f docker/docker-compose.yml exec jupyter jupyter nbconvert --to notebook --execute --inplace notebooks/exploration.ipynb
```

### Troubleshooting

- **Spark workers get no executors / jobs are killed:** Docker has too little memory. The cluster is sized for 8 GB (2 workers × 2.5 GB). With more memory, raise `SPARK_WORKER_MEMORY` in `docker/docker-compose.yml` and set `SPARK_EXECUTOR_MEMORY` (default `2g`) for the `jupyter` service.
- **`PYTHON_VERSION_MISMATCH` when running locally:** `jobs/common.py` sets `PYSPARK_PYTHON` to the current interpreter; run the jobs with the venv's `python`.
- **HDFS stays in safe mode / no live datanode:** wait 10–20 s after `up`, or check `docker compose -f docker/docker-compose.yml logs namenode datanode`.
- **The `pyspark` version must match the cluster** (4.1.3, pinned in `requirements.txt` and `docker/Dockerfile.spark`).

---

## Data

### Downloading the dataset

Historical SL data is downloaded from [Trafiklab KoDa](https://www.trafiklab.se/api/trafiklab-apis/koda/) with `scripts/download_koda.py`.

#### Why two types of data?

We download two kinds of data, and the analysis needs both:

| | Static GTFS (`.zip`) | GTFS-RT TripUpdates (`.7z`) |
|---|---|---|
| What it is | The **planned** timetable | What **actually happened** during the day |
| Contents | `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt` | Arrival/departure times and delays for each trip at each stop |
| Identifies things by | IDs **and** readable info (line numbers, stop names, coordinates) | IDs only (`trip_id`, `stop_id`) |

- **Real-time data gives us the delays.** It records when vehicles actually arrived and departed, so it shows how late each trip was. The timetable alone can't tell us that.
- **Static data tells us what the delays belong to.** Real-time records only contain IDs such as `trip_id` and `stop_id`. To answer questions like "which line is most delayed" or "which station has the most delays", we join them with `trips.txt`, `routes.txt` and `stops.txt` to get line numbers, station names and coordinates (for the map).
- **The static data must match the date.** SL changes its timetable over time, and trip IDs can change between versions. So the script downloads the static GTFS that was valid on each date. That way the real-time IDs for that day match the timetable they refer to.

#### 1. Get an API key

1. Create a free account on [Trafiklab](https://www.trafiklab.se/).
2. Create a project and add the **KoDa** API to it.
3. Copy the API key from the project page.

```bash
cp .env.example .env
```

Open `.env` and set `KODA_KEY` to your KoDa key. `.env` is in `.gitignore`, so the key is not committed.

#### 2. Run the download script

```bash
# TripUpdates + static GTFS for every day in the range (end date inclusive)
python scripts/download_koda.py --start 2026-09-13 --end 2026-09-26

# Choose which real-time feeds to download
python scripts/download_koda.py --start 2026-09-13 --end 2026-09-13 --feeds TripUpdates ServiceAlerts

# Real-time feeds only, skip static GTFS
python scripts/download_koda.py --start 2026-09-13 --end 2026-09-13 --no-static
```

Options:

- `--start`, `--end`: date range in `YYYY-MM-DD` format.
- `--feeds`: one or more of `TripUpdates` (default), `VehiclePositions`, `ServiceAlerts`.
- `--no-static`: skip the static GTFS download.

For each date, the script saves:

```
data/raw/koda/
├── rt/<feed>/sl-<feed>-<date>.7z    # GTFS-RT feeds
└── static/GTFS-SL-<date>.zip        # static GTFS valid on that date
```

Notes:

- KoDa builds archives on request, so the first request for a date can take several minutes. The script waits and retries.
- Files that already exist are skipped. If some downloads fail, run the same command again to retry them.
- `data/` is in `.gitignore` because the files are too large for git.

### Facts about the data that matter for the analysis

- **Analysis period:** Sunday 2026-09-13 to Saturday 2026-09-26 (service dates, Stockholm time): 10 weekdays and 4 weekend days. The files are bundled in `data/export/SL-KoDa-2026-09-13_to_2026-09-26.zip`.
- **Time zones:** the archive and snapshot file names are **Stockholm local time**, even though the snapshot names end in `Z`: `sl-tripupdates-2026-09-15T00-00-03Z.pb` carries the feed timestamp 2026-09-14 21:59:52 UTC. So the archive of day D covers local day D. The pipeline only uses the feed timestamps (real UTC) and converts to `Europe/Stockholm` explicitly.
- **Snapshots are predictions:** each snapshot (one every ~20 s) repeats the latest prediction for every upcoming stop, and keeps a stop ~10 minutes after the vehicle passed it. Averaging raw rows would be wrong; the silver job keeps the last report of each stop visit, which is the observed arrival.
- **Duplicate files:** the feed changes every ~20 s but was saved every ~14 s, so ~30% of the snapshot files are byte-identical copies; `rt_to_parquet.py` skips them.
- **2026-09-27 is corrupt** on the KoDa server (`Header CRC error`; a copy is kept in `data/raw/koda/corrupt/`), so the period ends on 09-26. Trips of service date 09-26 that run past midnight are missing.
- **Do not use `data/gtfs_static/`:** it is the 2026-10-06 timetable, valid from 5 October. Only 94% of the September trip IDs exist in it, against 99%+ for the static file of the same date.
- **Delayed** means arriving more than 3 minutes (180 s) late (`DELAY_THRESHOLD_S` in `jobs/common.py`). Delays beyond ±2 hours are treated as feed errors and dropped.
