# Draft 12/10: building the SL delay pipeline

Work log of 2026-10-12: how the data was checked, how the pipeline was built, what went wrong along the way and what the first results are. For how to run everything, see the [README](README.md#how-to-run-the-system).

## 1. Starting point

- Done before today: `scripts/download_koda.py` (KoDa download), `scripts/decompresser.py` (unpack everything into `data/decompressed/`), and the project proposal (`Group 67-Project Proposal.pdf`).
- The proposal commits to 4 stages, **ingestion → preprocessing → distributed analytics → visualization**, on HDFS + PySpark + Parquet + Plotly Dash + Docker. It sets out to answer:
  1. Which lines have the highest average delays?
  2. Which stations have the most frequent delays?
  3. During which hours are delays most severe?
  4. How do weekday and weekend patterns differ?
- Decisions taken today: run on a **Docker HDFS + Spark cluster** (not only local Spark), and use **Plotly Dash** for the visualization.

## 2. Checking the data

### What is in `data/`

| Folder | Contents | Verdict |
|---|---|---|
| `data/raw/koda/rt/TripUpdates/` | Real-time archives for 09-07, 09-08 and 09-12 … 09-27 | 09-13 … 09-26 OK; 09-27 corrupt (removed, copy kept in `corrupt/`); 09-07/08/12 outside the period |
| `data/raw/koda/static/` | Timetable for each date | OK for the period |
| `data/raw/koda/corrupt/` | Earlier broken downloads of 09-17 and 09-27 | Not used |
| `data/export/SL-KoDa-2026-09-13_to_2026-09-26.zip` | Clean bundle of the period: 15 real-time + 14 static files + README + SHA256SUMS (1.6 GB, stored without compression) | All 29 checksums OK; identical to `raw/` |
| `data/gtfs_static/` | Loose timetable files | **Wrong version** (2026-10-06, valid from 5 Oct); do not use |

### Checks that were run

- **Integrity:** `shasum -a 256 -c SHA256SUMS` passes on all 29 files. Every archive except 09-27 lists ~6,000 `.pb` snapshots covering the whole day. The largest gap between snapshots is 112 s.
- **Joins:** real-time IDs were checked against the timetable of the same date. 99.2% of `trip_id`s and 100% of `stop_id`s match, but only 94.4% of trip IDs match the October `data/gtfs_static/`.
- **Content:** every stop update has an arrival delay (seconds) and an arrival time (epoch). Modes are bus (`route_type` 700), metro (401), tram (900), commuter rail (100) and ferry (1000). Buses are ~87% of all observed arrivals.

### Two surprises, found by comparing file names with feed timestamps

1. **The file names are Stockholm local time, not UTC**, even though they end in `Z`. For example, `sl-tripupdates-2026-09-15T00-00-03Z.pb` has the feed header time 2026-09-14 21:59:52 UTC. The archive of day D therefore covers *local* day D, and the 09-12 archive (once thought necessary for 00:00–02:00 on 09-13) is not needed. The pipeline uses only the feed timestamps.
2. **About 30% of the snapshot files are duplicates.** The feed changes every ~20 s but was saved every ~14 s. Every file whose header timestamp equals the previous one is byte-identical (127 of 499 in the test hours).

### The key property of the real-time feed

- Each snapshot lists the **latest prediction** for every upcoming stop of every running trip. One stop visit is reported ~80 times (median), so the raw rows are not delays: averaging them would weight each arrival by how long it stayed in the feed, and would mix predictions with observations.
- Measured on 09-15, a stop stays in the feed for a median of **~10 minutes after the vehicle passed it**. So the **last report of each stop visit is the observed arrival**: in 98.7% of visits, that report's arrival time is at or before the time of the last snapshot.

## 3. What was built

### Bronze: local conversion (`scripts/`)

- **`scripts/rt_to_parquet.py` (new).** For each day archive it:
  1. unpacks into `data/tmp/` with `py7zr`;
  2. parses each `.pb` with `gtfs-realtime-bindings` and skips duplicate snapshots;
  3. writes one row per stop update to `data/bronze/tripupdates/file_day=<date>/part-NNNN.parquet` (zstd, 5M rows per file);
  4. deletes the temp folder.

  It runs 4 days in parallel and skips days that are already done.
  - Columns: `snapshot_ts, trip_id, start_date, trip_sched_rel, vehicle_id, stop_id, stop_sequence, stop_sched_rel, arr_delay, arr_time, arr_uncertainty, dep_delay, dep_time`.
  - Why convert before HDFS: ~84,000 small `.pb` files would cause HDFS's small-files problem, and Spark has no GTFS-RT reader.
- **`scripts/decompresser.py` (changed).** It now only extracts the 5 static files the pipeline needs (`trips`, `routes`, `stops`, `stop_times`, `calendar_dates`) for the 14 period dates into `data/bronze/static/feed_date=<date>/`. Unused imports (`pyunpack`, `libarchive`, `lzma`) were removed, and the docstring was fixed. Real-time data no longer goes through this script, since it would need ~5 GB per day unpacked.

### Cluster (`docker/`)

| Service | Image | Role |
|---|---|---|
| `namenode`, `datanode` | `sl-hdfs:3.4.1` (built from `docker/hadoop/`) | HDFS, replication 1, permissions off, data in Docker volumes |
| `spark-master` | `sl-spark:4.1.3` (`docker/Dockerfile.spark`) | Spark standalone master (UI :8080) |
| `spark-worker-1/2` | `sl-spark:4.1.3` | 4 cores, 2.5 GB each (fits Docker's 8 GB) |
| `jupyter` | `sl-spark:4.1.3` | Spark driver + Jupyter Lab (:8888), `SL_DATA=hdfs://namenode:8020/sl` |

`docker/hdfs.sh` has three commands: `load` (bronze → HDFS), `fetch-gold` (HDFS gold → `data/gold/`) and `ls`.

### Spark jobs (`jobs/`)

- **`jobs/common.py`:** the shared settings:
  - data root: `SL_DATA`, either HDFS or the local `data/` folder;
  - Spark session, with the session time zone set to UTC;
  - analysis period 2026-09-13 … 09-26;
  - delay threshold 180 s, outlier bound ±2 h;
  - `route_type` → mode names.
- **`jobs/silver_stop_events.py`:**
  1. Group by (`trip_id`, `start_date`, `stop_sequence`) and take `max_by(struct(...), snapshot_ts)`, i.e. the last report of each stop visit.
  2. Keep observed visits only (event time ≤ last snapshot that reported it). Drop added/cancelled trips, skipped stops and |delay| > 2 h.
  3. Join `trips`, `routes` and `stops` of the timetable whose `feed_date` equals the service date. Roll platforms up to stations with `parent_station`.
  4. Derive local time (`Europe/Stockholm`), the hour, and weekday/weekend from the service date.
  5. Write `silver/stop_events` partitioned by `service_date`, plus `silver/qa` with row counts for every step.
- **`jobs/gold_aggregates.py`:**
  - Tables: `by_line`, `by_station`, `by_hour`, `by_day_type`, `by_date`, `histogram` (30 s bins) and a copy of `qa`.
  - Metrics in every table: arrivals, number and % more than 3 min late, mean, median and p90 delay.
  - "All" rows for mode and day type are computed in Spark, so the dashboard never has to combine percentiles. Station names and coordinates come from the newest timetable version (`max_by`), so the output is deterministic.

### Dashboard and notebook

- **`dashboard/app.py`:**
  - Tabs: Lines (top 25), Stations (map sized by volume and coloured by % delayed, plus top-20 and busiest-20 tables), Hour of day, Weekday vs weekend (hourly curves, distribution, per-mode bars, summary table), Data quality.
  - Filters for mode, metric and minimum sample apply to every tab.
  - Each mode has a fixed colour, and the palette was checked with the colour-blindness validator.
- **`notebooks/exploration.ipynb`:** what a snapshot row looks like; the dedup shown on one real Green line metro trip at Alvik (171 reports for one stop visit, with the prediction going from 30 s early to an observed 60 s late); the QA counts; weekday vs weekend per mode. It was executed in the `jupyter` container.

### Other changes

- `requirements.txt` now pins `pyspark==4.1.3` to match the cluster, and adds `py7zr`, `pandas`, `dash` and `plotly`. All of them are installed in `.venv`.
- `README.md` has a step-by-step "How to run the system" section, a repository layout and a section of facts about the data.

## 4. Problems found and how they were solved

| Problem | Fix |
|---|---|
| `rt/TripUpdates/sl-TripUpdates-2026-09-27.7z` is corrupt (Header CRC error), so the decompress script would always fail | Deleted it after checking it is byte-identical to the copy in `corrupt/` |
| `data/gtfs_static/` is the October timetable | Not used; documented in the README |
| First bronze run gave 88M rows for one day, more than expected | Found the duplicate snapshot files and the local-time file names; the converter skips duplicates (61M rows, −30%) and starts at 09-13 |
| pip installed pyspark 4.2, but the newest `apache/spark` image is 4.1.3 | Pinned 4.1.3 everywhere |
| Local Spark failed with `PYTHON_VERSION_MISMATCH` (workers ran the system Python 3.14, the driver the venv's 3.13) | `common.py` sets `PYSPARK_PYTHON` to the driver's interpreter for local runs |
| Reading `feed_date=*/trips.txt` with a glob printed a noisy stack trace | Read the `static/` folder with `pathGlobFilter` instead |
| `apache/hadoop` image is amd64-only and **segfaulted under QEMU** on Apple Silicon | Own HDFS image: official Hadoop 3.4.1 tarball (sha512-checked) on `eclipse-temurin:11-jre`, native arm64/amd64 |
| Hadoop download from archive.apache.org was extremely slow | Download from dlcdn.apache.org first, with the archive as fallback (~90 s) |
| Jupyter crashed: the `spark` user's home is `/nonexistent` | `Dockerfile.spark` creates `/home/spark` and sets `HOME` |
| Docker Desktop has only 8 GB | Workers sized at 2 × 2.5 GB (executors 2 GB), Hadoop heaps at 512 MB |
| Station coordinates differed slightly between local and cluster gold (`first()` is not deterministic, and coordinates differ between timetable versions) | Use `max_by(column, service_date)`, i.e. the newest timetable |

## 5. Verification

- **Bronze:** 14 days, **777,382,896** stop updates, 5.6 GB of Parquet, plus 1.9 GB of static files. The conversion took ~20 minutes with 4 workers.
- **Silver QA (identical on local Spark and on the cluster):**

| Step | Rows |
|---|---|
| bronze stop updates | 777,382,896 |
| stop visits (all service dates) | 8,312,905 |
| stop visits in period | 8,270,405 |
| not observed (left the feed early) | 74,550 |
| added/canceled trip or skipped stop | 19,359 |
| \|delay\| > 2 h | 507 |
| observed events | 8,175,989 |
| trip_id not in static GTFS | 47 |
| stop_id not in static GTFS | 0 |
| **silver stop events** | **8,175,942** |

- **Local vs cluster:** gold from the cluster equals gold from local Spark in every column, except the approximate percentiles, which differ by ≤ 1 s.
- **Run times on the cluster** (2 workers × 4 cores, 2 GB executors, reading HDFS): silver 3 min 44 s, gold ~1 min. Local Spark (10 cores) took silver ~4 min and gold ~1 min 12 s.
- **Dashboard:** every tab renders for every mode × metric. The server answers HTTP 200, and a stations callback returned 279 KB. It has **not** been checked by eye in a browser yet.
- **Notebook:** it executes without errors.

## 6. First results (service dates 2026-09-13 … 09-26)

"Delayed" = more than 3 min late.

| Mode | Arrivals | % delayed | Mean delay | Median |
|---|---|---|---|---|
| All | 8,175,942 | 20.3% | 85 s | 59 s |
| Bus | 7,150,678 | 22.7% | 96 s | 73 s |
| Ferry | 39,431 | 21.8% | 43 s | 34 s |
| Commuter rail | 138,450 | 6.9% | 15 s | −26 s |
| Tram | 300,157 | 3.3% | 16 s | 15 s |
| Metro | 547,226 | 1.9% | −11 s | −12 s |

- **Q1 lines:** the most delayed are bus lines. Among lines with ≥ 2,000 arrivals: 791 (84% delayed, mean 543 s), 893 (76%), 848 (66%), then 492, 191, 795 and 67. The metro is almost always on time; the Green line (17/18/19) is the least punctual metro line at ~3%.
- **Q2 stations:** stations with ≥ 2,000 arrivals and the highest share delayed are bus stops along those lines, e.g. Tungelsta skola (77%) and Mulstavägen (67%).
- **Q3 hours:** delays rise through the day to a peak at **16:00 (27.1% delayed)**, then fall. The morning peak is milder (08:00, 21.3%). Night hours 01–04 also score high, but with few arrivals (night buses).
- **Q4 weekday vs weekend:** weekends are *worse* overall (23.0% vs 19.6% delayed, median 69 s vs 57 s), driven by buses (26.6% vs 21.8%). Metro (0.8% vs 2.3%) and commuter rail (3.3% vs 7.9%) are better on weekends. Saturdays 09-19 and 09-26 are the worst days (25–26%).

## 7. Known limitations

- Trips of service date 09-26 that run past midnight are missing, because the 09-27 archive is corrupt. 09-26 has ~6% fewer arrivals than 09-19.
- Cancelled trips that carry no stop updates are not in bronze, so cancellations are excluded rather than counted.
- ~0.9% of stop visits never get an observed report (the trip left the feed early), so they are excluded.
- The 3-minute threshold is a choice; it is one constant in `jobs/common.py`.
- Each "station" is a GTFS parent station. For buses this is a stop, which is why the station ranking is dominated by bus stops; the dashboard's mode filter and minimum-sample slider help.

## 8. Next steps

1. Check the dashboard in a browser and polish the charts for the report.
2. Decide the report's analyses: e.g. a significance test for weekday vs weekend (per-day values), a per-line view by hour, and delay growth along a route (by `stop_sequence`).
3. For the report's "distributed" part, document the Spark UI (stages, shuffle sizes, partitions) and run times. Optionally compare 1 vs 2 workers, or more memory if Docker gets 12 GB or more.
4. Split the work: data/pipeline (dedup, time zones, QA), results and figures, dashboard and limitations.
