# Stockholm Public Transport Delay Analytics Using Apache Spark

## Goals:

We want to answer questions such as:

- Which transportation lines have the highest average delays?
- Which stations experience the most frequent delays?
- During which hours are delays most severs?
- How do delay patterns differ between weekdays and weekends?

## Tools:

We aim to use a distributed data processing pipeline:

- Storage: HDFS
- Processing: Apache Spark (PySpark)
- Data Format: CSV and GTFS file converted to Parquet
- Visualization: Plotly Dash (or Power BI)
- Development: Docker, Jupyter Notebook, Git, Python

## Data:

We use Stockholm's public transport open dataset provided by SL. We will create a free API key from Trafiklab (create a free account on Trafiklab, create a project, enable the GTFS Regional dataset) and download the dataset using the key

The dataset include GTFS files containing: stops.txt, routes.txt, trips.txt, stop_times.txt, calendar.txt

## Methodology and Algorithm

One pipeline contains 4 pages:

- Data ingestion: Download GTFS dataset, upload files into HDFS
- Data preprocessing: Load data into Spark DF, clean missing values, job routes, trips, stops, and stop times.
- Distributed Analytics: calculate average delays per route and station, aggregate delays by hour of day, compare weekday versus weekend patterns, rank the busiest stations.
- Visualization: Create charts showing delay distributions, display hours trends, presend a map highlighting stations with the largest delay

## Downloading the Dataset

Historical SL data is downloaded from [Trafiklab KoDa](https://www.trafiklab.se/api/trafiklab-apis/koda/) with `scripts/download_koda.py`.

### Why two types of data?

We download two kinds of data, and the analysis needs both:

| | Static GTFS (`.zip`) | GTFS-RT TripUpdates (`.7z`) |
|---|---|---|
| What it is | The **planned** timetable | What **actually happened** during the day |
| Contents | `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `calendar.txt` | Arrival/departure times and delays for each trip at each stop |
| Identifies things by | IDs **and** readable info (line numbers, stop names, coordinates) | IDs only (`trip_id`, `stop_id`) |

- **Real-time data gives us the delays.** It records when vehicles actually arrived and departed, so it shows how late each trip was. The timetable alone can't tell us that.
- **Static data tells us what the delays belong to.** Real-time records only contain IDs such as `trip_id` and `stop_id`. To answer questions like "which line is most delayed" or "which station has the most delays", we join them with `trips.txt`, `routes.txt` and `stops.txt` to get line numbers, station names and coordinates (for the map). `stop_times.txt` also gives the scheduled time for computing a delay when a real-time record doesn't include one.
- **The static data must match the date.** SL changes its timetable over time, and trip IDs can change between versions. So the script downloads the static GTFS that was valid on each date. That way the real-time IDs for that day match the timetable they refer to.

### 1. Get an API key

1. Create a free account on [Trafiklab](https://www.trafiklab.se/).
2. Create a project and add the **KoDa** API to it.
3. Copy the API key from the project page.

### 2. Set up the environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
```

Open `.env` and set `KODA_KEY` to your KoDa key. `.env` is in `.gitignore`, so the key is not committed.

### 3. Run the download script

```bash
# TripUpdates + static GTFS for every day in the range (end date inclusive)
python scripts/download_koda.py --start 2026-09-07 --end 2026-10-04

# Choose which real-time feeds to download
python scripts/download_koda.py --start 2026-09-07 --end 2026-09-07 --feeds TripUpdates ServiceAlerts

# Real-time feeds only, skip static GTFS
python scripts/download_koda.py --start 2026-09-07 --end 2026-09-07 --no-static
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
- The real-time archives are `.7z` files. To extract them, install 7-Zip (for example `brew install sevenzip` on macOS).
- `data/` is in `.gitignore` because the files are too large for git.
