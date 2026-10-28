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

### 4. Run the decompression script

```bash
python scripts/decompresser.py
```
This will create a new tree of data inside the *data* folder. It has the same structure as *step 3*, but instead
of *raw*, it is *decompressed*.

### 5. Run the HDFS ingression script

```bash
 python scripts/hdfs-ingression.py 
```
This script is fetching the relevant files from inside the static folder (and the date folders) and putting them inside hdfs. Afterwards, it is inputting the trip updates partitioned after hour due to the large size. Only the last entry is being inputed, as that denotes the final delay that a train had, not the real time one.

### 6. Run the Spark data Reading

```bash
python scripts/spark-preprocessing.py
```
This cleans the trip updates in HDFS, joins them with the timetable and writes
the result back to HDFS as `/sl/stop_events`. See *Preprocessing with Spark* below.

### 7. Run the Spark analytics

```bash
python scripts/spark-analytics.py
```
This answers the project questions from `/sl/stop_events` and writes small
result tables to HDFS and to `data/analytics/*.csv`. See *Analytics with Spark* below.

Notes:

- KoDa builds archives on request, so the first request for a date can take several minutes. The script waits and retries.
- Files that already exist are skipped. If some downloads fail, run the same command again to retry them.
- The real-time archives are `.7z` files. To extract them, install 7-Zip (for example `brew install sevenzip` on macOS).
- `data/` is in `.gitignore` because the files are too large for git.

## Setting up HDFS

A single-node HDFS running on your own machine.

### 1. Install Hadoop

Java 17 is required: Hadoop works with 8, 11 or 17, but Spark 4 only runs on
17 or newer, so 17 is the version that suits both. Check with `java -version`;
on Ubuntu, install it with `sudo apt install openjdk-17-jdk` if it is missing.

```bash
cd ~
curl -LO https://dlcdn.apache.org/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz
curl -LO https://dlcdn.apache.org/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz.sha512

tar -xzf hadoop-3.4.3.tar.gz
mv hadoop-3.4.3 hadoop
```

### 2. Configure

Replace `~/hadoop/etc/hadoop/core-site.xml` with:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <!-- Default filesystem: plain paths like /sl/raw resolve to HDFS. -->
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>

  <!-- Where HDFS keeps its own data. The Hadoop default is under /tmp,
       which Ubuntu wipes on reboot, taking the filesystem with it. -->
  <property>
    <name>hadoop.tmp.dir</name>
    <value>${user.home}/hadoop-data</value>
  </property>
</configuration>
```

Replace `~/hadoop/etc/hadoop/hdfs-site.xml` with:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <!-- Single node: one copy of each block. The default of 3 would store
       every byte three times on the same disk. -->
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

Append to `~/hadoop/etc/hadoop/hadoop-env.sh`:

```bash
# Resolve JAVA_HOME from whichever java is on PATH, so this works on any machine.
export JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$(which java)")")")
```

`core-site.xml` uses `${user.home}`, which Hadoop expands itself, so the same
config works for everyone regardless of username.

### 3. Put the Hadoop commands on your PATH

Add to your `~/.bashrc` (then `source ~/.bashrc`), or to `~/.zshrc` on macOS.
Everything else in this guide works on macOS unchanged, with Java 17 from
Homebrew (`brew install openjdk@17`):

```bash
export HADOOP_HOME=~/hadoop
export PATH=$HADOOP_HOME/bin:$PATH
```

### 4. Format the filesystem

Once only, on first setup. This creates an empty HDFS; running it again wipes
everything already stored.

```bash
hdfs namenode -format -nonInteractive
```

### 5. Start HDFS

```bash
hdfs --daemon start namenode
hdfs --daemon start datanode
```

The daemons keep running in the background but do not survive a reboot, so run
these two commands again after restarting your machine. Stop them with
`hdfs --daemon stop datanode` and `hdfs --daemon stop namenode`.

We start the two daemons directly instead of using `start-dfs.sh`, because that
script reaches every node over SSH and would mean installing an SSH server and
setting up a passwordless key just to talk to your own machine.

### 6. Check it works

```bash
jps                    # expect: NameNode, DataNode
hdfs dfsadmin -report  # expect: a live datanode and its free space
```

The namenode web UI is at <http://localhost:9870>, which also browses the
stored files (*Utilities -> Browse the file system*).

Troubleshooting: if a daemon does not appear in `jps`, read the matching log in
`~/hadoop/logs/`.

## Putting the data into HDFS

With HDFS running:

```bash
python scripts/hdfs-ingression.py
```

The script converts both datasets to Parquet and puts them into HDFS:

```
/sl/gtfs/stops/date=2026-09-07/stops.parquet                   the timetable
/sl/gtfs/routes/date=2026-09-07/routes.parquet
/sl/gtfs/trips/ ... stop_times/ ... calendar/ ...
/sl/tripupdates/date=2026-09-07/hour=16/tripupdates.parquet    the delays
```

The local `.txt` and `.pb` files are never uploaded as they are. They are read,
turned into Parquet, and only the Parquet goes to HDFS, which is both far
smaller and the format Spark reads fastest. The Parquet is first written to
`data/parquet/`, uploaded one date at a time, and deleted again, so the local
disk never holds a second full copy of the dataset.

### Why the folders are named `date=` and `hour=`

HDFS itself has no idea what a date is, its folders are just folders. But
`date=2026-09-07` is the name Hive and Spark recognise, so

```python
spark.read.parquet("hdfs://localhost:9000/sl/tripupdates")
```

comes back with a `date` and an `hour` column that were never stored in the
files, only in the folder names. A query such as `where(date == "2026-09-07")`
then opens one day's folder and skips the rest, instead of reading everything
and throwing most of it away.

The hours are the ones KoDa used to file the snapshots, which are **Stockholm
time**, even though the file names end in `Z`: the `hour=03` folder holds
snapshots from 01:00 to 01:59 UTC. The hour a snapshot was taken is not the
hour the vehicle ran, though, so for questions about the time of day use
`hour_of_day` in `stop_events`, which comes from the scheduled time.

### Why only one snapshot per hour is kept

The TripUpdates feed is a full snapshot of every trip, taken every ~14 seconds:
6,031 files for a single day. The same (trip, stop) therefore appears about a
hundred times per hour, each time with a slightly revised estimate.

Keeping all of it is 7.1 million rows per hour, and about 4.8 billion for the
month. Keeping only the newest row per trip and stop is 74 thousand rows per
hour, a shrink of 96x, and loses nothing: the last estimate before the vehicle
passed is the one that says what really happened. Because the snapshot file
names sort chronologically, the script just overwrites each (trip, stop) as it
reads, and whatever is left at the end is the newest.

### What the delay table contains

| Column | Meaning |
|---|---|
| `trip_id`, `stop_id`, `stop_sequence` | which trip, at which stop, the how-manieth stop of the trip |
| `arrival_delay`, `departure_delay` | seconds late; **negative means early**; empty when SL sent no value |
| `arrival_time`, `departure_time` | when it actually happened, in unix seconds |
| `trip_start_date` | the service day the trip belongs to, for joining with the right timetable |
| `snapshot_time` | when SL published this estimate |
| `date`, `hour` | from the folder names, added by Spark |

### Checking what arrived

```bash
hdfs dfs -ls /sl/tripupdates          # one folder per date
hdfs dfs -ls /sl/tripupdates/date=2026-09-07   # one folder per hour
hdfs dfs -du -h /sl                   # how much each dataset takes
```

`/sl/raw/gtfs/` holds the plain GTFS text files of 28 dates, kept as ground
truth from an earlier upload.

## Preprocessing with Spark

```bash
python scripts/spark-preprocessing.py
# or on a local copy of the /sl folder, without HDFS
python scripts/spark-preprocessing.py --base /path/to/sl
```

This reads the Parquet out of HDFS, cleans it, joins it with the timetable and
writes one table back, with one row per stop that a vehicle really made:

```
/sl/stop_events/service_date=2026-09-15/part-*.parquet
```

What it does, in order:

1. **One row per stop.** The ingestion keeps the newest row per (trip, stop)
   inside each hour, but a trip that runs for a while shows up in several hour
   folders, the earlier ones holding predictions. On two days of data half
   of all (trip, stop) pairs appear in more than one hour. Spark keeps the
   newest per (`trip_id`, `stop_sequence`, `trip_start_date`). The start date
   is part of the key because SL reuses trip ids on different days.
2. **Only what happened.** A stop whose time is still later than the end of
   the hour it was last seen in never got an actual time (the trip was
   cancelled, or it runs past the last downloaded day). About 0.4% of rows,
   dropped.
3. **One delay per stop.** The departure delay at the first stop of a trip,
   since vehicles arrive early there and wait (median arrival delay -75 s,
   departure +23 s), and the arrival delay everywhere else. Rows without any
   delay (0.2%) and delays outside -15 min to +2 h (feed errors, about 0.04%)
   are dropped.
4. **Joined with the timetable of the same day.** `trips`, `routes`, `stops`
   and `stop_times` are taken from the timetable whose `date` equals the trip's
   service day. About 0.3% of trips are not in that timetable and are dropped.
   Each platform is mapped to its `parent_station`, so all platforms of
   a station count as one.
5. **Ready for the analysis.** The scheduled time, actual time, Stockholm hour
   of day, weekday or weekend, and the transport mode are added.

| Column | Meaning |
|---|---|
| `service_date` | the day the trip belongs to (partition folder) |
| `trip_id`, `stop_sequence` | which trip, the how-manieth stop |
| `route_id`, `route_short_name`, `route_desc`, `route_type`, `transport_mode` | the line, e.g. `14`, `tunnelbanans röda linje`, `401`, `metro` |
| `direction_id`, `agency_id` | direction of travel, operator |
| `stop_id`, `stop_name`, `platform_code` | the platform the vehicle stopped at |
| `station_id`, `station_name`, `station_lat`, `station_lon` | the station that platform belongs to, for the map |
| `scheduled_time`, `actual_time` | timestamps; `actual_time - scheduled_time = delay_seconds` |
| `delay_seconds` | seconds late, negative means early |
| `is_late` | at least 3 minutes late |
| `hour_of_day` | Stockholm hour of the scheduled time, 0-23 |
| `day_of_week`, `day_type` | `Monday`..., and `weekday` or `weekend` |

Line names are not unique across transport modes: `14` is both a metro line
and a Waxholmsbolaget ferry. Group lines by `route_id`, or by
`transport_mode` and `route_short_name` together, never by the name alone.

Spark reads straight from HDFS over `hdfs://localhost:9000`, so the daemons
have to be running, and nothing is copied back to the local disk. Spark 4 needs
Java 17 (step 1 of the HDFS setup); on Java 11 it stops with
`UnsupportedClassVersionError ... class file version 61.0`.

## Analytics with Spark

```bash
python scripts/spark-analytics.py
# or on a local copy of the /sl folder, without HDFS
python scripts/spark-analytics.py --base /path/to/sl
```

This reads `/sl/stop_events` and writes one small table per question, as
Parquet to `/sl/analytics/<table>` and as CSV to `data/analytics/<table>.csv`
for the dashboard. It also prints the main results.

| Table | One row per | Answers |
|---|---|---|
| `lines` | line (`route_id`) | which lines have the highest average delay (`rank_avg_delay`) |
| `stations` | station, with coordinates | which stations are most often late (`rank_late_pct`), which are busiest (`rank_busiest`), the map |
| `hourly` | `day_type` (`weekday`, `weekend`, `all`) and hour | during which hours delays are worst |
| `day_types` | transport mode (and `all`) and `day_type` | how weekdays and weekends differ |
| `histogram` | mode, `day_type` and one-minute delay bin | the delay distribution |
| `daily` | service day | spotting days that stand out |

Every table has the same measures: `stops`, `avg_delay_s`, `median_delay_s`,
`p90_delay_s`, `late_stops` and `late_pct` (share of stops 3+ minutes late).

How to read the numbers:

- **Only lines and stations with enough data are ranked**: at least 100 stops
  per day for a line and 50 for a station. The others are still in the
  tables, with an empty rank, so the dashboard can show them.
- **Lines and stations are grouped by id, not name.** `14` is a metro line and
  a ferry, and `Slussen` is five stations (metro, several bus stops, ferry).
  The `modes` column says which of them a station is. When a timetable
  renames or moves a stop, the newest name and position are used.
- **A stop is a vehicle stopping, not a passenger.** "Busiest station" means
  the most departures, which favours big bus terminals.
- **Delays add up along a trip**, so a station's numbers mostly reflect the
  lines that run through it: in four days of data, seven of the ten
  most-often-late stations are consecutive stops of buses 835/848/893
  around Tungelsta.
- **Night hours have few stops**, so their averages move a lot from day to day.
  Look at `stops` next to `avg_delay_s` in `hourly`.
