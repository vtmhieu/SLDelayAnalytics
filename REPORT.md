# Stockholm Public Transport Delay Analytics Using Apache Spark

Group 67: Hieu Vu, Rong Jin, Serban

## 1. Problem

Stockholm's public transport produces a constant stream of real-time data
about where every vehicle is and how late it runs. We use a month of that
data to answer four questions:

1. Which transport lines have the highest average delays?
2. Which stations experience the most frequent delays?
3. During which hours are delays most severe?
4. How do delay patterns differ between weekdays and weekends?

and, as a side result, which stations are the busiest.

## 2. Data

All data comes from SL through Trafiklab's KoDa archive, which keeps
historical copies of SL's open feeds. We use two parts of it, and the analysis
needs both:

- **GTFS-RT TripUpdates** say what actually happened. KoDa stores a full
  snapshot of the feed about every 14 seconds, around 6,000 files a day, each
  holding the latest arrival and departure time and delay for every running
  trip at every stop. Trips and stops are only identified by ids.
- **Static GTFS** is the planned timetable: `stops`, `routes`, `trips`,
  `stop_times` and `calendar`. It turns the ids into line numbers, station
  names and coordinates, and gives the scheduled times. SL changes the
  timetable over time and trip ids change with it, so we download the
  timetable that was valid on each day.

Of the static tables we keep the identifiers that connect the two datasets
(`trip_id`, `stop_id`, `route_id`, `service_id`, `shape_id`,
`parent_station`, `agency_id`) and the descriptive columns needed for the
answers (line names, transport mode, station names and positions, scheduled
times).

We downloaded trip updates for 17 days (7–8 and 12–26 September 2026) and the
timetable for 17 days (7–8 and 13–27 September). A service day can only be
analysed when both exist, which leaves **16 service days: 12 weekdays and 4
weekend days**, with 9.5 million vehicle stops on 481,507 trips, 578 lines
and 5,891 stations.

## 3. Pipeline

```
 Trafiklab KoDa
      │  download_koda.py          .7z / .zip archives per day
      ▼
 data/raw  ──  decompresser.py     (or hdfs-ingression.py --day-by-day)
      │
      ▼  hdfs-ingression.py        protobuf / CSV  ->  Parquet
 HDFS /sl/gtfs/<table>/date=…            the timetable, one copy per day
      /sl/tripupdates/date=…/hour=…      newest delay per trip and stop per hour
      │
      ▼  spark-preprocessing.py    dedup, clean, join with the timetable
 HDFS /sl/stop_events/service_date=…     one row per stop a vehicle made
      │
      ▼  spark-analytics.py        aggregations
 HDFS /sl/analytics/<table>  +  data/analytics/<table>.csv
      │
      ▼  dashboard.py              Plotly Dash
 http://127.0.0.1:8050
```

Storage is a single-node HDFS (Hadoop 3.4.3) and processing is PySpark 4 in
local mode. All intermediate data is Parquet, partitioned by day (and hour for
the raw delays), which Spark reads as `date` and `hour` columns and can skip
folders by.

### 3.1 Ingestion

`hdfs-ingression.py` converts each day's timetable tables from CSV to Parquet
and uploads them under `/sl/gtfs/<table>/date=<day>`. Identifier columns are
read as text so that ids like `9021001000101000` keep their exact value.

The trip updates are where the volume is. One day is about 6,000 snapshot
files and 4.5 GB unpacked, and keeping every snapshot would be about 7 million
rows per hour. Each snapshot repeats every running trip with a slightly
revised estimate, and only the last estimate before the vehicle passed says
what really happened. So the script reads the snapshots of each hour in
order and keeps the newest row per trip, service day and stop, which shrinks
the data about 96 times without losing information. The hours are converted
in parallel (8 worker processes) and uploaded one day at a time, and the local
Parquet is deleted after upload.

Because a month of unpacked snapshots (about 85 GB) does not fit on a laptop,
`--day-by-day` unpacks, ingests and deletes one date at a time.

### 3.2 Preprocessing (Spark)

`spark-preprocessing.py` turns the per-hour rows into one clean table with one
row per stop a vehicle made:

1. **Deduplicate across hours.** A trip that runs for a while shows up in
   several hour folders, the earlier ones holding predictions. A window over
   (`trip_id`, `stop_sequence`, `trip_start_date`) keeps the newest row. The
   service day is part of the key because SL reuses trip ids on other days.
2. **Keep only what happened.** If a stop's time is still later than the end
   of the hour it was last seen in, it never got an actual time (the trip was
   cancelled or runs past the last downloaded day), and it is dropped.
3. **Pick one delay per stop.** At the first stop of a trip vehicles arrive
   early and wait, so the departure delay is used there and the arrival delay
   everywhere else. Rows without a delay, and delays outside -15 minutes to
   +2 hours (feed errors), are dropped.
4. **Join with the timetable of the same day**: `trips` (line), `routes`
   (line name and transport mode), `stops` (platform, station and
   coordinates) and `stop_times` (scheduled time). Platforms are mapped to
   their parent station so that all platforms of a station count together.
5. **Derive the analysis columns**: scheduled and actual time, the Stockholm
   hour of day of the scheduled time, weekday or weekend of the service day,
   and whether the stop was late (3 minutes or more).

On the full month:

| Step | Rows | Note |
|---|---:|---|
| Rows in HDFS (newest per trip, stop and hour) | 15,312,827 | |
| After deduplicating across hours | 10,082,707 | a third of the rows were older estimates |
| After cleaning | 10,050,522 | 0.3% never happened, had no delay or an impossible one |
| After joining the timetable | **9,522,561** | |

Of the 527,961 rows lost in the join, 502,822 belong to service days whose
timetable was never downloaded (6, 11 and 12 September), so none of their
trips can be matched. On every other day only 0.2–0.3% of rows are not in that
day's timetable.

Preprocessing the month takes about 80 seconds on a laptop.

### 3.3 Analytics (Spark)

`spark-analytics.py` reads the stop events and computes, for every grouping,
the same measures: number of stops, average, median and 90th percentile delay,
and the share of stops 3 or more minutes late. The groupings are line,
station, hour of day by day type and transport mode, day type by transport
mode, one-minute delay bins (for the distribution) and service day. Medians
and percentiles are exact.

Two details matter for correct answers:

- **Group by id, not by name.** Line `14` is both a metro line and a
  Waxholmsbolaget ferry, and `Slussen` is five separate stations (metro,
  several bus terminals, ferry). Lines are grouped by `route_id` and stations
  by `station_id`, with the newest name and position, since a timetable
  change can rename or move a stop.
- **Only rank what has enough data.** A line needs at least 100 stops per
  day and a station 50 to be ranked, so that a rarely served stop with three
  late buses does not top the list. The threshold scales with the number of
  days loaded.

Results are written to HDFS and exported as CSV for the dashboard.

### 3.4 Visualization

`dashboard.py` is a Plotly Dash page over the exported CSVs, with one
transport-mode filter for all charts: headline numbers, delay by hour
(weekday against weekend), the most delayed lines, the stations most often
late, a map of stations shaded by how often they are late, the delay
distribution, weekday against weekend per mode, and the busiest stations.

## 4. Results

Over the 16 days the average stop is **92 seconds late** (median 60 s), and
**20.3% of stops are 3 or more minutes late**. Delays are skewed: a quarter
of stops are early, another quarter are up to a minute late, 8.5% are 5+
minutes late and 1.5% are 10+ minutes late.

### 4.1 Which lines have the highest average delays?

Among the 485 lines with at least 100 stops per day:

| Rank | Line | Stops | Average delay | Median | Late (3+ min) |
|---:|---|---:|---:|---:|---:|
| 1 | Bus 791 | 6,499 | 9 min 08 s | 8 min 01 s | 84.8% |
| 2 | Bus 893 | 8,606 | 7 min 37 s | 6 min 08 s | 76.2% |
| 3 | Bus 848 | 28,919 | 5 min 16 s | 4 min 23 s | 65.6% |
| 4 | Bus 492 | 2,545 | 4 min 47 s | 3 min 44 s | 59.5% |
| 5 | Bus 981 | 1,954 | 4 min 41 s | 3 min 31 s | 56.5% |
| 6 | Bus 191 | 5,236 | 4 min 19 s | 3 min 37 s | 58.8% |
| 7 | Bus 795 | 6,782 | 4 min 11 s | 3 min 13 s | 53.4% |
| 8 | Bus 67 | 49,978 | 3 min 58 s | 2 min 52 s | 48.9% |
| 9 | Bus 702 | 35,670 | 3 min 51 s | 2 min 25 s | 42.6% |
| 10 | Bus 780 | 1,930 | 3 min 50 s | 3 min 18 s | 56.2% |

The worst lines are all buses. Bus 791 is late for 85% of its stops. Bus 67
stands out as the busiest line in the top ten, with 50,000 stops. Rail
modes are far more punctual: no metro, commuter train or tram line averages
more than about a minute late (metro lines between -16 s and +30 s), against
a median of 88 s across bus lines.

### 4.2 Which stations experience the most frequent delays?

Among stations with at least 50 stops per day, the stations most often 3+
minutes late are all bus stops, in two corridors:

- **Tungelsta** (Haninge): Tungelsta skola (76.5% late), Mulstavägen (66.8%),
  Hammarbergsvägen, Holmvretsvägen and Lidaskolan (around 62%), all
  stops along buses 835, 848 and 893.
- **Östermalm**: Skeppargatan and Runebergsplan (62.3%), Historiska museet
  (61.9%), Strandvägen (60.4%) and Karlaplan (59.4%), all on the route of
  bus 67.

Delays build up along a trip, so a station's punctuality mostly reflects the
lines through it rather than the station itself.

### 4.3 During which hours are delays most severe?

On weekdays, delays grow through the day and **peak in the afternoon rush,
at 16:00 (2 min 02 s average, 27.6% late)**. There is a smaller bump in the
morning rush at 08:00 (95 s, 22.2%), and the most punctual daytime hours are
06:00 and 10:00 (about 72 s, 15%). On weekends, delays rise from 10:00 and stay
high, around 2 minutes, until 17:00.

The hours from 01:00 to 04:00 have high averages (up to 2 min 11 s), but only
2.8% of stops run then, so they matter less and vary more.

### 4.4 How do weekdays and weekends differ?

| Mode | Weekday avg | Weekend avg | Weekday late | Weekend late |
|---|---:|---:|---:|---:|
| All | 89 s | 108 s | 19.7% | 23.2% |
| Bus | 98 s | 124 s | 21.8% | 26.8% |
| Ferry | 106 s | 86 s | 24.0% | 20.3% |
| Metro | 13 s | 3 s | 2.5% | 0.9% |
| Train | 26 s | -5 s | 8.5% | 3.5% |
| Tram | 29 s | 24 s | 3.3% | 3.0% |

Weekends are worse overall, by **19 seconds on average** and 3.5 points
more late stops, but only because of the buses. Every other mode is more
punctual at the weekend, when there are fewer services. Saturdays were the two
worst days of the month (115 s and 126 s average). Across all modes the
weekend is furthest behind between 10:00 and 14:00, by 27 to 40 seconds.

### 4.5 Busiest stations

By vehicle stops per day, the busiest stations are bus terminals:
Londonviadukten (2,299 stops per day, 47 lines), Henriksdal (2,199, 42 lines),
Danderyds sjukhus (2,103), Gullmarsplan (1,981) and T-Centralen's metro station
(1,868, all 7 metro lines). Busy does not mean late: Henriksdal and
Londonviadukten have 35% and 27% late stops, while Gullmarsplan has 7% and
T-Centralen 2%.

## 5. Verification

Every stage was checked against an independent implementation rather than by
inspection:

- **Preprocessing.** The whole step was rebuilt in pandas from the same input
  and compared row by row with Spark's output: the same rows, and every
  column identical (delay, scheduled and actual time, hour, station, line,
  day type). Random rows were traced back to the raw protobuf snapshots and
  their delays matched. Scheduled time plus delay equals the actual time for
  every row but a handful (see below), which confirms the timetable join and
  the time zone handling.
- **Analytics.** All six tables were recomputed in pandas from the full
  month of stop events and compared value by value, rankings included, with
  no differences.
- **Full month.** The 9.5 million stop events have no duplicate stops and no
  missing values, and all delays are within the cleaning bounds. Six rows
  (out of 9.5 million) have a delay that disagrees with SL's own timestamps
  by up to a minute, an inconsistency in the feed itself.
- **Things the checks caught.** The checks found real bugs that were then
  fixed: a station whose coordinates moved 70 m between timetable versions
  was split into two half-counted rows; rankings were arbitrary on ties; and
  Spark wrote HDFS blocks with three replicas on a single node.
- **Data facts.** The hour folders turned out to be Stockholm time, not UTC
  (the snapshots in `hour=03` are from 01:00 to 01:59 UTC), so the hour of day
  is taken from the scheduled time instead.

## 6. Limitations

- **Sixteen days in September.** No holidays, no winter weather, and one
  timetable season. The weekend results rest on two Saturdays and two Sundays.
- **Missing days around the edges.** Trip updates for 9–11 September and 27
  September were not downloaded, so the trips of 8 and 26 September that run
  past midnight never got an actual time and are missing (about 26,000 and
  35,000 stops). This slightly weakens the 00:00–04:00 figures.
- **Vehicles, not passengers.** Every stop counts the same, whether a full
  metro train or an empty night bus, and "busiest" means the most vehicle
  stops.
- **Delays add up along a trip.** A station's numbers mostly reflect the lines
  through it: the most-often-late stations are consecutive stops of the same
  few bus lines.
- **Cancelled trips are invisible.** A trip that never ran has no actual time
  and is dropped, so cancellations, often the worst disruption for a
  passenger, do not count as delays.
- **Single machine.** HDFS and Spark run on one laptop. The pipeline is written
  for a cluster (partitioned Parquet in HDFS, Spark jobs) but was not run on
  one.

## 7. Running it

The README describes every step. In short, with HDFS running:

```bash
python scripts/download_koda.py --start 2026-09-07 --end 2026-09-27
python scripts/hdfs-ingression.py --day-by-day
python scripts/spark-preprocessing.py
python scripts/spark-analytics.py
python scripts/dashboard.py
```
