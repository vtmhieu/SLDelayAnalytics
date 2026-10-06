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
