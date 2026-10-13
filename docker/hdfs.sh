#!/usr/bin/env bash
# Move data between the local data/ folder and HDFS in the Docker cluster.
#
#   docker/hdfs.sh load         copy data/bronze (real-time + static) to hdfs:///sl/bronze
#   docker/hdfs.sh fetch-gold   copy hdfs:///sl/gold to data/gold for the dashboard
#   docker/hdfs.sh ls           list everything under hdfs:///sl
set -euo pipefail

cd "$(dirname "$0")"
hdfs() { docker compose exec -T namenode hdfs dfs "$@"; }

case "${1:-}" in
  load)
    hdfs -mkdir -p /sl/bronze
    for table in tripupdates static; do
      hdfs -rm -r -f -skipTrash "/sl/bronze/$table"
      hdfs -put "/work/data/bronze/$table" /sl/bronze/
    done
    hdfs -du -s -h /sl/bronze/tripupdates /sl/bronze/static
    ;;
  fetch-gold)
    rm -rf ../data/gold
    mkdir -p ../data/gold
    hdfs -get '/sl/gold/*' /work/data/gold/   # quoted: HDFS expands the glob
    ls ../data/gold
    ;;
  ls)
    hdfs -ls -R /sl | grep -v '\.parquet$\|\.crc$\|_SUCCESS$\|\.txt$'
    ;;
  *)
    sed -n '2,7p' "$0"
    exit 1
    ;;
esac
