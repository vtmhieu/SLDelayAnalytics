#!/bin/sh
# Format the namenode the first time it starts (its volume is empty), then run the command.
set -e
if [ "$1" = "hdfs" ] && [ "$2" = "namenode" ] && [ ! -d /hadoop/nn/current ]; then
  hdfs namenode -format -nonInteractive -force
fi
exec "$@"
