#!/bin/bash
# Helper script to run Spark jobs easily
# Usage: ./run_spark.sh spark/01_ingest_raw.py

SPARK_MASTER="spark://localhost:7077"
SCRIPT="$1"

if [ -z "$SCRIPT" ]; then
    echo "Usage: ./run_spark.sh <spark_script.py>"
    exit 1
fi

docker compose exec spark bash -c "cd /workspace && spark-submit --master $SPARK_MASTER $SCRIPT"

