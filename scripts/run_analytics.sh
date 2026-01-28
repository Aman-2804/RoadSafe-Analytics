#!/bin/bash

# RoadSafe Analytics Platform - Analytics Query Runner
# This script executes all analytics queries

set -e  # Exit on error

echo "📊 Running RoadSafe Analytics Queries..."

# Set environment variables
export SPARK_HOME=${SPARK_HOME:-/usr/local/spark}
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Run analytics queries
for query_file in sql/*.sql; do
    if [ -f "$query_file" ]; then
        echo "Executing: $query_file"
        spark-sql -f "$query_file"
    fi
done

echo "✅ Analytics queries completed!"








