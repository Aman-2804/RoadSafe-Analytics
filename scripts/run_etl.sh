#!/bin/bash

# RoadSafe Analytics Platform - ETL Pipeline Runner
# This script orchestrates the complete ETL process

set -e  # Exit on error

echo "🚀 Starting RoadSafe Analytics ETL Pipeline..."

# Set environment variables
export SPARK_HOME=${SPARK_HOME:-/usr/local/spark}
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Step 1: Data Ingestion
echo "📥 Step 1: Ingesting raw data..."
spark-submit spark/ingestion.py

# Step 2: Data Transformation
echo "🔄 Step 2: Transforming and cleaning data..."
spark-submit spark/transformations.py

# Step 3: Load to Database
echo "💾 Step 3: Loading data to database..."
spark-submit spark/load.py

echo "✅ ETL Pipeline completed successfully!"








