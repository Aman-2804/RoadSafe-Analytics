# Quick Start Guide

## Simple Commands (No Full Paths Needed!)

### 1. Start Spark Container + Enter It

```bash
docker compose up -d
docker exec -it roadsafe-spark bash
```

### 2. Inside Container - Sanity Checks

```bash
# Check you're in the right place
pwd          # Should show: /workspace
ls           # Should see your repo files

# Check Spark works
spark-submit --version

# Check input files exist
ls -lh data/raw/
ls -lh docs/icbc_tas_reference/
```

### 3. Run Bronze Ingest (Simple!)

```bash
# Easy way - use the helper function
spark-run spark/01_ingest_raw.py

# Or directly
spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py
```

### 4. Verify Output

```bash
ls -lh data/bronze/
ls -lh data/bronze/nyc_collisions/

# Quick peek at parquet
spark-shell -e 'spark.read.parquet("data/bronze/nyc_collisions").show(5,false)'
```

## What's Configured Automatically

✅ **PATH** - Spark binaries are in PATH (no `/opt/spark/bin/` needed)
✅ **PYTHONPATH** - Set to `/workspace` (imports work automatically)
✅ **SPARK_MASTER** - Default is `spark://localhost:7077`
✅ **Helper Function** - `spark-run <script.py>` for easy execution

## All Your Scripts

```bash
# Bronze layer
spark-run spark/01_ingest_raw.py

# Silver layer
spark-run spark/02_clean_silver.py

# Gold layer
spark-run spark/03_build_gold.py

# ICBC view
spark-run spark/04_build_icbc_view.py

# Claims generation
spark-run spark/05_generate_claims.py

# Data quality tests
spark-run spark/06_data_quality_tests.py
```

## From Host Machine (Alternative)

If you prefer to run from your host machine:

```bash
./run_spark.sh spark/01_ingest_raw.py
```

This wrapper script handles all the Docker and path setup for you.




