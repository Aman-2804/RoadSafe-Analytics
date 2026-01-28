# RoadSafe Analytics Platform

A Hadoop/Spark-based data engineering pipeline for traffic collisions, claims, and road safety analytics.

## 🎯 Project Overview

This project demonstrates enterprise-level data engineering capabilities by building a complete ETL pipeline that processes traffic collision and insurance claim datasets. The platform transforms raw data into analytics-ready fact and dimension tables, enabling business insights related to road safety and claim severity.

**Tech Stack:** Apache Spark, Hadoop, SQL, Python, PostgreSQL, Data Engineering

## 🏗️ Architecture

```
Raw CSVs (Traffic / Weather / Claims)
        ↓
Spark Ingestion (Schema enforced)
        ↓
Data Cleaning & Validation
        ↓
Fact + Dimension Tables
        ↓
Analytics Queries (Spark SQL)
        ↓
Dashboard / Reports
```

## 📁 Project Structure

```
roadsafe-data-platform/
├── data/
│   ├── raw/              # Raw source data files
│   └── processed/        # Processed/cleaned data
├── spark/                # PySpark ETL jobs
├── sql/                  # Analytics queries
├── docs/                 # Architecture, source→target mapping, test plan
│   └── icbc_tas_reference/  # ICBC TAS reference data and codes
├── dashboards/           # Visualization exports and screenshots
└── scripts/              # Utility scripts and orchestration
```

## 🔧 Tech Stack

- **Storage:** HDFS (local Hadoop pseudo-cluster)
- **Processing:** Apache Spark (PySpark)
- **SQL:** Spark SQL + ANSI SQL
- **ETL:** Spark transformations
- **Database:** PostgreSQL (enterprise RDBMS)
- **Orchestration:** Bash scripts / Makefile
- **CI:** GitHub Actions
- **Visualization:** Tableau / Power BI / Superset

## 📊 Data Model

### Fact Tables
- `fact_collisions` - Collision events with measures
- `fact_claims` - Insurance claim transactions

### Dimension Tables
- `dim_location` - Geographic locations (intersections, highways, municipalities)
- `dim_vehicle` - Vehicle type classifications
- `dim_weather` - Weather condition codes
- `dim_time` - Time dimension (date, hour, day of week, etc.)

## 🔍 Business Questions

1. Which intersections have the highest collision frequency?
2. Do weather conditions correlate with claim severity?
3. Peak collision times by day/hour
4. Which vehicle types have higher average claim payouts?
5. Collision trends before vs after road safety changes

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Apache Spark 3.x
- PostgreSQL
- Java 8+ (for Spark)

### Installation

```bash
pip install -r requirements.txt
```

### Running the Pipeline

```bash
# Run ETL pipeline
./scripts/run_etl.sh

# Run analytics queries
./scripts/run_analytics.sh
```

## 🐳 Docker + Spark Environment Setup

This section documents the complete Docker setup process, including all errors encountered and their solutions. This setup simulates "Data Ops" and provides reproducible environments like enterprise deployments.

### Purpose

The Docker setup provides:
- A Spark container you can run jobs from
- A shared folder (your repo) mounted into the container
- Reproducible development environment
- Enterprise-like deployment simulation

### Initial Setup Attempt

**Step 1: Created `docker-compose.yml`**

Initially, we attempted to use the Bitnami Spark image as specified:

```yaml
services:
  spark:
    image: bitnami/spark:3.5
    container_name: roadsafe-spark
    environment:
      - SPARK_MODE=master
    volumes:
      - ./:/workspace
    working_dir: /workspace
    ports:
      - "4040:4040"   # Spark UI for jobs
    command: bash -lc "sleep infinity"
```

### Error Encountered

**Error Message:**
```
Error response from daemon: failed to resolve reference "docker.io/bitnami/spark:3.5": 
docker.io/bitnami/spark:3.5: not found
```

**Root Cause:**
The Docker image tag `bitnami/spark:3.5` does not exist in Docker Hub. The Bitnami Spark image either:
- Doesn't have a `3.5` tag available
- May require a different tag format (e.g., `3.5.0`, `latest`, or version-specific tags)
- May not be publicly available in the expected format

### Investigation Process

1. **Checked Docker Hub for available tags:**
   ```bash
   docker search bitnami/spark --limit 5
   ```
   - Found that `bitnami/spark` exists but specific tags weren't accessible

2. **Attempted alternative tags:**
   - `bitnami/spark:latest` - Not found
   - `bitnami/spark:3` - Not found
   - `bitnami/spark:3.5.0` - Not found

3. **Explored official Apache Spark image:**
   - Tested `apache/spark:3.5.0` - **Successfully available!**
   - This is the official Apache Spark Docker image maintained by the Apache Software Foundation

### Solution Implemented

**Final Working Configuration:**

Updated `docker-compose.yml` to use the official Apache Spark image:

```yaml
services:
  spark:
    image: apache/spark:3.5.0
    container_name: roadsafe-spark
    volumes:
      - ./:/workspace
    working_dir: /workspace
    ports:
      - "4040:4040"   # Spark UI for jobs
      - "7077:7077"   # Spark master port
      - "8080:8080"   # Spark master web UI
    command: >
      bash -c "
      /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
      --host 0.0.0.0
      --port 7077
      --webui-port 8080
      "
```

**Key Changes:**
1. **Image:** Changed from `bitnami/spark:3.5` to `apache/spark:3.5.0`
   - Official Apache Spark image
   - Version 3.5.0 (matches Spark 3.5 requirement)
   - Publicly available and maintained

2. **Configuration:**
   - Removed `SPARK_MODE=master` environment variable (not used by Apache image)
   - Added explicit Spark Master startup command
   - Added additional port mappings (7077 for master, 8080 for master UI)

3. **Command:**
   - Starts Spark Master in standalone mode
   - Binds to `0.0.0.0` to allow external connections
   - Configures web UI on port 8080

### Verification

**Start the container:**
```bash
docker compose up -d
```

**Expected Output:**
- Container `roadsafe-spark` starts successfully
- Spark Master initializes and runs
- Logs show: `Master: I have been elected leader! New state: ALIVE`

**Check container status:**
```bash
docker compose ps
```

**View logs:**
```bash
docker compose logs spark
```

### Using the Spark Container

**1. Execute commands in the container:**
```bash
docker compose exec spark bash
```

**2. Run Spark jobs:**
```bash
# Method 1: From host machine (RECOMMENDED)
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py"

# Method 2: Enter container first, then run
docker compose exec spark bash
cd /workspace
export PYTHONPATH=/workspace
/opt/spark/bin/spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py
```

**⚠️ Important:** Always use the full path `/opt/spark/bin/spark-submit` and set `PYTHONPATH=/workspace` to ensure imports work correctly.

**3. Access Spark UIs:**
- **Spark Master UI:** `http://localhost:8080` - Shows cluster status, workers, running applications
- **Spark Application UI:** `http://localhost:4040` - Shows details of running Spark applications

**4. Access your project files:**
- All project files are mounted at `/workspace` inside the container
- Changes made on host are immediately available in container
- Files created in container are available on host

### Container Details

**Ports:**
- `4040` - Spark Application UI (shows when jobs are running)
- `7077` - Spark Master RPC port (for connecting workers/applications)
- `8080` - Spark Master Web UI (cluster overview)

**Volume Mount:**
- `./:/workspace` - Entire project directory mounted into container
- Working directory set to `/workspace` for convenience

**Container Name:**
- `roadsafe-spark` - Easy to reference in commands

### Troubleshooting

**Issue: Port already in use**
```bash
# Check what's using the port
lsof -i :4040
lsof -i :7077
lsof -i :8080

# Stop the container
docker compose down

# Change ports in docker-compose.yml if needed
```

**Issue: Container exits immediately**
```bash
# Check logs for errors
docker compose logs spark

# Verify image was pulled correctly
docker images | grep spark
```

**Issue: Cannot connect to Spark Master**
- Ensure container is running: `docker compose ps`
- Check master is listening: `docker compose logs spark | grep "Starting Spark master"`
- Verify port mapping: `docker compose ps` should show ports mapped

**Issue: Files not visible in container**
- Verify volume mount: `docker compose exec spark ls -la /workspace`
- Check working directory: `docker compose exec spark pwd`

### Key Learnings

1. **Image Availability:** Not all Docker images have the tags you expect. Always verify tags exist before using them.

2. **Official vs Third-Party:** Official images (like `apache/spark`) are often more reliable and better maintained than third-party alternatives.

3. **Image Differences:** Different images may have different configuration methods:
   - Bitnami images often use environment variables (`SPARK_MODE`)
   - Apache images use explicit commands and configuration files

4. **Port Management:** Spark requires multiple ports:
   - Master RPC port (7077) for cluster communication
   - Master UI (8080) for cluster monitoring
   - Application UI (4040) for job monitoring

5. **Volume Mounts:** Mounting the entire project directory allows seamless development - edit on host, run in container.

### Next Steps

With the Docker environment set up, you can now:
- Develop Spark jobs locally
- Test ETL pipelines in an isolated environment
- Run analytics queries using Spark SQL
- Access Spark UIs for monitoring and debugging

## 📦 Step 3: Bronze Layer Ingestion (Raw CSV → Parquet)

This section documents the creation of the first real Spark job that ingests raw CSV data and converts it to Parquet format in the Bronze layer.

### Why Bronze Layer Matters

The Bronze layer is a standard data lake pattern (part of the medallion architecture):
- **Raw data standardized:** Converts various formats (CSV, JSON, etc.) into a consistent format
- **Parquet benefits:**
  - **Columnar storage:** Faster analytical queries
  - **Compression:** Reduces storage costs
  - **Type safety:** Schema is preserved
  - **Splittable:** Enables parallel processing
- **Downstream consistency:** Everything after Bronze uses the same fast, typed format

### Implementation

**Created: `spark/01_ingest_raw.py`**

This script:
1. Reads the raw CSV file from `data/raw/nyc_motor_vehicle_collisions.csv`
2. Infers the schema automatically
3. Prints the schema for inspection
4. Shows sample rows for data exploration
5. Writes the data as Parquet to `data/bronze/nyc_collisions`

**Script Details:**

```python
from pyspark.sql import SparkSession

RAW_PATH = "data/raw/nyc_motor_vehicle_collisions.csv"
BRONZE_OUT = "data/bronze/nyc_collisions"

def main():
    spark = (SparkSession.builder
             .appName("roadsafe-bronze-ingest")
             .getOrCreate())

    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(RAW_PATH))

    print("=== RAW SCHEMA ===")
    df.printSchema()

    print("=== SAMPLE ROWS ===")
    df.show(20, truncate=False)

    # Write as Parquet (faster + typed + splittable)
    (df.write
       .mode("overwrite")
       .parquet(BRONZE_OUT))

    print(f"Wrote bronze parquet to: {BRONZE_OUT}")
    spark.stop()

if __name__ == "__main__":
    main()
```

**Key Components Explained:**

1. **SparkSession:** Entry point for Spark functionality
   - `.appName()`: Sets the application name (visible in Spark UI)
   - `.getOrCreate()`: Reuses existing session or creates new one

2. **CSV Reading:**
   - `option("header", "true")`: First row contains column names
   - `option("inferSchema", "true")`: Automatically detects data types (int, string, double, etc.)

3. **Schema Inspection:**
   - `printSchema()`: Shows column names and data types
   - `show(20, truncate=False)`: Displays 20 rows with full content (no truncation)

4. **Parquet Writing:**
   - `.mode("overwrite")`: Replaces existing data if it exists
   - `.parquet()`: Writes in Parquet format (columnar, compressed)

### Directory Structure

Created the Bronze layer directory:
```
data/
├── raw/                    # Original CSV files
│   └── nyc_motor_vehicle_collisions.csv
└── bronze/                 # Standardized Parquet files
    └── nyc_collisions/     # Parquet files (created by Spark)
        ├── part-00000-*.parquet
        ├── part-00001-*.parquet
        └── _SUCCESS         # Marker file indicating successful write
```

### Running the Ingestion Job

**From Docker Container:**

```bash
# Method 1: Execute from host machine (RECOMMENDED)
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py"

# Method 2: Enter container and run
docker compose exec spark bash
cd /workspace
export PYTHONPATH=/workspace
/opt/spark/bin/spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py
```

**⚠️ Important Notes:**
- Always run from `/workspace` directory (project root)
- Set `PYTHONPATH=/workspace` so Python can find the `spark` module
- Use full path `/opt/spark/bin/spark-submit` (not just `spark-submit`)
- Never run `spark-submit` directly on your host machine - Spark only exists in the container

**Expected Output:**

```
=== RAW SCHEMA ===
root
 |-- CRASH DATE: string (nullable = true)
 |-- CRASH TIME: string (nullable = true)
 |-- BOROUGH: string (nullable = true)
 |-- ZIP CODE: string (nullable = true)
 |-- LATITUDE: double (nullable = true)
 |-- LONGITUDE: double (nullable = true)
 |-- LOCATION: string (nullable = true)
 |-- ON STREET NAME: string (nullable = true)
 |-- CROSS STREET NAME: string (nullable = true)
 |-- OFF STREET NAME: string (nullable = true)
 |-- NUMBER OF PERSONS INJURED: integer (nullable = true)
 |-- NUMBER OF PERSONS KILLED: integer (nullable = true)
 |-- NUMBER OF PEDESTRIANS INJURED: integer (nullable = true)
 |-- NUMBER OF PEDESTRIANS KILLED: integer (nullable = true)
 |-- NUMBER OF CYCLIST INJURED: integer (nullable = true)
 |-- NUMBER OF CYCLIST KILLED: integer (nullable = true)
 |-- NUMBER OF MOTORIST INJURED: integer (nullable = true)
 |-- NUMBER OF MOTORIST KILLED: integer (nullable = true)
 |-- CONTRIBUTING FACTOR VEHICLE 1: string (nullable = true)
 |-- CONTRIBUTING FACTOR VEHICLE 2: string (nullable = true)
 |-- ... (more columns)

=== SAMPLE ROWS ===
[Shows first 20 rows of data]

Wrote bronze parquet to: data/bronze/nyc_collisions
```

### Troubleshooting

**Issue: `spark-submit: command not found` (when running locally)**
- **Error:** `zsh: command not found: spark-submit`
- **Cause:** Spark is not installed on your local machine - it only exists in the Docker container
- **Solution:** Always run Spark jobs through Docker:
  ```bash
  docker compose exec spark /opt/spark/bin/spark-submit \
    --master spark://localhost:7077 \
    spark/01_ingest_raw.py
  ```
- **Note:** Never run `spark-submit` directly on your host machine - it must run inside the container

**Issue: `spark-submit: command not found` (in container)**
- **Solution:** Use full path: `/opt/spark/bin/spark-submit`
- **Reason:** The Apache Spark image doesn't add Spark binaries to PATH by default

**Issue: `ModuleNotFoundError: No module named 'spark'`**
- **Error:** `ModuleNotFoundError: No module named 'spark'`
- **Cause:** Python can't find the `spark` module because workspace isn't in PYTHONPATH
- **Solution:** Set PYTHONPATH when running:
  ```bash
  docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py"
  ```
- **Alternative:** Run from workspace directory and ensure PYTHONPATH includes workspace root

**Issue: Cannot find CSV file**
- **Solution:** Ensure you're running from `/workspace` directory (project root)
- **Verify:** `docker compose exec spark ls -la /workspace/data/raw/`

**Issue: Permission denied when writing Parquet**
- **Solution:** Check directory permissions in container
- **Verify:** `docker compose exec spark ls -la /workspace/data/`

**Issue: Out of memory errors**
- **Solution:** Add memory configuration to spark-submit:
  ```bash
  /opt/spark/bin/spark-submit \
    --master spark://localhost:7077 \
    --driver-memory 2g \
    --executor-memory 2g \
    spark/01_ingest_raw.py
  ```

### Key Learnings

1. **Schema Inference:** `inferSchema` is convenient but can be slow for large files. For production, define explicit schemas. ⚠️ **Updated in Step 4:** We now use strict schemas instead of inference.

2. **Parquet Format:** 
   - Creates multiple part files (one per partition/task)
   - Includes `_SUCCESS` marker file when write completes
   - Automatically compressed (snappy by default)

3. **Overwrite Mode:** 
   - `overwrite`: Replaces entire directory
   - `append`: Adds to existing data
   - `ignore`: Skips if output exists
   - `error` (default): Fails if output exists

4. **Spark UI:** 
   - Monitor job progress at `http://localhost:4040` while job runs
   - View stages, tasks, and execution timeline
   - Check for bottlenecks or errors

5. **Data Exploration:** 
   - Always inspect schema and sample data before processing
   - Helps identify data quality issues early
   - Informs downstream transformations

### Verification

After running the job, verify the output:

```bash
# Check Parquet files were created
docker compose exec spark ls -la /workspace/data/bronze/nyc_collisions/

# Verify _SUCCESS marker exists
docker compose exec spark ls /workspace/data/bronze/nyc_collisions/_SUCCESS

# Read back the Parquet to verify
docker compose exec spark /opt/spark/bin/spark-shell --master spark://localhost:7077
# Then in Spark shell:
# val df = spark.read.parquet("data/bronze/nyc_collisions")
# df.show(5)
# df.count()
```

### Next Steps

With Bronze layer complete:
- ✅ Raw CSV data is now standardized as Parquet
- ✅ Schema is preserved and types are enforced
- ✅ Data is ready for Silver layer transformations (cleaning, validation)
- ✅ Downstream jobs can read Parquet efficiently

## 🔒 Step 4: Schema Enforcement (Strict Typing)

This section documents the migration from schema inference to strict schema enforcement, a critical enterprise data engineering practice.

### Why Schema Enforcement Matters

**Enterprise pipelines don't "guess" types - they enforce contracts.**

**Problems with `inferSchema`:**
1. **Performance:** Spark must read the entire file to infer types (slow for large files)
2. **Inconsistency:** Different data samples can produce different inferred types
3. **Type Errors:** Can misidentify types (e.g., treating numeric strings as integers)
4. **No Contract:** No explicit agreement between data producers and consumers
5. **Production Risk:** Schema changes can break downstream jobs silently

**Benefits of Strict Schemas:**
1. **Performance:** Faster reads (no inference pass needed)
2. **Reliability:** Consistent types across all runs
3. **Data Quality:** Fails fast on type mismatches (catches bad data early)
4. **Documentation:** Schema serves as a contract and documentation
5. **Enterprise Standard:** Industry best practice for production pipelines

### Implementation

**Created: `spark/schemas.py`**

This module defines explicit schemas for all datasets. We define only the columns we care about (NYC dataset has many columns; we don't need all).

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

# Schema for NYC Motor Vehicle Collisions dataset
# Note: Column names match the CSV header exactly (with spaces)
# This enforces strict typing and prevents schema inference issues
NYC_COLLISIONS_SCHEMA = StructType([
    StructField("CRASH DATE", StringType(), True),
    StructField("CRASH TIME", StringType(), True),
    StructField("BOROUGH", StringType(), True),
    StructField("ZIP CODE", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("LOCATION", StringType(), True),

    StructField("NUMBER OF PERSONS INJURED", IntegerType(), True),
    StructField("NUMBER OF PERSONS KILLED", IntegerType(), True),
    StructField("NUMBER OF PEDESTRIANS INJURED", IntegerType(), True),
    StructField("NUMBER OF PEDESTRIANS KILLED", IntegerType(), True),
    StructField("NUMBER OF CYCLIST INJURED", IntegerType(), True),
    StructField("NUMBER OF CYCLIST KILLED", IntegerType(), True),
    StructField("NUMBER OF MOTORIST INJURED", IntegerType(), True),
    StructField("NUMBER OF MOTORIST KILLED", IntegerType(), True),

    StructField("CONTRIBUTING FACTOR VEHICLE 1", StringType(), True),
    StructField("CONTRIBUTING FACTOR VEHICLE 2", StringType(), True),
    StructField("VEHICLE TYPE CODE 1", StringType(), True),
    StructField("VEHICLE TYPE CODE 2", StringType(), True),
])
```

**Schema Components Explained:**

1. **StructType:** Root container for the schema
2. **StructField:** Individual column definition with:
   - **Name:** Must match CSV column name exactly (including spaces)
   - **DataType:** Explicit type (StringType, IntegerType, DoubleType)
   - **Nullable:** `True` = allows nulls, `False` = rejects nulls

3. **Data Types Used:**
   - `StringType()`: Text data (dates, times, names, codes)
   - `IntegerType()`: Whole numbers (counts, IDs)
   - `DoubleType()`: Decimal numbers (coordinates, measurements)

**Updated: `spark/01_ingest_raw.py`**

Changed from schema inference to explicit schema:

**Before:**
```python
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")  # ❌ Guessing types
      .csv(RAW_PATH))
```

**After:**
```python
from spark.schemas import NYC_COLLISIONS_SCHEMA

df = (spark.read
      .option("header", "true")
      .schema(NYC_COLLISIONS_SCHEMA)  # ✅ Explicit contract
      .csv(RAW_PATH))
```

### Key Changes

1. **Removed:** `option("inferSchema", "true")`
2. **Added:** `from spark.schemas import NYC_COLLISIONS_SCHEMA`
3. **Added:** `.schema(NYC_COLLISIONS_SCHEMA)` instead of inferSchema

### Column Name Matching

**Important:** Schema field names must match CSV column names exactly.

The NYC CSV uses column names with spaces:
- `"CRASH DATE"` (not `CRASH_DATE`)
- `"NUMBER OF PERSONS INJURED"` (not `NUMBER_OF_PERSONS_INJURED`)
- `"VEHICLE TYPE CODE 1"` (not `VEHICLE_TYPE_CODE1`)

When reading CSV with `header=true`, Spark uses the exact column names from the header. The schema must match these exactly, including:
- Spaces (not underscores)
- Case sensitivity
- Special characters

### Schema Selection Strategy

**Why only selected columns?**

The NYC dataset has 29 columns, but we only defined 20 in the schema:
- **Focus on business value:** Only include columns needed for analytics
- **Reduce complexity:** Fewer columns = simpler transformations
- **Performance:** Reading fewer columns is faster
- **Maintainability:** Easier to understand and maintain

**Columns included:**
- ✅ Location data (BOROUGH, ZIP CODE, LATITUDE, LONGITUDE)
- ✅ Temporal data (CRASH DATE, CRASH TIME)
- ✅ Injury/fatality counts (all NUMBER OF... columns)
- ✅ Contributing factors (CONTRIBUTING FACTOR VEHICLE 1, 2)
- ✅ Vehicle types (VEHICLE TYPE CODE 1, 2)

**Columns excluded:**
- Street names (ON STREET NAME, CROSS STREET NAME) - can be added later if needed
- Additional vehicles (VEHICLE TYPE CODE 3, 4, 5) - focus on primary vehicles
- Additional contributing factors (VEHICLE 3, 4, 5) - focus on primary factors

### Error Handling

**What happens if data doesn't match schema?**

1. **Type Mismatch:** 
   - If a string appears in an IntegerType field → Spark will try to cast it
   - If casting fails → Returns `null` (since nullable=True)
   - Example: `"N/A"` in NUMBER_OF_PERSONS_INJURED → becomes `null`

2. **Missing Columns:**
   - If CSV has columns not in schema → They are ignored (not read)
   - If schema has columns not in CSV → Returns `null` for those columns

3. **Extra Columns:**
   - CSV columns not in schema are simply not read
   - No error, just ignored

### Testing the Schema

**Run the ingestion job:**
```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --master spark://localhost:7077 \
  spark/01_ingest_raw.py
```

**Expected behavior:**
- ✅ Faster execution (no inference pass)
- ✅ Consistent schema across runs
- ✅ Only selected columns are read
- ✅ Type mismatches result in nulls (not errors)

**Verify schema in output:**
```bash
# Read back and check schema
docker compose exec spark /opt/spark/bin/spark-shell --master spark://localhost:7077
```

In Spark shell:
```scala
val df = spark.read.parquet("data/bronze/nyc_collisions")
df.printSchema()
// Should show only the 20 columns we defined, with correct types
```

### Troubleshooting

**Issue: Schema mismatch errors**
- **Error:** `AnalysisException: cannot resolve 'COLUMN_NAME'`
- **Cause:** Column name in schema doesn't match CSV header
- **Solution:** Check CSV header and update schema field names to match exactly

**Issue: All values are null for a column**
- **Cause:** Type mismatch - data can't be cast to schema type
- **Solution:** Check data format, may need to adjust schema type or handle nulls

**Issue: Import error for schemas module**
- **Error:** `ModuleNotFoundError: No module named 'spark'`
- **Cause:** Python path issue
- **Solution:** Ensure running from project root, or use absolute import path

**Issue: Performance not improved**
- **Cause:** May still be reading full file if other bottlenecks exist
- **Solution:** Check Spark UI for actual execution time, compare with/without schema

### Key Learnings

1. **Schema as Contract:** Explicit schemas document the expected data structure and serve as a contract between pipeline stages.

2. **Fail Fast:** Type mismatches are caught early, preventing downstream errors.

3. **Performance:** Explicit schemas eliminate the inference pass, making reads faster.

4. **Column Selection:** Only read columns you need - improves performance and reduces complexity.

5. **Name Matching:** Schema field names must match CSV column names exactly (spaces, case, special chars).

6. **Nullable Fields:** Setting `nullable=True` allows graceful handling of bad data (becomes null instead of failing).

7. **Enterprise Standard:** Production pipelines always use explicit schemas - no guessing allowed.

### Migration Checklist

- ✅ Created `spark/schemas.py` with explicit schema definition
- ✅ Updated `spark/01_ingest_raw.py` to use schema instead of inferSchema
- ✅ Removed `option("inferSchema", "true")`
- ✅ Added schema import
- ✅ Verified column names match CSV header exactly
- ✅ Tested ingestion job with new schema

### Next Steps

With strict schema enforcement:
- ✅ Pipeline now enforces data contracts
- ✅ Faster reads (no inference)
- ✅ Consistent types across runs
- ✅ Ready for production-like data quality checks
- ✅ Foundation for Silver layer transformations with validated data

## ✨ Step 5: Silver Layer - Clean & Normalize Data

This section documents the creation of the Silver layer, where raw data is cleaned, normalized, and made analytics-ready. This is where most real data engineering time goes.

### Why Silver Layer Matters

**Silver = "Analytics-ready clean data"**

The Silver layer transforms Bronze data into a format that's:
- **Clean:** Invalid data filtered out
- **Normalized:** Consistent formats (dates parsed, strings trimmed, nulls standardized)
- **Validated:** Data quality checks applied
- **Structured:** Column names standardized, IDs generated
- **Ready for Analytics:** Can be directly queried for business insights

**Key Differences from Bronze:**
- **Bronze:** Raw data standardized to Parquet (format conversion)
- **Silver:** Cleaned, validated, normalized data (quality + structure)

### Implementation

**Created: `spark/02_clean_silver.py`**

This script performs comprehensive data cleaning and normalization:

**1. String Normalization:**
- Trims whitespace from string columns
- Converts empty strings to `null` (consistent null handling)
- Applied to: BOROUGH, ZIP CODE, contributing factors, vehicle types

**2. Date/Time Parsing:**
- Parses `CRASH DATE` from "MM/dd/yyyy" format to DateType
- Parses `CRASH TIME` from "HH:mm" format to TimestampType
- Enables time-based analytics (grouping by date, filtering by time range)

**3. Data Quality Filters:**
- **Valid Dates:** Filters out rows with unparseable dates
- **Valid Coordinates:** Filters to NYC bounds (lat: 40.3-41.1, lon: -74.5 to -73.4)
- **Non-null Coordinates:** Ensures location data exists

**4. ID Generation:**
- Creates stable `collision_id` using SHA256 hash
- Hash based on: date + time + latitude + longitude
- Ensures uniqueness and reproducibility

**5. Column Renaming:**
- Converts to lowercase with underscores (snake_case)
- Makes column names consistent and SQL-friendly
- Example: `"NUMBER OF PERSONS INJURED"` → `persons_injured`

### Script Details

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark.schemas import NYC_COLLISIONS_SCHEMA

def to_null_if_blank(col):
    """Convert empty strings and whitespace-only strings to null"""
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(F.trim(col))

# Main transformations:
# 1. Normalize strings (trim + null empty)
# 2. Parse dates/times
# 3. Filter invalid data
# 4. Generate collision_id
# 5. Rename columns
# 6. Write to Silver layer
```

### Transformations Applied

**1. String Normalization:**
```python
.withColumn("BOROUGH", to_null_if_blank(F.col("BOROUGH")))
.withColumn("ZIP CODE", to_null_if_blank(F.col("ZIP CODE")))
# ... etc
```
- Trims whitespace: `"  Brooklyn  "` → `"Brooklyn"`
- Converts empty to null: `""` → `null`
- Consistent null handling across all string fields

**2. Date/Time Parsing:**
```python
.withColumn("crash_date", F.to_date("CRASH DATE", "MM/dd/yyyy"))
.withColumn("crash_time", F.to_timestamp("CRASH TIME", "HH:mm"))
```
- **Input:** `"09/11/2021"` → **Output:** `2021-09-11` (DateType)
- **Input:** `"2:39"` → **Output:** `1970-01-01 02:39:00` (TimestampType)
- Enables date arithmetic and time-based filtering

**3. Data Quality Filters:**
```python
# Filter invalid dates
df = df.filter(F.col("crash_date").isNotNull())

# Filter invalid coordinates (NYC bounds)
df = df.filter(
    (F.col("LATITUDE").isNotNull()) &
    (F.col("LONGITUDE").isNotNull()) &
    (F.col("LATITUDE").between(40.3, 41.1)) &
    (F.col("LONGITUDE").between(-74.5, -73.4))
)
```
- **Date Filter:** Removes rows with unparseable dates
- **Coordinate Filter:** Only keeps rows within NYC geographic bounds
- **Null Filter:** Ensures coordinates exist

**4. ID Generation:**
```python
df = df.withColumn("collision_id",
    F.sha2(F.concat_ws("|",
        F.col("CRASH DATE"),
        F.col("CRASH TIME"),
        F.col("LATITUDE").cast("string"),
        F.col("LONGITUDE").cast("string")), 256))
```
- Creates unique ID from: `date|time|lat|lon`
- Uses SHA256 hash for deterministic uniqueness
- Example: `"09/11/2021|2:39|40.62179|-73.970024"` → hash → `collision_id`

**5. Column Selection & Renaming:**
```python
.select(
    "collision_id",
    "crash_date",
    F.col("CRASH TIME").alias("crash_time_str"),
    F.col("BOROUGH").alias("borough"),
    F.col("NUMBER OF PERSONS INJURED").alias("persons_injured"),
    # ... etc
)
```
- Selects only needed columns
- Renames to snake_case for consistency
- Creates clean, SQL-friendly schema

### Directory Structure

```
data/
├── raw/                    # Original CSV files
├── bronze/                # Standardized Parquet (format conversion)
│   └── nyc_collisions/
└── silver/                # Cleaned, normalized Parquet (quality + structure)
    └── collisions/
        ├── part-00000-*.parquet
        └── _SUCCESS
```

### Running the Silver Cleaning Job

**Command:**
```bash
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/02_clean_silver.py"
```

**Expected Output:**
```
=== Processing ===
[Spark job execution logs]

Wrote silver parquet to: data/silver/collisions
Rows written: [number]
```

**What Happens:**
1. Reads raw CSV with strict schema
2. Applies all transformations (normalize, parse, filter)
3. Writes cleaned data to `data/silver/collisions/`
4. Prints row count for verification

### Data Quality Improvements

**Before (Bronze):**
- Empty strings: `""`, `"   "`, `null` (inconsistent)
- Dates: String format `"09/11/2021"` (can't do date math)
- Coordinates: May include invalid values outside NYC
- Column names: Mixed case with spaces (hard to query)

**After (Silver):**
- Empty strings: All converted to `null` (consistent)
- Dates: Proper DateType `2021-09-11` (can do date math)
- Coordinates: Only valid NYC coordinates (40.3-41.1 lat, -74.5 to -73.4 lon)
- Column names: Clean snake_case `persons_injured` (SQL-friendly)
- IDs: Stable collision_id for deduplication and joins

### Verification

**Check Silver output:**
```bash
# List Parquet files
docker compose exec spark ls -la /workspace/data/silver/collisions/

# Verify _SUCCESS marker
docker compose exec spark ls /workspace/data/silver/collisions/_SUCCESS

# Read and inspect in Spark shell
docker compose exec spark /opt/spark/bin/spark-shell --master spark://localhost:7077
```

In Spark shell:
```scala
val df = spark.read.parquet("data/silver/collisions")
df.printSchema()
// Should show clean column names: collision_id, crash_date, borough, etc.

df.show(10)
// Should show cleaned data with proper types

df.filter($"crash_date".isNotNull).count()
// Should match total count (all dates valid)

df.filter($"latitude".between(40.3, 41.1)).count()
// Should match total count (all coordinates valid)
```

### Troubleshooting

**Issue: Date parsing fails for some rows**
- **Error:** Some dates become null after parsing
- **Cause:** Date format doesn't match expected "MM/dd/yyyy"
- **Solution:** Check actual date formats in data, adjust format string if needed
- **Example:** If dates are "yyyy-MM-dd", use `F.to_date("CRASH DATE", "yyyy-MM-dd")`

**Issue: All rows filtered out by coordinate bounds**
- **Cause:** Coordinate bounds too restrictive or data uses different coordinate system
- **Solution:** Check actual coordinate ranges in data, adjust bounds if needed
- **Verify:** `df.select(F.min("LATITUDE"), F.max("LATITUDE"), F.min("LONGITUDE"), F.max("LONGITUDE")).show()`

**Issue: collision_id not unique**
- **Cause:** Multiple collisions at same location/time (rare but possible)
- **Solution:** Add more fields to hash (e.g., borough, vehicle types) or use row_number()
- **Check:** `df.groupBy("collision_id").count().filter($"count" > 1).show()`

**Issue: Memory errors during filtering**
- **Cause:** Large dataset, complex filters
- **Solution:** Add partitioning or increase Spark memory:
  ```bash
  /opt/spark/bin/spark-submit \
    --master spark://localhost:7077 \
    --driver-memory 4g \
    --executor-memory 4g \
    spark/02_clean_silver.py
  ```

**Issue: Column name errors after renaming**
- **Error:** `AnalysisException: cannot resolve 'COLUMN_NAME'`
- **Cause:** Column name in select doesn't match actual column name (case/space sensitive)
- **Solution:** Verify column names match schema exactly (with spaces)

### Key Learnings

1. **Data Cleaning is Iterative:** Start with basic cleaning, add more filters as you discover data quality issues.

2. **Null Handling:** Standardize null representation early (empty strings → null) to avoid downstream issues.

3. **Date Parsing:** Always validate date formats before parsing. Different sources may use different formats.

4. **Geographic Filtering:** Use domain knowledge (NYC bounds) to filter invalid coordinates.

5. **ID Generation:** Hash-based IDs are deterministic and reproducible, good for deduplication.

6. **Column Naming:** Consistent naming (snake_case) makes SQL queries easier and reduces errors.

7. **Filter Order Matters:** Filter invalid data early to reduce processing of bad rows.

8. **Data Quality Metrics:** Track how many rows are filtered at each step (validates cleaning logic).

### Silver Layer Benefits

- ✅ **Analytics-Ready:** Data can be directly queried for insights
- ✅ **Type Safety:** Dates are DateType (not strings), enabling date math
- ✅ **Data Quality:** Invalid data filtered out (bad dates, invalid coordinates)
- ✅ **Consistency:** Standardized nulls, trimmed strings, clean column names
- ✅ **Performance:** Filtered data is smaller, queries run faster
- ✅ **Stable IDs:** collision_id enables joins and deduplication

### Next Steps

With Silver layer complete:
- ✅ Data is cleaned and normalized
- ✅ Dates are parsed and queryable
- ✅ Invalid data is filtered
- ✅ Column names are consistent
- ✅ Ready for Gold layer (fact/dimension tables) or direct analytics

## ⭐ Step 6: Gold Layer - Star Schema (Dimensions + Fact)

This section documents the creation of the Gold layer using a star schema design pattern, which is the industry standard for analytical data warehouses.

### Why Star Schema Matters

**Gold tables are business-ready:**
- **Dimensions:** Clean lookup tables with descriptive attributes
- **Fact:** Transactional data that joins to dimensions by keys
- **Ready for Tableau/Power BI:** Optimized structure for BI tools
- **Fast Queries:** Star schema enables efficient analytical queries
- **Industry Standard:** Most common data warehouse design pattern

**Why Star Schema for ICBC:**
- ICBC explicitly mentions data modeling and source-to-target mappings
- Star schema is the standard for insurance/claims analytics
- Enables complex SQL queries for business insights
- Supports dimensional analysis (time, location, vehicle, factor)

### Star Schema Design

**Key Concept: Surrogate Keys**
- Integer keys (1, 2, 3, ...) for each dimension row
- Stable, never-changing identifiers
- Enable efficient joins and indexing
- Separate from business keys (dates, codes, etc.)

**Structure:**
```
fact_collisions (fact table)
    ├── time_key → dim_time
    ├── location_key → dim_location
    ├── primary_vehicle_key → dim_vehicle
    ├── secondary_vehicle_key → dim_vehicle
    ├── primary_factor_key → dim_factor
    └── secondary_factor_key → dim_factor
```

### Implementation

**Created: `spark/03_build_gold.py`**

This script builds a complete star schema:

**1. dim_time (Time Dimension)**
- **Source:** `crash_date` + `crash_time_str` from Silver
- **Attributes:**
  - `time_key` (surrogate key)
  - `year`, `month`, `day`
  - `day_of_week` (1=Sunday, 7=Saturday)
  - `day_name` (e.g., "Monday")
  - `month_name` (e.g., "January")
  - `quarter` (1-4)
  - `hour` (0-23, extracted from time string)
  - `time_category` (e.g., "06:00-08:59", "15:00-17:59")
  - `is_weekend` (boolean)
- **Output:** `data/gold/dim_time/`

**2. dim_location (Location Dimension)**
- **Source:** Unique combinations of `borough`, `zip_code`, `latitude`, `longitude`
- **Attributes:**
  - `location_key` (surrogate key)
  - `borough`
  - `zip_code`
  - `latitude`
  - `longitude`
  - `location_name` (e.g., "Brooklyn, 11230")
- **Output:** `data/gold/dim_location/`

**3. dim_vehicle (Vehicle Type Dimension)**
- **Source:** Unique vehicle types from `vehicle_type_code_1` and `vehicle_type_code_2`
- **Attributes:**
  - `vehicle_key` (surrogate key)
  - `vehicle_type_code` (e.g., "Sedan", "Pick-up Truck")
  - `vehicle_type_name` (normalized/categorized name)
- **Output:** `data/gold/dim_vehicle/`

**4. dim_factor (Contributing Factor Dimension)**
- **Source:** Unique contributing factors from `contributing_factor_vehicle_1` and `contributing_factor_vehicle_2`
- **Attributes:**
  - `factor_key` (surrogate key)
  - `contributing_factor` (e.g., "Aggressive Driving/Road Rage")
  - `factor_category` (e.g., "Driver Behavior", "Road/Weather")
- **Output:** `data/gold/dim_factor/`

**5. fact_collisions (Fact Table)**
- **Source:** Silver layer joined to all dimensions
- **Attributes:**
  - `collision_id` (business key)
  - `time_key` (FK → dim_time)
  - `location_key` (FK → dim_location)
  - `primary_vehicle_key` (FK → dim_vehicle)
  - `secondary_vehicle_key` (FK → dim_vehicle)
  - `primary_factor_key` (FK → dim_factor)
  - `secondary_factor_key` (FK → dim_factor)
  - **Measures:**
    - `persons_injured`, `persons_killed`
    - `pedestrians_injured`, `pedestrians_killed`
    - `cyclist_injured`, `cyclist_killed`
    - `motorist_injured`, `motorist_killed`
    - `total_persons_affected` (derived)
    - `severity_category` (derived: "Fatal", "Injury", "Property Damage Only")
- **Output:** `data/gold/fact_collisions/`

### Key Implementation Details

**Surrogate Key Generation:**
```python
window = Window.orderBy("crash_date", "crash_time_str")
dim_time = dim_time.withColumn("time_key", F.row_number().over(window))
```
- Uses `row_number()` with `Window.orderBy()` for deterministic keys
- Keys are stable across runs (same order = same keys)

**Dimension Joins:**
```python
fact_df = (silver_df
          .join(dim_time, 
               (silver_df["crash_date"] == dim_time["crash_date"]) &
               (silver_df["crash_time_str"] == dim_time["crash_time_str"]),
               "left")
          # ... more joins
)
```
- Left joins ensure all fact rows are preserved
- Joins on business keys (date, location, codes)
- Maps to surrogate keys for fact table

**Time Category Logic:**
```python
.withColumn("time_category",
           F.when((F.col("hour") >= 0) & (F.col("hour") < 6), "00:00-05:59")
           .when((F.col("hour") >= 6) & (F.col("hour") < 9), "06:00-08:59")
           # ... etc
)
```
- Groups hours into meaningful time periods
- Enables time-based analysis (rush hour, night, etc.)

### Running the Gold Layer Build

**Command:**
```bash
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/03_build_gold.py"
```

**Expected Output:**
```
Reading Silver layer...
Silver rows: [count]

=== Building dim_time ===
dim_time rows: [count]

=== Building dim_location ===
dim_location rows: [count]

=== Building dim_vehicle ===
dim_vehicle rows: [count]

=== Building dim_factor ===
dim_factor rows: [count]

=== Building fact_collisions ===
fact_collisions rows: [count]

=== Gold layer complete ===
```

### Directory Structure

```
data/gold/
├── dim_time/
│   └── part-*.parquet
├── dim_location/
│   └── part-*.parquet
├── dim_vehicle/
│   └── part-*.parquet
├── dim_factor/
│   └── part-*.parquet
└── fact_collisions/
    └── part-*.parquet
```

### Verification

**Check Gold tables:**
```bash
# List all dimension and fact tables
docker compose exec spark ls -la /workspace/data/gold/

# Verify each table
docker compose exec spark /opt/spark/bin/spark-shell --master spark://localhost:7077
```

In Spark shell:
```scala
// Check dimensions
val dimTime = spark.read.parquet("data/gold/dim_time")
dimTime.show(10)

val dimLocation = spark.read.parquet("data/gold/dim_location")
dimLocation.show(10)

// Check fact table
val fact = spark.read.parquet("data/gold/fact_collisions")
fact.show(10)

// Verify joins work
val joined = fact
  .join(dimTime, "time_key", "left")
  .join(dimLocation, "location_key", "left")
  .select("collision_id", "year", "month_name", "borough", "persons_injured")
joined.show(10)
```

### Key Learnings

1. **Star Schema Benefits:**
   - Separates descriptive attributes (dimensions) from measures (facts)
   - Enables efficient queries (small dimensions, large facts)
   - Easy to understand and maintain

2. **Surrogate Keys:**
   - Integer keys are faster to join than strings
   - Stable across data refreshes
   - Enable efficient indexing

3. **Dimension Design:**
   - One row per unique business key combination
   - Include all descriptive attributes needed for analysis
   - Keep dimensions small (denormalized is OK for dimensions)

4. **Fact Table Design:**
   - One row per business event (collision)
   - Contains foreign keys to dimensions
   - Contains measures (counts, amounts, etc.)
   - Can be very large (millions/billions of rows)

5. **Join Strategy:**
   - Use left joins to preserve all fact rows
   - Join on business keys, then select surrogate keys
   - Handle nulls gracefully (some collisions may not have all dimensions)

## 🏢 Step 7: ICBC TAS Reference Integration

This section documents the creation of an ICBC-aligned view that matches ICBC TAS (Traffic Accident System) naming conventions and structure.

### Why ICBC Alignment Matters

**This is your "ICBC alignment" - you'll speak their language:**
- **Field Naming:** Match ICBC TAS field names (e.g., `PERSON_INJURY_CNT` instead of `persons_injured`)
- **Code Mappings:** Use ICBC code tables for standardized classifications
- **Business Context:** Understand ICBC-specific terminology and classifications
- **Resume Value:** Demonstrates ability to work with enterprise insurance data standards

### ICBC TAS Overview

**TAS (Traffic Accident System):** ICBC's standardized system for recording and classifying traffic collisions.

**Key ICBC TAS Naming Conventions:**
- Uppercase with underscores: `ACCIDENT_NUMBER`, `PERSON_INJURY_CNT`
- Suffixes: `_CNT` for counts, `_CD` for codes, `_DT` for dates
- Descriptive names: `ACCIDENT_DATE`, `ACCIDENT_TIME`, `ACCIDENT_LOCATION`

### Implementation

**Created: `spark/04_build_icbc_view.py`**

This script creates an ICBC-aligned view by:
1. Joining Gold fact and dimension tables
2. Renaming columns to match ICBC TAS conventions
3. Adding ICBC-specific fields (even if NULL for NYC data)
4. Creating standardized severity classifications

**ICBC Field Mappings:**

| Original Field | ICBC TAS Field | Description |
|---------------|----------------|-------------|
| `collision_id` | `ACCIDENT_NUMBER` | Unique collision identifier |
| `crash_date` | `ACCIDENT_DATE` | Date of collision |
| `year` | `YEAR` | Year of collision |
| `month_name` | `MONTH_NAME` | Full month name |
| `day_of_week` | `ACC_DAY_OF_WEEK` | Day of week (1-7) |
| `hour` | `ACCIDENT_TIME` | Hour of collision (0-23) |
| `time_category` | `TIME_CATEGORY` | Time period grouping |
| `borough` | `BOROUGH` | Borough name |
| `latitude` | `LATITUDE` | Latitude coordinate |
| `longitude` | `LONGITUDE` | Longitude coordinate |
| `persons_injured` | `PERSON_INJURY_CNT` | Count of persons injured |
| `persons_killed` | `PERSON_FATALITY_CNT` | Count of persons killed |
| `severity_category` | `ACC_SEVERITY_CD` | Severity code (Fatal/Injury/PDO) |
| `severity_category` | `ACCIDENT_TYPE` | Accident type classification |

**ICBC-Specific Fields (Placeholders):**
- `ROAD_CONDITION_CD`: Road condition code (NULL for NYC data)
- `WEATHER_CONDITION_CD`: Weather condition code (NULL for NYC data)
- `ROAD_SURFACE_CD`: Road surface code (NULL for NYC data)

*Note: NYC data doesn't include road/weather conditions. These fields are included as placeholders for future enrichment (e.g., weather API integration).*

### ICBC View Structure

**Output:** `data/gold/icbc_view/`

**Key Features:**
- All columns use ICBC TAS naming conventions
- Includes both original and ICBC field names where appropriate
- Maintains dimension keys for joins
- Includes derived fields (severity, accident type)
- Placeholder fields for missing data (road/weather conditions)

### Running the ICBC View Build

**Command:**
```bash
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/04_build_icbc_view.py"
```

**Expected Output:**
```
Reading Gold layer tables...
Building ICBC-aligned view...
ICBC-aligned view written to: data/gold/icbc_view
Rows: [count]

=== ICBC View Schema ===
[Schema with ICBC field names]

=== Sample ICBC View Data ===
[Sample rows with ICBC naming]
```

### ICBC Reference Files

**Location:** `docs/icbc_tas_reference/`

**Files:**
- `tas_e02_codes.csv`: ICBC code mappings and descriptions
- `tas_e02_metadata.csv`: ICBC TAS metadata definitions

**Usage:**
- Reference for understanding ICBC field definitions
- Can be used to build lookup tables for code mappings
- Documents ICBC-specific classifications and terminology

### Verification

**Check ICBC view:**
```bash
docker compose exec spark /opt/spark/bin/spark-shell --master spark://localhost:7077
```

In Spark shell:
```scala
val icbc = spark.read.parquet("data/gold/icbc_view")
icbc.printSchema()
// Should show ICBC field names: ACCIDENT_NUMBER, PERSON_INJURY_CNT, etc.

icbc.select("ACCIDENT_NUMBER", "ACCIDENT_DATE", "PERSON_INJURY_CNT", "ACC_SEVERITY_CD").show(10)

// Verify ICBC naming conventions
icbc.columns.filter(_.contains("CNT"))  // Count fields
icbc.columns.filter(_.contains("CD"))   // Code fields
```

### Key Learnings

1. **Enterprise Alignment:**
   - Matching industry naming conventions demonstrates professionalism
   - Makes data more accessible to business users familiar with ICBC standards
   - Enables easier integration with ICBC systems (if needed)

2. **Field Naming Standards:**
   - Uppercase with underscores is common in enterprise systems
   - Suffixes (`_CNT`, `_CD`, `_DT`) provide semantic meaning
   - Consistent naming improves data discoverability

3. **Placeholder Fields:**
   - Include fields even if data isn't available (NULL values)
   - Enables future enrichment without schema changes
   - Maintains compatibility with ICBC TAS structure

4. **Code Mappings:**
   - Use reference files to understand code meanings
   - Can build lookup tables for code-to-description mappings
   - Enables standardized reporting and analysis

5. **Resume Value:**
   - Demonstrates understanding of enterprise data standards
   - Shows ability to work with insurance/claims data
   - Highlights data modeling and transformation skills

### Next Steps

With Gold layer and ICBC view complete:
- ✅ Star schema implemented (dimensions + fact)
- ✅ Surrogate keys for efficient joins
- ✅ ICBC-aligned view with TAS naming
- ✅ Ready for analytics and reporting
- ✅ Compatible with BI tools (Tableau, Power BI)
- ✅ Foundation for complex SQL queries

## 💰 Step 8: Generate Synthetic Claims Data

This section documents the creation of synthetic insurance claims data linked to collisions, making the project ICBC-specific and insurance-focused.

### Why Claims Data Matters for ICBC

**ICBC is insurance + claims heavy:**
- ICBC is an insurance company - claims are their core business
- This makes your project feel ICBC-specific and relevant
- Enables insurance-specific analytics:
  - "Claim amount by borough"
  - "Severity vs claim amount"
  - "Average days to close by claim type"
  - "Total claim costs by vehicle type"

**Business Value:**
- Demonstrates understanding of insurance domain
- Shows ability to work with claims data
- Enables financial analysis (claim costs, reserves, etc.)
- Supports actuarial and risk analysis

### Implementation

**Created: `spark/05_generate_claims.py`**

This script generates realistic synthetic claims data:

**1. Claim Generation Logic:**
- **Not all collisions result in claims** - realistic probability distribution
- **Higher probability for severe collisions:**
  - Fatal collisions: 100% claim rate
  - Injury collisions: 85% claim rate
  - Property damage: 30% claim rate

**2. Claim Attributes:**

**claim_id:**
- UUID format for uniqueness
- Example: `"a1b2c3d4-e5f6-7890-abcd-ef1234567890"`

**collision_id:**
- Foreign key linking to `fact_collisions`
- Enables joins between collisions and claims

**claim_amount:**
- Correlated with collision severity
- **Fatal collisions:** $50k base + $200k per fatality
- **Injury collisions:** $10k base + $15k per injury
- **Property damage:** $3k-$10k random
- **Variance:** 0.7x to 1.5x multiplier for realism

**claim_status:**
- `OPEN`: 35% of claims (active, not yet resolved)
- `CLOSED`: 65% of claims (resolved, settled)

**days_to_close:**
- Only for `CLOSED` claims (NULL for OPEN)
- Correlated with severity:
  - **Fatal:** 180-545 days (complex cases)
  - **Injury:** 60-240 days (medical evaluation)
  - **Large claims (>$50k):** 90-210 days
  - **Small claims:** 30-90 days

### Script Details

```python
# Claim generation with probability based on severity
claims_df = (fact_collisions
            .withColumn("has_claim",
                       F.when(F.col("persons_killed") > 0, True)  # Always claim if fatal
                       .when(F.col("persons_injured") > 0, F.rand() < 0.85)  # 85% if injury
                       .otherwise(F.rand() < 0.30))  # 30% for property damage
            .filter(F.col("has_claim") == True)
            
            # Generate claim amounts correlated with severity
            .withColumn("claim_amount", ...)
            
            # Generate status and days to close
            .withColumn("claim_status", ...)
            .withColumn("days_to_close", ...)
)
```

### Claim Amount Calculation

**Base Amounts:**
- Fatal: $50,000 + ($200,000 × fatalities)
- Injury: $10,000 + ($15,000 × injuries)
- Property: $3,000 - $10,000 (random)

**Variance:**
- Multiplier: 0.7x to 1.5x (adds realism)
- Final amount rounded to 2 decimal places

**Example Calculations:**
- 1 fatality: $50k + $200k = $250k base → $175k-$375k after variance
- 2 injuries: $10k + $30k = $40k base → $28k-$60k after variance
- Property damage: $3k-$10k random

### Output Structure

**Output:** `data/gold/fact_claims/`

**Schema:**
```
fact_claims:
  - claim_id (string, UUID)
  - collision_id (string, FK to fact_collisions)
  - claim_amount (double, rounded to 2 decimals)
  - claim_status (string: "OPEN" or "CLOSED")
  - days_to_close (integer, NULL for OPEN claims)
  - persons_injured (integer, from collision)
  - persons_killed (integer, from collision)
  - total_persons_affected (integer, from collision)
  - severity_category (string, from collision)
```

### Running the Claims Generation

**Command:**
```bash
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/05_generate_claims.py"
```

**Expected Output:**
```
Reading Gold fact_collisions...
Total collisions: [count]

=== Generating synthetic claims ===

=== Claims Generation Complete ===
Total claims generated: [count]
Claims per collision: [ratio]

=== Claims Summary Statistics ===
[Summary stats: total, open, closed, avg amount, etc.]

=== Claims by Severity ===
[Breakdown by severity category]

Claims written to: data/gold/fact_claims
```

### Analytics Enabled

With claims data, you can now perform insurance-specific analytics:

**1. Claim Amount by Borough:**
```sql
SELECT 
    l.borough,
    COUNT(c.claim_id) as claim_count,
    AVG(c.claim_amount) as avg_claim_amount,
    SUM(c.claim_amount) as total_claim_amount
FROM fact_claims c
JOIN fact_collisions col ON c.collision_id = col.collision_id
JOIN dim_location l ON col.location_key = l.location_key
GROUP BY l.borough
ORDER BY total_claim_amount DESC;
```

**2. Severity vs Claim Amount:**
```sql
SELECT 
    col.severity_category,
    COUNT(c.claim_id) as claim_count,
    AVG(c.claim_amount) as avg_claim_amount,
    MIN(c.claim_amount) as min_claim_amount,
    MAX(c.claim_amount) as max_claim_amount
FROM fact_claims c
JOIN fact_collisions col ON c.collision_id = col.collision_id
GROUP BY col.severity_category
ORDER BY avg_claim_amount DESC;
```

**3. Days to Close Analysis:**
```sql
SELECT 
    c.claim_status,
    AVG(c.days_to_close) as avg_days_to_close,
    MIN(c.days_to_close) as min_days,
    MAX(c.days_to_close) as max_days,
    COUNT(*) as claim_count
FROM fact_claims c
WHERE c.claim_status = 'CLOSED'
GROUP BY c.claim_status;
```

**4. Claim Costs by Vehicle Type:**
```sql
SELECT 
    v.vehicle_type_name,
    COUNT(c.claim_id) as claim_count,
    AVG(c.claim_amount) as avg_claim_amount,
    SUM(c.claim_amount) as total_claim_amount
FROM fact_claims c
JOIN fact_collisions col ON c.collision_id = col.collision_id
JOIN dim_vehicle v ON col.primary_vehicle_key = v.vehicle_key
GROUP BY v.vehicle_type_name
ORDER BY total_claim_amount DESC;
```

### Verification

**Check claims data:**
```bash
docker compose exec spark /opt/spark/bin/spark-shell --master spark://localhost:7077
```

In Spark shell:
```scala
val claims = spark.read.parquet("data/gold/fact_claims")
claims.printSchema()
claims.show(20)

// Verify claim amounts are reasonable
claims.select(
  F.min("claim_amount"),
  F.max("claim_amount"),
  F.avg("claim_amount"),
  F.count("*")
).show()

// Check claim status distribution
claims.groupBy("claim_status").count().show()

// Verify joins work
val joined = claims
  .join(spark.read.parquet("data/gold/fact_collisions"), "collision_id", "left")
  .select("claim_id", "collision_id", "claim_amount", "severity_category", "persons_injured")
joined.show(10)
```

### Key Learnings

1. **Synthetic Data Generation:**
   - Use realistic probability distributions
   - Correlate synthetic fields with real data (severity → claim amount)
   - Add variance for realism (not all claims are identical)

2. **Insurance Domain Knowledge:**
   - Fatal claims are most expensive and take longest to close
   - Not all collisions result in claims
   - Claim amounts correlate with severity
   - Open vs closed status is important for financial reporting

3. **Data Relationships:**
   - Claims link to collisions via `collision_id`
   - Enables joins for multi-fact analysis
   - Supports both collision and claims analytics

4. **Business Value:**
   - Enables financial analysis (total claim costs, reserves)
   - Supports actuarial analysis (risk assessment)
   - Demonstrates insurance domain expertise

5. **Resume Value:**
   - Shows understanding of insurance business
   - Demonstrates ability to generate realistic synthetic data
   - Highlights financial/actuarial analysis capabilities

### Next Steps

With claims data complete:
- ✅ Synthetic claims linked to collisions
- ✅ Realistic claim amounts and statuses
- ✅ Enables insurance-specific analytics
- ✅ Supports financial and actuarial analysis
- ✅ Makes project ICBC-relevant and insurance-focused
- ✅ Ready for advanced SQL queries combining collisions and claims

## 📊 Step 9: SQL Analytics Queries (Spark SQL)

This section documents the creation of comprehensive SQL analytics queries that demonstrate strong SQL skills, which is a key requirement for ICBC positions.

### Why SQL Analytics Matters

**ICBC job wants strong SQL - this proves it:**
- Demonstrates ability to write complex analytical queries
- Shows understanding of star schema joins
- Proves capability to extract business insights from data
- Essential skill for data engineering and analytics roles

**Business Value:**
- Enables self-service analytics
- Supports business intelligence and reporting
- Provides answers to key business questions
- Foundation for dashboards and visualizations

### Implementation

**Created: `sql/analytics.sql`**

Comprehensive SQL queries covering:

**1. Top Hotspots (Location Analysis)**
- Locations with highest collision frequency
- Breakdown by borough and zip code
- Includes injury/fatality statistics

**2. Worst Hours (Time Distribution)**
- Hours with highest collision frequency
- Time category analysis
- Day of week patterns

**3. Trend Over Time (Monthly Analysis)**
- Month-over-month trends
- Year-over-year comparisons
- Seasonal patterns

**4. Claim Amount by Borough (Financial Analysis)**
- Total and average claim amounts by location
- Severity-based claim analysis
- Financial impact by geography

**5. Contributing Factors Ranking**
- Most common contributing factors
- Factor category analysis
- Correlation with severity

**6. Vehicle Type Analysis**
- Collisions by vehicle type
- Claim costs by vehicle type
- Risk assessment by vehicle category

**7. Severity Analysis**
- Severity distribution
- Severity by time of day
- Percentage breakdowns

**8. Claims Analysis**
- Claim status distribution
- Days to close analysis
- Claim amounts by severity

**9. Weekend vs Weekday Analysis**
- Comparison of collision patterns
- Severity differences

**10. Comprehensive Dashboard Query**
- Summary statistics
- Key metrics for dashboards

### Running SQL Queries

**Method 1: Using Spark SQL Runner Script**

**Created: `spark/run_sql.py`**

This script:
- Registers Gold tables as Spark SQL temporary views
- Reads SQL queries from `sql/analytics.sql`
- Executes each query and displays results

**Command:**
```bash
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/run_sql.py sql/analytics.sql"
```

**Method 2: Using Spark SQL Shell**

```bash
# Start Spark SQL shell
docker compose exec spark /opt/spark/bin/spark-sql --master spark://localhost:7077

# Register tables
CREATE TEMPORARY VIEW fact_collisions AS SELECT * FROM parquet.`data/gold/fact_collisions`;
CREATE TEMPORARY VIEW fact_claims AS SELECT * FROM parquet.`data/gold/fact_claims`;
CREATE TEMPORARY VIEW dim_time AS SELECT * FROM parquet.`data/gold/dim_time`;
CREATE TEMPORARY VIEW dim_location AS SELECT * FROM parquet.`data/gold/dim_location`;
CREATE TEMPORARY VIEW dim_vehicle AS SELECT * FROM parquet.`data/gold/dim_vehicle`;
CREATE TEMPORARY VIEW dim_factor AS SELECT * FROM parquet.`data/gold/dim_factor`;

# Run queries from sql/analytics.sql
```

**Method 3: Using Spark Shell (Scala/Python)**

```bash
docker compose exec spark /opt/spark/bin/spark-shell --master spark://localhost:7077
```

Then in Spark shell:
```scala
// Register tables
val factCollisions = spark.read.parquet("data/gold/fact_collisions")
factCollisions.createOrReplaceTempView("fact_collisions")
// ... register other tables

// Run SQL
spark.sql("SELECT * FROM fact_collisions LIMIT 10").show()
```

### Example Queries

**Top Hotspots:**
```sql
SELECT 
    l.borough,
    COUNT(fc.collision_id) as collision_count,
    SUM(fc.persons_injured) as total_injured
FROM fact_collisions fc
JOIN dim_location l ON fc.location_key = l.location_key
GROUP BY l.borough
ORDER BY collision_count DESC;
```

**Claim Amount by Borough:**
```sql
SELECT 
    l.borough,
    COUNT(claims.claim_id) as claim_count,
    AVG(claims.claim_amount) as avg_claim_amount,
    SUM(claims.claim_amount) as total_claim_amount
FROM fact_collisions fc
JOIN dim_location l ON fc.location_key = l.location_key
JOIN fact_claims claims ON fc.collision_id = claims.collision_id
GROUP BY l.borough
ORDER BY total_claim_amount DESC;
```

### Key SQL Skills Demonstrated

1. **Complex Joins:**
   - Star schema joins (fact to multiple dimensions)
   - Left joins for optional relationships
   - Self-joins for comparisons

2. **Aggregations:**
   - COUNT, SUM, AVG, MIN, MAX
   - GROUP BY with multiple columns
   - Window functions (if needed)

3. **Filtering:**
   - WHERE clauses
   - HAVING clauses
   - NULL handling

4. **Analytical Functions:**
   - Subqueries
   - CASE statements
   - UNION operations

5. **Business Logic:**
   - Percentage calculations
   - Ranking and ordering
   - Conditional aggregations

### Next Steps

With SQL analytics complete:
- ✅ Comprehensive SQL queries for business insights
- ✅ Demonstrates strong SQL skills
- ✅ Enables self-service analytics
- ✅ Foundation for dashboards and reporting
- ✅ Ready for BI tool integration

## ✅ Step 10: Data Quality Tests

This section documents the implementation of comprehensive data quality tests, which directly maps to "preparing test cases" and demonstrates data engineering maturity.

### Why Data Quality Tests Matter

**This maps directly to "preparing test cases" + data engineering maturity:**
- ICBC job requirements emphasize test cases and data quality
- Demonstrates professional data engineering practices
- Catches data issues early in the pipeline
- Ensures data reliability for business decisions
- Even basic checks impress interviewers

**Business Value:**
- Prevents bad data from reaching analytics
- Ensures data integrity and reliability
- Builds confidence in analytical results
- Supports regulatory compliance

### Implementation

**Created: `spark/06_data_quality_tests.py`**

Comprehensive data quality test suite covering:

**1. Fact Table Tests:**
- ✅ No null collision_id
- ✅ No negative injury counts
- ✅ No null claim_id
- ✅ No negative claim amounts
- ✅ Valid claim status values
- ✅ days_to_close logic (NULL for OPEN claims)

**2. Dimension Table Tests:**
- ✅ Dimension keys are unique
- ✅ Dimension keys are not null
- ✅ No duplicate surrogate keys

**3. Referential Integrity Tests:**
- ✅ Fact foreign keys exist in dimensions
- ✅ Claims reference valid collisions
- ✅ All joins are valid

**4. Data Consistency Tests:**
- ✅ Row counts make sense (Gold <= Silver)
- ✅ Claims count <= Collisions count
- ✅ Derived fields match calculations
- ✅ Severity category matches injury counts

### Test Framework

**Test Function:**
```python
def run_test(test_name, condition, df, error_message):
    """Run a data quality test and report results"""
    failed_count = df.filter(~condition).count()
    passed = failed_count == 0
    # Report results with ✅ or ❌
```

**Test Structure:**
- Clear test names
- Specific error messages
- Pass/fail reporting
- Summary statistics

### Running Data Quality Tests

**Command:**
```bash
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/06_data_quality_tests.py"
```

**Expected Output:**
```
================================================================================
ROADSAFE DATA QUALITY TESTS
================================================================================

Loading data tables...
Loaded tables:
  - fact_collisions: [count] rows
  - fact_claims: [count] rows
  ...

================================================================================
FACT TABLE TESTS
================================================================================

✅ PASS | fact_collisions: No null collision_id
   All [count] rows passed

✅ PASS | fact_collisions: No negative injury counts
   All [count] rows passed

...

================================================================================
TEST SUMMARY
================================================================================

Total Tests: 14
Passed: 14 ✅
Failed: 0 ❌

🎉 ALL TESTS PASSED! Data quality is excellent.
```

### Test Categories

**1. Completeness Tests:**
- No null values in key fields
- Required fields are populated

**2. Validity Tests:**
- Values are within expected ranges
- No negative counts or amounts
- Status values are valid

**3. Uniqueness Tests:**
- Surrogate keys are unique
- No duplicate records

**4. Referential Integrity Tests:**
- Foreign keys reference valid records
- Joins are possible without orphaned records

**5. Consistency Tests:**
- Derived fields match calculations
- Row counts are logical
- Business rules are enforced

### Key Learnings

1. **Test Early and Often:**
   - Catch data quality issues early
   - Prevent downstream problems
   - Build confidence in data

2. **Comprehensive Coverage:**
   - Test all critical fields
   - Test relationships between tables
   - Test business logic

3. **Clear Reporting:**
   - Easy to understand pass/fail
   - Specific error messages
   - Summary statistics

4. **Automation:**
   - Run tests as part of pipeline
   - Fail pipeline if tests fail
   - Integrate with CI/CD

5. **Resume Value:**
   - Demonstrates professional practices
   - Shows attention to data quality
   - Highlights testing expertise

### Integration with Pipeline

**Add to ETL Scripts:**
```python
# After writing Gold tables
from spark.data_quality_tests import run_all_tests
if not run_all_tests():
    raise Exception("Data quality tests failed!")
```

**Run After Each Step:**
- After Bronze ingestion
- After Silver cleaning
- After Gold build
- After claims generation

### Next Steps

With data quality tests complete:
- ✅ Comprehensive test coverage
- ✅ Automated quality checks
- ✅ Professional data engineering practices
- ✅ Demonstrates testing expertise
- ✅ Ensures data reliability
- ✅ Ready for production deployment

## 📈 Business Insights

*Results and visualizations will be documented here as the project progresses.*

## 📝 Documentation

- [Architecture Diagram](docs/architecture.md)
- [Source → Target Mapping](docs/source_target_mapping.md)
- [Schema Definitions](docs/schemas.md)
- [Test Cases](docs/test_plan.md)
- [ICBC TAS Reference Data](docs/icbc_tas_reference/README.md)

## 🎓 Resume Bullet Points

- Designed and implemented a Hadoop/Spark-based data pipeline processing traffic collision and insurance claim datasets
- Built ETL workflows using PySpark and Spark SQL to transform raw data into analytics-ready fact and dimension tables
- Modeled relational schemas and performed complex ANSI SQL queries for business insights related to road safety and claim severity
- Developed data quality checks, source-to-target mappings, and test cases to validate pipeline accuracy
- Created interactive dashboards to visualize collision hotspots, claim trends, and weather correlations

What you can do with RoadSafe Analytics Platform
1. Data pipeline operations (ETL)
Run the full data pipeline:
# Step 1: Ingest raw CSV → Bronze (Parquet)spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py# Step 2: Clean & normalize → Silverspark-submit --master spark://localhost:7077 spark/02_clean_silver.py# Step 3: Build star schema → Goldspark-submit --master spark://localhost:7077 spark/03_build_gold.py# Step 4: Create ICBC-aligned viewspark-submit --master spark://localhost:7077 spark/04_build_icbc_view.py# Step 5: Generate synthetic claimsspark-submit --master spark://localhost:7077 spark/05_generate_claims.py
What this creates:
Bronze layer: Raw data as Parquet (fast, typed, compressed)
Silver layer: Cleaned, normalized, validated data
Gold layer: Star schema with fact and dimension tables
ICBC view: Enterprise-aligned dataset for insurance analytics
Claims data: Synthetic insurance claims linked to collisions
2. Analytics and insights
Run 19 SQL analytics queries:
spark-submit --master spark://localhost:7077 spark/run_sql.py sql/analytics.sql
Analytics include:
Top collision hotspots by location (borough, zip code, intersections)
Worst hours for collisions (time distribution analysis)
Collision trends over time (monthly/yearly patterns)
Claim analysis (amounts, status, days to close)
Contributing factors ranking (most common causes)
Vehicle type analysis (which types have more collisions)
Severity analysis (injury vs fatality patterns)
Geographic patterns (by borough, zip code)
3. Data quality assurance
Run data quality tests:
spark-submit --master spark://localhost:7077 spark/06_data_quality_tests.py
Tests include:
No null collision IDs
No negative injury counts
Claims validation (amounts, status, logic)
Dimension key uniqueness
Referential integrity (foreign keys exist)
Row count validations
Data consistency checks
4. Business use cases
For ICBC/insurance:
Identify high-risk locations for targeted safety improvements
Analyze claim patterns and costs
Understand peak collision times for resource allocation
Track collision trends over time
Analyze contributing factors to prevent future incidents
Calculate average claim amounts by severity/borough
Monitor claim status and closure rates
For data engineering:
Demonstrate ETL pipeline skills
Show data modeling (star schema)
Implement data quality checks
Use enterprise patterns (Bronze/Silver/Gold)
Work with Spark, Parquet, and SQL
Handle schema enforcement and data contracts
5. Interactive exploration
Use Spark Shell for ad-hoc queries:
docker exec -it roadsafe-spark spark-shell
Then query your data:
// Load tablesval collisions = spark.read.parquet("data/gold/fact_collisions")val locations = spark.read.parquet("data/gold/dim_location")// Create temp viewscollisions.createOrReplaceTempView("fact_collisions")locations.createOrReplaceTempView("dim_location")// Run SQL queriesspark.sql("SELECT borough, COUNT(*) as count FROM dim_location GROUP BY borough").show()
6. Data visualization (ready for BI tools)
Your Gold layer tables are ready for:
Tableau
Power BI
Apache Superset
Any SQL-compatible BI tool
The star schema is optimized for:
Fast joins
Easy drag-and-drop analysis
Standard SQL queries
7. Custom analytics
Create your own SQL queries in sql/analytics.sql or write new Spark jobs:
from pyspark.sql import SparkSessionspark = SparkSession.builder.appName("custom-analysis").getOrCreate()# Load your datacollisions = spark.read.parquet("data/gold/fact_collisions")claims = spark.read.parquet("data/gold/fact_claims")# Your custom analysis hereresult = collisions.join(claims, "collision_id").groupBy("severity_category").agg({    "claim_amount": "avg"}).show()
Summary
This platform lets you:
Process raw traffic collision data through a complete ETL pipeline
Generate insurance claims data for financial analysis
Run 19 pre-built analytics queries
Validate data quality with automated tests
Create ICBC-aligned datasets for enterprise use
Explore data interactively with Spark SQL
Export to BI tools for visualization
Build custom analytics on top of the star schema
All commands run inside Docker, so the environment is reproducible and consistent.



bronze to silver losing injusries why learn later