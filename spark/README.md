# Spark ETL Jobs

This directory contains PySpark ETL jobs for data ingestion, transformation, and loading.

## Structure

- `schemas.py` - **Schema definitions** - Explicit schemas for all datasets (strict typing)
- `01_ingest_raw.py` - **Bronze layer:** Ingests raw CSV and writes Parquet (uses strict schema)
- `02_clean_silver.py` - **Silver layer:** Cleans, normalizes, and validates data (date parsing, null handling, quality filters)
- `03_build_gold.py` - **Gold layer:** Builds star schema (dimensions + fact table with surrogate keys)
- `04_build_icbc_view.py` - **ICBC view:** Creates ICBC TAS-aligned view with enterprise naming conventions
- `05_generate_claims.py` - **Claims generation:** Generates synthetic insurance claims linked to collisions
- `06_data_quality_tests.py` - **Data quality:** Comprehensive data quality test suite
- `run_sql.py` - **SQL runner:** Executes SQL queries from sql/analytics.sql using Spark SQL
- `ingestion.py` - Raw data ingestion with schema enforcement (planned)
- `transformations.py` - Data cleaning and validation (planned)
- `load.py` - Load data into fact and dimension tables (planned)
- `utils.py` - Shared utilities and helper functions (planned)

## Usage

### Running from Docker Container

```bash
# Bronze ingestion (CSV → Parquet)
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py"

# Silver cleaning (Bronze → Silver)
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/02_clean_silver.py"

# Gold star schema (Silver → Gold)
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/03_build_gold.py"

# ICBC-aligned view (Gold → ICBC View)
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/04_build_icbc_view.py"

# Generate synthetic claims (Gold → Claims)
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/05_generate_claims.py"
```

### Running Locally (if Spark is installed)

```bash
spark-submit spark/01_ingest_raw.py
```

## Job Details

### 01_ingest_raw.py

**Purpose:** Ingest raw CSV data into Bronze layer as Parquet

**Input:** `data/raw/nyc_motor_vehicle_collisions.csv`

**Output:** `data/bronze/nyc_collisions/` (Parquet format)

**What it does:**
1. Reads CSV with header and strict schema (no inference)
2. Prints schema and sample rows
3. Writes standardized Parquet files

**Why Parquet:**
- Columnar storage for faster analytics
- Built-in compression
- Type safety
- Splittable for parallel processing

### 02_clean_silver.py

**Purpose:** Clean and normalize Bronze data into Silver layer

**Input:** `data/raw/nyc_motor_vehicle_collisions.csv` (reads directly from raw, can also read from bronze)

**Output:** `data/silver/collisions/` (Parquet format)

**What it does:**
1. Reads raw CSV with strict schema
2. Normalizes strings (trim whitespace, convert empty to null)
3. Parses dates and times to proper types
4. Filters invalid data (bad dates, invalid coordinates)
5. Generates stable collision_id using hash
6. Renames columns to snake_case for consistency
7. Writes cleaned Parquet files

**Key Transformations:**
- String normalization: `"  Brooklyn  "` → `"Brooklyn"`, `""` → `null`
- Date parsing: `"09/11/2021"` → `2021-09-11` (DateType)
- Coordinate filtering: Only NYC bounds (lat: 40.3-41.1, lon: -74.5 to -73.4)
- ID generation: SHA256 hash of date|time|lat|lon

### 03_build_gold.py

**Purpose:** Build star schema with dimensions and fact table

**Input:** `data/silver/collisions/` (Silver layer Parquet)

**Output:** 
- `data/gold/dim_time/` - Time dimension
- `data/gold/dim_location/` - Location dimension
- `data/gold/dim_vehicle/` - Vehicle type dimension
- `data/gold/dim_factor/` - Contributing factor dimension
- `data/gold/fact_collisions/` - Fact table

**What it does:**
1. Reads Silver layer data
2. Creates dimension tables with surrogate keys (integer IDs)
3. Creates fact table with foreign keys to dimensions
4. Enables efficient analytical queries and BI tool integration

**Star Schema Benefits:**
- Fast joins (small dimensions, large facts)
- Easy to understand business model
- Optimized for Tableau/Power BI
- Industry standard for data warehouses

### 04_build_icbc_view.py

**Purpose:** Create ICBC TAS-aligned view with enterprise naming

**Input:** Gold layer tables (dimensions + fact)

**Output:** `data/gold/icbc_view/` (ICBC-aligned Parquet)

**What it does:**
1. Joins all Gold dimension and fact tables
2. Renames columns to match ICBC TAS conventions
3. Adds ICBC-specific fields (placeholders for missing data)
4. Creates standardized severity classifications

**ICBC Naming Examples:**
- `persons_injured` → `PERSON_INJURY_CNT`
- `severity_category` → `ACC_SEVERITY_CD`
- `crash_date` → `ACCIDENT_DATE`
- `collision_id` → `ACCIDENT_NUMBER`

### 05_generate_claims.py

**Purpose:** Generate synthetic insurance claims data linked to collisions

**Input:** `data/gold/fact_collisions/` (Gold fact table)

**Output:** `data/gold/fact_claims/` (Claims fact table)

**What it does:**
1. Reads Gold fact_collisions table
2. Generates claims with probability based on severity:
   - Fatal collisions: 100% claim rate
   - Injury collisions: 85% claim rate
   - Property damage: 30% claim rate
3. Calculates claim amounts correlated with severity
4. Assigns claim status (OPEN/CLOSED) and days to close
5. Writes synthetic claims fact table

**Key Features:**
- Realistic claim generation (not all collisions have claims)
- Claim amounts correlate with injury/fatality counts
- Status distribution: 35% OPEN, 65% CLOSED
- Days to close: 30-545 days based on severity
- Enables insurance-specific analytics

**Business Value:**
- Makes project ICBC-relevant (insurance-focused)
- Enables financial analysis (claim costs, reserves)
- Supports actuarial and risk analysis
- Demonstrates insurance domain expertise



