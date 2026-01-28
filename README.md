# RoadSafe Analytics 🚗📊

**RoadSafe Analytics** is a data engineering pipeline for traffic collision analysis built with **Apache Spark, Docker, and Python**.
It processes millions of collision records (Motor Vehicle Collisions - Crashes in NYC) through a multi-layered data lake architecture (Bronze → Silver → Gold), ensuring data quality while preserving critical safety metrics. The pipeline transforms raw CSV data into queryable Parquet files, builds dimensional models for analytics using SQL, generates synthetic insurance claims and interactive visualizations.

## What can it do?

✨ **Data Pipeline** - Multi-layer ETL from raw CSV to analytics-ready star schema  
📊 **Interactive Dashboards** - Collision hotspots, time trends, severity analysis, and claims insights  
🔍 **SQL Analytics** - 19 pre-built queries for business intelligence  
⚡ **Data Quality** - Automated tests ensuring data integrity across layers  
🏗️ **Dimensional Modeling** - Star schema with fact and dimension tables  
📈 **ICBC Alignment** - Data structures aligned with ICBC Traffic Accident System standards

## Technologies Used

- **Apache Spark 3.5** - Distributed data processing engine
- **PySpark** - Python API for Spark
- **Spark SQL** - SQL interface for analytics
- **Docker & Docker Compose** - Containerized Spark cluster
- **Plotly** - Interactive data visualizations
- **Parquet** - Columnar storage format for efficient analytics

## Project Structure

```
roadsafe-analytics/
├── spark/                       # ETL pipeline scripts
│   ├── 01_ingest_raw.py         # Bronze layer: CSV → Parquet
│   ├── 02_clean_silver.py       # Silver layer: Data cleaning & normalization
│   ├── 03_build_gold.py         # Gold layer: Star schema construction
│   ├── 04_build_icbc_view.py    # ICBC TAS aligned presentation layer
│   ├── 05_generate_claims.py    # Synthetic insurance claims generation
│   ├── 06_data_quality_tests.py # Data quality validation suite
│   ├── 07_create_dashboards.py  # Dashboard generation
│   ├── run_sql.py               # SQL query execution engine
│   └── schemas.py               # Schema definitions
├── sql/                         # Analytics queries
│   └── analytics.sql            # 19 pre-built SQL queries
├── dashboards/                  # Generated HTML dashboards
│   ├── 00_summary.html          # Executive summary dashboard
│   ├── 01_collision_hotspots.html
│   ├── 02_time_trends.html
│   ├── 03_claims_analysis.html
│   ├── 04_severity_analysis.html
│   └── 05_contributing_factors.html
├── data/                        # Data lake layers
│   ├── raw/                     # Source CSV files
│   ├── bronze/                  # Raw Parquet (schema-enforced)
│   ├── silver/                  # Cleaned & normalized data
│   └── gold/                    # Star schema tables
│       ├── dim_time/
│       ├── dim_location/
│       ├── dim_vehicle/
│       ├── dim_factor/
│       ├── fact_collisions/
│       └── fact_claims/
├── docs/                        # Documentation
│   └── icbc_tas_reference/      # ICBC reference data
├── docker-compose.yml           # Spark cluster configuration
├── .bashrc                      # Container environment setup
├── requirements.txt             # Python dependencies
└── README.md                  
```

## Quick Start

### Prerequisites

- Docker Desktop
- Docker Compose
- 4GB+ RAM available for Spark

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd roadsafe-analytics
   ```

2. **Download the raw dataset (required)**

   The raw CSV is intentionally **not** committed to git (it’s too large for GitHub). Download it from NYC Open Data and save it to `data/raw/`:

   ```bash
   mkdir -p data/raw
   curl -L "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD" \
     -o data/raw/nyc_motor_vehicle_collisions.csv
   ```

   Dataset reference: `https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data`

3. **Start Spark cluster**
   ```bash
   docker compose up -d
   ```
   
   Wait for services to start (check with `docker compose ps`).

4. **Run the pipeline**
   
   Execute each stage sequentially:
   ```bash
   # Bronze: Ingest raw CSV → Parquet
   docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py"
   
   # Silver: Clean and normalize data
   docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/02_clean_silver.py"
   
   # Gold: Build star schema
   docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/03_build_gold.py"
   
   # ICBC View: Create ICBC-aligned presentation layer
   docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/04_build_icbc_view.py"
   
   # Claims: Generate synthetic insurance claims
   docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/05_generate_claims.py"
   ```

5. **Run data quality tests**
   ```bash
   docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/06_data_quality_tests.py"
   ```

6. **Generate dashboards**
   ```bash
   docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/07_create_dashboards.py"
   ```
   
   Dashboards will be generated in the `dashboards/` directory.

7. **Run SQL analytics**
   ```bash
   docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/run_sql.py sql/analytics.sql"
   ```

8. **View dashboards**
   
   Open `dashboards/00_summary.html` in your browser, or serve locally:
   ```bash
   python3 -m http.server 8000
   # Then visit:
   # http://localhost:8000/dashboards/00_summary.html
   # http://localhost:8000/dashboards/01_collision_hotspots.html
   # http://localhost:8000/dashboards/02_time_trends.html
   # http://localhost:8000/dashboards/04_severity_analysis.html
   ```

## Data Quality Tests

The pipeline includes automated data quality tests covering:

Run tests:
```bash
docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/06_data_quality_tests.py"
```

## SQL Analytics

The project includes pre-built SQL queries covering:

- Top collision hotspots by location
- Worst hours and days for collisions
- Monthly and yearly trends
- Claim amounts by borough and severity
- Contributing factors analysis
- Vehicle type distributions

Execute all queries:
```bash
docker compose exec spark bash -c "cd /workspace && spark-submit --master spark://localhost:7077 spark/run_sql.py sql/analytics.sql"
```

## Dashboards

Interactive HTML dashboards generated using Plotly:

- **Summary Dashboard** - Key metrics and KPIs
- **Collision Hotspots** - Geographic visualization of high-incident areas
- **Time Trends** - Temporal patterns and seasonality
- **Claims Analysis** - Insurance claims insights
- **Severity Analysis** - Injury and fatality breakdowns
- **Contributing Factors** - Root cause analysis

All dashboards are self-contained HTML files requiring no server.

## Development

### Running Commands Inside Container

For frequent development, enter the container once:
```bash
docker exec -it roadsafe-spark bash
cd /workspace
spark-submit --master spark://localhost:7077 spark/01_ingest_raw.py
```

### Spark UI

Monitor job execution:
- **Spark UI**: http://localhost:4040 (active jobs)
- **Master UI**: http://localhost:8080 (cluster status)
- **Worker UI**: http://localhost:8081 (worker status)

### Interactive Spark Shell

```bash
docker exec -it roadsafe-spark spark-shell --master spark://localhost:7077
```

### Environment Variables

The Docker containers are configured with:
- `PATH`: Includes Spark binaries
- `PYTHONPATH`: Includes workspace and user-installed packages
- `SPARK_MASTER_URL`: Default Spark master URL

## Troubleshooting

**"spark-submit: command not found"**
- Ensure you're running commands inside the Docker container
- Use `docker compose exec spark bash` to enter the container

**"ModuleNotFoundError"**
- Check that `PYTHONPATH` includes `/workspace`
- Install packages with `pip install --user <package>`

**Dashboard shows 0 for injuries/fatalities**
- Ensure Silver layer was rebuilt after the coordinate filter fix
- Re-run: `spark/02_clean_silver.py`, then `spark/03_build_gold.py`

**Out of memory errors**
- Increase Spark worker memory in `docker-compose.yml`:
  ```yaml
  SPARK_WORKER_MEMORY: 4g
  ```

**Check service status**
```bash
docker compose ps
docker compose logs spark
docker compose logs spark-worker
```


## License

MIT

---

