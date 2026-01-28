# Analytics Queries

This directory contains SQL queries for business analytics and insights.

## Structure

- `analytics.sql` - **Comprehensive analytics queries** - All business intelligence queries in one file

## Query Categories

The `analytics.sql` file contains 10 categories of queries:

1. **Top Hotspots** - Location analysis with highest collision frequency
2. **Worst Hours** - Time distribution analysis
3. **Trend Over Time** - Monthly and yearly trend analysis
4. **Claim Amount by Borough** - Financial analysis by location
5. **Contributing Factors Ranking** - Most common contributing factors
6. **Vehicle Type Analysis** - Collisions and claims by vehicle type
7. **Severity Analysis** - Collision severity patterns
8. **Claims Analysis** - Claim status, amounts, and days to close
9. **Weekend vs Weekday** - Comparison of collision patterns
10. **Dashboard Query** - Summary statistics for dashboards

## Usage

### Method 1: Using Spark SQL Runner (Recommended)

```bash
docker compose exec spark bash -c "cd /workspace && PYTHONPATH=/workspace /opt/spark/bin/spark-submit --master spark://localhost:7077 spark/run_sql.py sql/analytics.sql"
```

This script:
- Automatically registers Gold tables as Spark SQL views
- Executes all queries from the SQL file
- Displays results for each query

### Method 2: Using Spark SQL Shell

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

# Then run queries from analytics.sql
```

### Method 3: Using Spark Shell (Scala/Python)

```bash
docker compose exec spark /opt/spark/bin/spark-shell --master spark://localhost:7077
```

Then register tables and run SQL queries.

## Query Examples

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

## SQL Skills Demonstrated

- Complex star schema joins
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY with multiple columns
- Filtering and NULL handling
- Subqueries and analytical functions
- Business logic calculations
- Percentage and ranking calculations



