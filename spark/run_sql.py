"""
Run SQL queries from sql/analytics.sql using Spark SQL
"""
from pyspark.sql import SparkSession
import sys
import os

GOLD_BASE = "data/gold"

def main():
    spark = SparkSession.builder.appName("roadsafe-sql-analytics").getOrCreate()
    
    # Register Gold tables as temporary views
    print("Registering Gold tables as Spark SQL views...")
    
    spark.read.parquet(f"{GOLD_BASE}/fact_collisions").createOrReplaceTempView("fact_collisions")
    try:
        spark.read.parquet(f"{GOLD_BASE}/fact_claims").createOrReplaceTempView("fact_claims")
    except Exception:
        print("  ⚠️  fact_claims not found - some queries may fail")
    spark.read.parquet(f"{GOLD_BASE}/dim_time").createOrReplaceTempView("dim_time")
    spark.read.parquet(f"{GOLD_BASE}/dim_location").createOrReplaceTempView("dim_location")
    spark.read.parquet(f"{GOLD_BASE}/dim_vehicle").createOrReplaceTempView("dim_vehicle")
    spark.read.parquet(f"{GOLD_BASE}/dim_factor").createOrReplaceTempView("dim_factor")
    
    print("✅ Tables registered")
    print()
    
    # Read SQL file
    sql_file = sys.argv[1] if len(sys.argv) > 1 else "sql/analytics.sql"
    
    if not os.path.exists(sql_file):
        print(f"Error: SQL file not found: {sql_file}")
        sys.exit(1)
    
    print(f"Reading SQL queries from: {sql_file}")
    print()
    
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    # Split by semicolon and clean up queries
    raw_queries = sql_content.split(';')
    queries = []
    
    for q in raw_queries:
        q = q.strip()
        # Skip empty queries
        if not q:
            continue
        # Skip queries that are only comments
        lines = [line.strip() for line in q.split('\n') if line.strip()]
        if not lines or all(line.startswith('--') for line in lines):
            continue
        # Remove comment-only lines from query
        cleaned_lines = [line for line in q.split('\n') if line.strip() and not line.strip().startswith('--')]
        if cleaned_lines:
            queries.append('\n'.join(cleaned_lines))
    
    print(f"Found {len(queries)} queries to execute")
    print("=" * 80)
    print()
    
    for i, query in enumerate(queries, 1):
        
        # Extract query name from comment if available
        lines = query.split('\n')
        query_name = None
        for line in lines[:5]:  # Check first 5 lines for comment
            if '--' in line and ('TOP' in line.upper() or 'WORST' in line.upper() or 'TREND' in line.upper() or 'CLAIM' in line.upper() or 'CONTRIBUTING' in line.upper()):
                query_name = line.split('--')[1].strip()
                break
        
        if not query_name:
            query_name = f"Query {i}"
        
        print(f"Executing: {query_name}")
        print("-" * 80)
        
        try:
            result = spark.sql(query)
            result.show(20, truncate=False)
            print()
        except Exception as e:
            print(f"❌ Error executing query: {str(e)}")
            print()
    
    print("=" * 80)
    print("✅ All queries executed")
    
    spark.stop()

if __name__ == "__main__":
    main()

