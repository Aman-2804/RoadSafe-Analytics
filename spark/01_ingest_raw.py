from pyspark.sql import SparkSession
from spark.schemas import NYC_COLLISIONS_SCHEMA

RAW_PATH = "data/raw/nyc_motor_vehicle_collisions.csv"
BRONZE_OUT = "data/bronze/nyc_collisions"

def main():
    spark = (SparkSession.builder
             .appName("roadsafe-bronze-ingest")
             .getOrCreate())

    df = (spark.read
          .option("header", "true")
          .schema(NYC_COLLISIONS_SCHEMA)
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

