from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark.schemas import NYC_COLLISIONS_SCHEMA

RAW_PATH = "data/raw/nyc_motor_vehicle_collisions.csv"
SILVER_OUT = "data/silver/collisions"

def to_null_if_blank(col):
    """Convert empty strings and whitespace-only strings to null"""
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(F.trim(col))

def main():
    spark = SparkSession.builder.appName("roadsafe-silver-clean").getOrCreate()

    # Read raw CSV with strict schema
    df = (spark.read
          .option("header", "true")
          .schema(NYC_COLLISIONS_SCHEMA)
          .csv(RAW_PATH))

    # Normalize strings: trim whitespace and convert empty strings to null
    # Note: Column names match schema exactly (with spaces)
    df = (df
          .withColumn("BOROUGH", to_null_if_blank(F.col("BOROUGH")))
          .withColumn("ZIP CODE", to_null_if_blank(F.col("ZIP CODE")))
          .withColumn("CONTRIBUTING FACTOR VEHICLE 1", to_null_if_blank(F.col("CONTRIBUTING FACTOR VEHICLE 1")))
          .withColumn("CONTRIBUTING FACTOR VEHICLE 2", to_null_if_blank(F.col("CONTRIBUTING FACTOR VEHICLE 2")))
          .withColumn("VEHICLE TYPE CODE 1", to_null_if_blank(F.col("VEHICLE TYPE CODE 1")))
          .withColumn("VEHICLE TYPE CODE 2", to_null_if_blank(F.col("VEHICLE TYPE CODE 2"))))

    # Parse date + time
    # CRASH DATE format: "MM/dd/yyyy" (e.g., "09/11/2021")
    # CRASH TIME format: "HH:mm" (e.g., "2:39" or "14:39")
    df = (df
          .withColumn("crash_date", F.to_date("CRASH DATE", "MM/dd/yyyy"))
          .withColumn("crash_time", F.to_timestamp("CRASH TIME", "HH:mm")))

    # Basic validity filters
    # Filter out rows with invalid dates
    df = df.filter(F.col("crash_date").isNotNull())

    # Filter coordinates: Keep rows with valid NYC coordinates OR rows with injuries/fatalities
    # This ensures we don't lose important injury data just because coordinates are missing
    # NYC approximate bounds: lat 40.3-41.1, lon -74.5 to -73.4
    has_valid_coords = (
        (F.col("LATITUDE").isNotNull()) &
        (F.col("LONGITUDE").isNotNull()) &
        (F.col("LATITUDE").between(40.3, 41.1)) &
        (F.col("LONGITUDE").between(-74.5, -73.4))
    )
    
    # Keep rows with valid coordinates OR rows with injuries/fatalities (even if coords missing)
    has_injuries_or_fatalities = (
        (F.col("NUMBER OF PERSONS INJURED").isNotNull() & (F.col("NUMBER OF PERSONS INJURED") > 0)) |
        (F.col("NUMBER OF PERSONS KILLED").isNotNull() & (F.col("NUMBER OF PERSONS KILLED") > 0))
    )
    
    df = df.filter(has_valid_coords | has_injuries_or_fatalities)

    # Create a stable collision id
    # If CSV had COLLISION_ID, we'd use that, but we'll create a hash-based ID
    # Using SHA256 hash of date|time|lat|lon for uniqueness
    df = df.withColumn("collision_id",
                       F.sha2(F.concat_ws("|",
                                          F.col("CRASH DATE"),
                                          F.col("CRASH TIME"),
                                          F.col("LATITUDE").cast("string"),
                                          F.col("LONGITUDE").cast("string")), 256))

    # Select and rename columns for cleaner Silver layer
    # Using lowercase with underscores for consistency
    out = (df
           .select(
               "collision_id",
               "crash_date",
               F.col("CRASH TIME").alias("crash_time_str"),  # Keep original time string
               F.col("BOROUGH").alias("borough"),
               F.col("ZIP CODE").alias("zip_code"),
               F.col("LATITUDE").alias("latitude"),
               F.col("LONGITUDE").alias("longitude"),
               F.col("NUMBER OF PERSONS INJURED").alias("persons_injured"),
               F.col("NUMBER OF PERSONS KILLED").alias("persons_killed"),
               F.col("NUMBER OF PEDESTRIANS INJURED").alias("pedestrians_injured"),
               F.col("NUMBER OF PEDESTRIANS KILLED").alias("pedestrians_killed"),
               F.col("NUMBER OF CYCLIST INJURED").alias("cyclist_injured"),
               F.col("NUMBER OF CYCLIST KILLED").alias("cyclist_killed"),
               F.col("NUMBER OF MOTORIST INJURED").alias("motorist_injured"),
               F.col("NUMBER OF MOTORIST KILLED").alias("motorist_killed"),
               F.col("CONTRIBUTING FACTOR VEHICLE 1").alias("contributing_factor_vehicle_1"),
               F.col("CONTRIBUTING FACTOR VEHICLE 2").alias("contributing_factor_vehicle_2"),
               F.col("VEHICLE TYPE CODE 1").alias("vehicle_type_code_1"),
               F.col("VEHICLE TYPE CODE 2").alias("vehicle_type_code_2")
           ))

    # Write to Silver layer
    out.write.mode("overwrite").parquet(SILVER_OUT)
    print(f"Wrote silver parquet to: {SILVER_OUT}")
    print(f"Rows written: {out.count()}")

    spark.stop()

if __name__ == "__main__":
    main()



