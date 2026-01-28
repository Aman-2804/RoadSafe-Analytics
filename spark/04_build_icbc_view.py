from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_BASE = "data/gold"
ICBC_VIEW_OUT = "data/gold/icbc_view"

def main():
    spark = SparkSession.builder.appName("roadsafe-icbc-view").getOrCreate()

    print("Reading Gold layer tables...")
    
    # Read dimension and fact tables
    fact_collisions = spark.read.parquet(f"{GOLD_BASE}/fact_collisions")
    dim_time = spark.read.parquet(f"{GOLD_BASE}/dim_time")
    dim_location = spark.read.parquet(f"{GOLD_BASE}/dim_location")
    dim_vehicle = spark.read.parquet(f"{GOLD_BASE}/dim_vehicle")
    dim_factor = spark.read.parquet(f"{GOLD_BASE}/dim_factor")

    print("Building ICBC-aligned view...")

    # Join fact with dimensions
    icbc_df = (fact_collisions
              .join(dim_time, "time_key", "left")
              .join(dim_location, "location_key", "left")
              .join(dim_vehicle.alias("v1"), 
                   fact_collisions["primary_vehicle_key"] == F.col("v1.vehicle_key"), "left")
              .join(dim_vehicle.alias("v2"),
                   fact_collisions["secondary_vehicle_key"] == F.col("v2.vehicle_key"), "left")
              .join(dim_factor.alias("f1"),
                   fact_collisions["primary_factor_key"] == F.col("f1.factor_key"), "left")
              .join(dim_factor.alias("f2"),
                   fact_collisions["secondary_factor_key"] == F.col("f2.factor_key"), "left"))

    # Create ICBC-aligned view with TAS naming conventions
    icbc_view = (icbc_df
                .select(
                    # Identifiers (ICBC TAS style)
                    fact_collisions["collision_id"].alias("ACCIDENT_NUMBER"),
                    fact_collisions["collision_id"].alias("COLLISION_ID"),  # Keep both for compatibility
                    
                    # Date/Time (ICBC TAS style)
                    dim_time["crash_date"].alias("ACCIDENT_DATE"),
                    dim_time["year"].alias("YEAR"),
                    dim_time["month"].alias("MONTH"),
                    dim_time["month_name"].alias("MONTH_NAME"),
                    dim_time["day"].alias("DAY"),
                    dim_time["day_of_week"].alias("ACC_DAY_OF_WEEK"),
                    dim_time["day_name"].alias("DAY_OF_WEEK_NAME"),
                    dim_time["quarter"].alias("QUARTER"),
                    dim_time["hour"].alias("ACCIDENT_TIME"),  # Hour as integer (0-23)
                    dim_time["time_category"].alias("TIME_CATEGORY"),
                    dim_time["is_weekend"].alias("IS_WEEKEND"),
                    
                    # Location (ICBC TAS style)
                    dim_location["borough"].alias("BOROUGH"),
                    dim_location["zip_code"].alias("ZIP_CODE"),
                    dim_location["latitude"].alias("LATITUDE"),
                    dim_location["longitude"].alias("LONGITUDE"),
                    dim_location["location_name"].alias("ACCIDENT_LOCATION"),
                    
                    # Vehicle Types (ICBC TAS style)
                    F.col("v1.vehicle_type_code").alias("VEHICLE_TYPE_CODE_1"),
                    F.col("v1.vehicle_type_name").alias("VEHICLE_TYPE_NAME_1"),
                    F.col("v2.vehicle_type_code").alias("VEHICLE_TYPE_CODE_2"),
                    F.col("v2.vehicle_type_name").alias("VEHICLE_TYPE_NAME_2"),
                    
                    # Contributing Factors (ICBC TAS style)
                    F.col("f1.contributing_factor").alias("CONTRIBUTING_FACTOR_1"),
                    F.col("f1.factor_category").alias("FACTOR_CATEGORY_1"),
                    F.col("f2.contributing_factor").alias("CONTRIBUTING_FACTOR_2"),
                    F.col("f2.factor_category").alias("FACTOR_CATEGORY_2"),
                    
                    # Injury/Fatality Counts (ICBC TAS style)
                    fact_collisions["persons_injured"].alias("PERSON_INJURY_CNT"),
                    fact_collisions["persons_killed"].alias("PERSON_FATALITY_CNT"),
                    fact_collisions["pedestrians_injured"].alias("PEDESTRIAN_INJURY_CNT"),
                    fact_collisions["pedestrians_killed"].alias("PEDESTRIAN_FATALITY_CNT"),
                    fact_collisions["cyclist_injured"].alias("CYCLIST_INJURY_CNT"),
                    fact_collisions["cyclist_killed"].alias("CYCLIST_FATALITY_CNT"),
                    fact_collisions["motorist_injured"].alias("MOTORIST_INJURY_CNT"),
                    fact_collisions["motorist_killed"].alias("MOTORIST_FATALITY_CNT"),
                    fact_collisions["total_persons_affected"].alias("TOTAL_PERSONS_AFFECTED_CNT"),
                    
                    # Severity (ICBC TAS style)
                    fact_collisions["severity_category"].alias("ACC_SEVERITY_CD"),
                    F.when(fact_collisions["persons_killed"] > 0, "FATAL")
                     .when(fact_collisions["persons_injured"] > 0, "INJURY")
                     .otherwise("PROPERTY_DAMAGE_ONLY").alias("ACCIDENT_TYPE"),
                    
                    # Road/Weather conditions (placeholder - NYC data doesn't have these)
                    F.lit(None).cast("string").alias("ROAD_CONDITION_CD"),
                    F.lit(None).cast("string").alias("WEATHER_CONDITION_CD"),
                    F.lit(None).cast("string").alias("ROAD_SURFACE_CD"),
                    
                    # Dimension keys (for joins)
                    fact_collisions["time_key"],
                    fact_collisions["location_key"],
                    fact_collisions["primary_vehicle_key"],
                    fact_collisions["secondary_vehicle_key"],
                    fact_collisions["primary_factor_key"],
                    fact_collisions["secondary_factor_key"]
                ))

    # Write ICBC-aligned view
    icbc_view.write.mode("overwrite").parquet(ICBC_VIEW_OUT)
    
    print(f"ICBC-aligned view written to: {ICBC_VIEW_OUT}")
    print(f"Rows: {icbc_view.count()}")
    print("\n=== ICBC View Schema ===")
    icbc_view.printSchema()
    
    print("\n=== Sample ICBC View Data ===")
    icbc_view.show(10, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()




