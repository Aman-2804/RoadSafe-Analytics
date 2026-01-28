from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_PATH = "data/silver/collisions"
GOLD_BASE = "data/gold"

def main():
    spark = SparkSession.builder.appName("roadsafe-gold-star-schema").getOrCreate()

    # Read Silver layer
    print("Reading Silver layer...")
    silver_df = spark.read.parquet(SILVER_PATH)
    print(f"Silver rows: {silver_df.count()}")

    # ============================================================================
    # DIMENSION TABLES (with surrogate keys)
    # ============================================================================

    # 1. DIM_TIME
    print("\n=== Building dim_time ===")
    dim_time = (silver_df
                .select("crash_date", "crash_time_str")
                .distinct()
                .withColumn("year", F.year("crash_date"))
                .withColumn("month", F.month("crash_date"))
                .withColumn("day", F.dayofmonth("crash_date"))
                .withColumn("day_of_week", F.dayofweek("crash_date"))  # 1=Sunday, 7=Saturday
                .withColumn("day_name", F.date_format("crash_date", "EEEE"))  # Full day name
                .withColumn("month_name", F.date_format("crash_date", "MMMM"))  # Full month name
                .withColumn("quarter", F.quarter("crash_date"))
                .withColumn("is_weekend", F.when(F.dayofweek("crash_date").isin([1, 7]), True).otherwise(False))
                # Extract hour from time string (format: "H:mm" or "HH:mm")
                .withColumn("hour", 
                           F.when(F.col("crash_time_str").isNotNull(),
                                  F.regexp_extract(F.col("crash_time_str"), r"^(\d+):", 1).cast("int"))
                           .otherwise(None))
                .withColumn("time_category",
                           F.when((F.col("hour") >= 0) & (F.col("hour") < 6), "00:00-05:59")
                           .when((F.col("hour") >= 6) & (F.col("hour") < 9), "06:00-08:59")
                           .when((F.col("hour") >= 9) & (F.col("hour") < 12), "09:00-11:59")
                           .when((F.col("hour") >= 12) & (F.col("hour") < 15), "12:00-14:59")
                           .when((F.col("hour") >= 15) & (F.col("hour") < 18), "15:00-17:59")
                           .when((F.col("hour") >= 18) & (F.col("hour") < 21), "18:00-20:59")
                           .otherwise("21:00-23:59"))
                .select("crash_date", "crash_time_str", "year", "month", "day", 
                       "day_of_week", "day_name", "month_name", "quarter",
                       "hour", "time_category", "is_weekend"))

    # Add surrogate key (time_key)
    window = Window.orderBy("crash_date", "crash_time_str")
    dim_time = dim_time.withColumn("time_key", F.row_number().over(window))

    # Reorder columns with time_key first
    dim_time = dim_time.select("time_key", *[c for c in dim_time.columns if c != "time_key"])
    
    dim_time.write.mode("overwrite").parquet(f"{GOLD_BASE}/dim_time")
    print(f"dim_time rows: {dim_time.count()}")

    # 2. DIM_LOCATION
    print("\n=== Building dim_location ===")
    dim_location = (silver_df
                   .select("borough", "zip_code", "latitude", "longitude")
                   .distinct()
                   .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
                   .withColumn("location_name",
                              F.when(F.col("borough").isNotNull(),
                                     F.concat_ws(", ", F.col("borough"), F.col("zip_code")))
                              .otherwise("Unknown"))
                   .select("borough", "zip_code", "latitude", "longitude", "location_name"))

    # Add surrogate key (location_key)
    window = Window.orderBy("borough", "zip_code", "latitude", "longitude")
    dim_location = dim_location.withColumn("location_key", F.row_number().over(window))

    # Reorder columns with location_key first
    dim_location = dim_location.select("location_key", *[c for c in dim_location.columns if c != "location_key"])
    
    dim_location.write.mode("overwrite").parquet(f"{GOLD_BASE}/dim_location")
    print(f"dim_location rows: {dim_location.count()}")

    # 3. DIM_VEHICLE
    print("\n=== Building dim_vehicle ===")
    # Collect unique vehicle types from both code1 and code2
    vehicle_codes = (silver_df
                    .select(F.col("vehicle_type_code_1").alias("vehicle_type_code"))
                    .union(silver_df.select(F.col("vehicle_type_code_2").alias("vehicle_type_code")))
                    .filter(F.col("vehicle_type_code").isNotNull())
                    .distinct())

    dim_vehicle = (vehicle_codes
                  .withColumn("vehicle_type_name", 
                            F.when(F.col("vehicle_type_code").rlike("(?i)sedan"), "Sedan")
                            .when(F.col("vehicle_type_code").rlike("(?i)truck"), "Truck")
                            .when(F.col("vehicle_type_code").rlike("(?i)bus"), "Bus")
                            .when(F.col("vehicle_type_code").rlike("(?i)motorcycle|moped"), "Motorcycle/Moped")
                            .when(F.col("vehicle_type_code").rlike("(?i)bicycle|bike"), "Bicycle")
                            .when(F.col("vehicle_type_code").rlike("(?i)van"), "Van")
                            .when(F.col("vehicle_type_code").rlike("(?i)taxi"), "Taxi")
                            .otherwise(F.col("vehicle_type_code")))
                  .select("vehicle_type_code", "vehicle_type_name"))

    # Add surrogate key (vehicle_key)
    window = Window.orderBy("vehicle_type_code")
    dim_vehicle = dim_vehicle.withColumn("vehicle_key", F.row_number().over(window))

    # Reorder columns with vehicle_key first
    dim_vehicle = dim_vehicle.select("vehicle_key", *[c for c in dim_vehicle.columns if c != "vehicle_key"])
    
    dim_vehicle.write.mode("overwrite").parquet(f"{GOLD_BASE}/dim_vehicle")
    print(f"dim_vehicle rows: {dim_vehicle.count()}")

    # 4. DIM_FACTOR
    print("\n=== Building dim_factor ===")
    # Collect unique contributing factors from both factor1 and factor2
    factor_codes = (silver_df
                   .select(F.col("contributing_factor_vehicle_1").alias("contributing_factor"))
                   .union(silver_df.select(F.col("contributing_factor_vehicle_2").alias("contributing_factor")))
                   .filter(F.col("contributing_factor").isNotNull())
                   .distinct())

    dim_factor = (factor_codes
                 .withColumn("factor_category",
                           F.when(F.col("contributing_factor").rlike("(?i)unspecified"), "Unspecified")
                           .when(F.col("contributing_factor").rlike("(?i)driver|following|speed|aggressive"), "Driver Behavior")
                           .when(F.col("contributing_factor").rlike("(?i)pavement|road|weather"), "Road/Weather")
                           .when(F.col("contributing_factor").rlike("(?i)vehicle|brake|tire"), "Vehicle Issue")
                           .when(F.col("contributing_factor").rlike("(?i)pedestrian|bicyclist"), "Pedestrian/Cyclist")
                           .otherwise("Other"))
                 .select("contributing_factor", "factor_category"))

    # Add surrogate key (factor_key)
    window = Window.orderBy("contributing_factor")
    dim_factor = dim_factor.withColumn("factor_key", F.row_number().over(window))

    # Reorder columns with factor_key first
    dim_factor = dim_factor.select("factor_key", *[c for c in dim_factor.columns if c != "factor_key"])
    
    dim_factor.write.mode("overwrite").parquet(f"{GOLD_BASE}/dim_factor")
    print(f"dim_factor rows: {dim_factor.count()}")

    # ============================================================================
    # FACT TABLE
    # ============================================================================

    print("\n=== Building fact_collisions ===")
    
    # Join to get dimension keys
    fact_df = (silver_df
              .join(dim_time, 
                   (silver_df["crash_date"] == dim_time["crash_date"]) &
                   (silver_df["crash_time_str"] == dim_time["crash_time_str"]),
                   "left")
              .join(dim_location,
                   (silver_df["borough"] == dim_location["borough"]) &
                   (silver_df["zip_code"] == dim_location["zip_code"]) &
                   (silver_df["latitude"] == dim_location["latitude"]) &
                   (silver_df["longitude"] == dim_location["longitude"]),
                   "left")
              .join(dim_vehicle.alias("v1"),
                   silver_df["vehicle_type_code_1"] == F.col("v1.vehicle_type_code"),
                   "left")
              .join(dim_vehicle.alias("v2"),
                   silver_df["vehicle_type_code_2"] == F.col("v2.vehicle_type_code"),
                   "left")
              .join(dim_factor.alias("f1"),
                   silver_df["contributing_factor_vehicle_1"] == F.col("f1.contributing_factor"),
                   "left")
              .join(dim_factor.alias("f2"),
                   silver_df["contributing_factor_vehicle_2"] == F.col("f2.contributing_factor"),
                   "left"))

    # Build fact table
    fact_collisions = (fact_df
                       .select(
                           silver_df["collision_id"],
                           F.col("time_key"),
                           F.col("location_key"),
                           F.col("v1.vehicle_key").alias("primary_vehicle_key"),
                           F.col("v2.vehicle_key").alias("secondary_vehicle_key"),
                           F.col("f1.factor_key").alias("primary_factor_key"),
                           F.col("f2.factor_key").alias("secondary_factor_key"),
                           # Measures
                           silver_df["persons_injured"],
                           silver_df["persons_killed"],
                           silver_df["pedestrians_injured"],
                           silver_df["pedestrians_killed"],
                           silver_df["cyclist_injured"],
                           silver_df["cyclist_killed"],
                           silver_df["motorist_injured"],
                           silver_df["motorist_killed"],
                           # Derived measures
                           (silver_df["persons_injured"] + silver_df["persons_killed"]).alias("total_persons_affected"),
                           F.when(silver_df["persons_killed"] > 0, "Fatal")
                            .when(silver_df["persons_injured"] > 0, "Injury")
                            .otherwise("Property Damage Only").alias("severity_category")
                       ))

    fact_collisions.write.mode("overwrite").parquet(f"{GOLD_BASE}/fact_collisions")
    print(f"fact_collisions rows: {fact_collisions.count()}")

    print("\n=== Gold layer complete ===")
    print(f"Dimensions and fact table written to: {GOLD_BASE}/")
    
    spark.stop()

if __name__ == "__main__":
    main()




