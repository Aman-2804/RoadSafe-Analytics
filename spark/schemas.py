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




