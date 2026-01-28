from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import uuid

GOLD_BASE = "data/gold"
CLAIMS_OUT = "data/gold/fact_claims"

def generate_claim_id():
    """Generate UUID for claim_id"""
    return str(uuid.uuid4())

# Register as UDF
generate_claim_id_udf = F.udf(generate_claim_id, StringType())

def main():
    spark = SparkSession.builder.appName("roadsafe-generate-claims").getOrCreate()

    print("Reading Gold fact_collisions...")
    fact_collisions = spark.read.parquet(f"{GOLD_BASE}/fact_collisions")
    
    collision_count = fact_collisions.count()
    print(f"Total collisions: {collision_count}")

    print("\n=== Generating synthetic claims ===")
    
    # Generate claims for collisions
    # Not all collisions result in claims, so we'll generate claims for a subset
    # Higher probability for collisions with injuries/fatalities
    
    claims_df = (fact_collisions
                .withColumn("has_claim",
                           # Higher probability of claim if there are injuries/fatalities
                           F.when(F.col("persons_killed") > 0, True)  # Always claim if fatal
                           .when(F.col("persons_injured") > 0, 
                                F.rand() < 0.85)  # 85% chance if injury
                           .when(F.col("total_persons_affected") > 0,
                                F.rand() < 0.60)  # 60% chance if any impact
                           .otherwise(F.rand() < 0.30))  # 30% chance for property damage only
                .filter(F.col("has_claim") == True)
                .withColumn("claim_id", generate_claim_id_udf())
                
                # Claim amount: correlate with severity
                # Base amounts with multipliers for injuries/fatalities
                .withColumn("base_amount",
                           F.when(F.col("persons_killed") > 0, 
                                 F.lit(50000) + (F.col("persons_killed") * F.lit(200000)))  # $50k base + $200k per fatality
                           .when(F.col("persons_injured") > 0,
                                F.lit(10000) + (F.col("persons_injured") * F.lit(15000)))  # $10k base + $15k per injury
                           .otherwise(F.lit(3000) + (F.rand() * F.lit(7000))))  # $3k-$10k for property damage
                
                # Add variance to claim amounts (random factor 0.7x to 1.5x)
                .withColumn("claim_amount",
                           F.col("base_amount") * (F.lit(0.7) + (F.rand() * F.lit(0.8))))
                
                # Claim status: OPEN or CLOSED
                # Newer collisions more likely to be OPEN
                .withColumn("claim_status",
                           F.when(F.rand() < 0.35, "OPEN")  # 35% are open
                           .otherwise("CLOSED"))
                
                # Days to close: correlated with claim amount and severity
                # Larger claims take longer to close
                .withColumn("days_to_close",
                           F.when(F.col("claim_status") == "OPEN", F.lit(None))  # NULL for open claims
                           .when(F.col("persons_killed") > 0,
                                F.lit(180) + (F.rand() * F.lit(365)))  # 180-545 days for fatal
                           .when(F.col("persons_injured") > 0,
                                F.lit(60) + (F.rand() * F.lit(180)))  # 60-240 days for injury
                           .when(F.col("claim_amount") > F.lit(50000),
                                F.lit(90) + (F.rand() * F.lit(120)))  # 90-210 days for large claims
                           .otherwise(F.lit(30) + (F.rand() * F.lit(60))))  # 30-90 days for small claims
                
                # Round claim amount to 2 decimal places
                .withColumn("claim_amount", F.round(F.col("claim_amount"), 2))
                
                # Select final columns
                .select(
                    "claim_id",
                    "collision_id",
                    "claim_amount",
                    "claim_status",
                    "days_to_close",
                    # Keep some collision attributes for analytics
                    "persons_injured",
                    "persons_killed",
                    "total_persons_affected",
                    "severity_category"
                ))

    # Write claims fact table
    claims_df.write.mode("overwrite").parquet(CLAIMS_OUT)
    
    claim_count = claims_df.count()
    print(f"\n=== Claims Generation Complete ===")
    print(f"Total claims generated: {claim_count}")
    print(f"Claims per collision: {claim_count / collision_count:.2f}")
    
    # Print summary statistics
    print("\n=== Claims Summary Statistics ===")
    claims_df.select(
        F.count("*").alias("total_claims"),
        F.sum(F.when(F.col("claim_status") == "OPEN", 1).otherwise(0)).alias("open_claims"),
        F.sum(F.when(F.col("claim_status") == "CLOSED", 1).otherwise(0)).alias("closed_claims"),
        F.avg("claim_amount").alias("avg_claim_amount"),
        F.min("claim_amount").alias("min_claim_amount"),
        F.max("claim_amount").alias("max_claim_amount"),
        F.avg("days_to_close").alias("avg_days_to_close")
    ).show(truncate=False)
    
    print("\n=== Claims by Severity ===")
    claims_df.groupBy("severity_category").agg(
        F.count("*").alias("claim_count"),
        F.avg("claim_amount").alias("avg_claim_amount"),
        F.sum("claim_amount").alias("total_claim_amount")
    ).orderBy("severity_category").show(truncate=False)
    
    print(f"\nClaims written to: {CLAIMS_OUT}")
    
    spark.stop()

if __name__ == "__main__":
    main()




