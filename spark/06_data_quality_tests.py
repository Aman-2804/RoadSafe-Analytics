from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_BASE = "data/gold"
SILVER_PATH = "data/silver/collisions"

def run_test(test_name, condition, df, error_message):
    """Run a data quality test and report results"""
    try:
        failed_count = df.filter(~condition).count()
        total_count = df.count()
        passed = failed_count == 0
        
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} | {test_name}")
        if not passed:
            print(f"   Failed rows: {failed_count} / {total_count}")
            print(f"   Error: {error_message}")
        else:
            print(f"   All {total_count} rows passed")
        return passed
    except Exception as e:
        print(f"❌ ERROR | {test_name}")
        print(f"   Exception: {str(e)}")
        return False

def main():
    spark = SparkSession.builder.appName("roadsafe-data-quality-tests").getOrCreate()
    
    print("=" * 80)
    print("ROADSAFE DATA QUALITY TESTS")
    print("=" * 80)
    print()
    
    # Load all tables
    print("Loading data tables...")
    fact_collisions = spark.read.parquet(f"{GOLD_BASE}/fact_collisions")
    
    # Load optional tables (may not exist if not run yet)
    try:
        fact_claims = spark.read.parquet(f"{GOLD_BASE}/fact_claims")
        has_claims = True
    except Exception:
        print("  ⚠️  fact_claims not found - skipping claims tests")
        fact_claims = None
        has_claims = False
    
    dim_time = spark.read.parquet(f"{GOLD_BASE}/dim_time")
    dim_location = spark.read.parquet(f"{GOLD_BASE}/dim_location")
    dim_vehicle = spark.read.parquet(f"{GOLD_BASE}/dim_vehicle")
    dim_factor = spark.read.parquet(f"{GOLD_BASE}/dim_factor")
    silver_df = spark.read.parquet(SILVER_PATH)
    
    print(f"Loaded tables:")
    print(f"  - fact_collisions: {fact_collisions.count()} rows")
    if has_claims:
        print(f"  - fact_claims: {fact_claims.count()} rows")
    print(f"  - dim_time: {dim_time.count()} rows")
    print(f"  - dim_location: {dim_location.count()} rows")
    print(f"  - dim_vehicle: {dim_vehicle.count()} rows")
    print(f"  - dim_factor: {dim_factor.count()} rows")
    print(f"  - silver: {silver_df.count()} rows")
    print()
    
    results = []
    
    # ============================================================================
    # FACT TABLE TESTS
    # ============================================================================
    print("=" * 80)
    print("FACT TABLE TESTS")
    print("=" * 80)
    print()
    
    # Test 1: No null collision_id
    results.append(run_test(
        "fact_collisions: No null collision_id",
        F.col("collision_id").isNotNull(),
        fact_collisions,
        "collision_id cannot be null"
    ))
    
    # Test 2: No negative injury counts
    results.append(run_test(
        "fact_collisions: No negative injury counts",
        (F.col("persons_injured") >= 0) &
        (F.col("persons_killed") >= 0) &
        (F.col("pedestrians_injured") >= 0) &
        (F.col("pedestrians_killed") >= 0) &
        (F.col("cyclist_injured") >= 0) &
        (F.col("cyclist_killed") >= 0) &
        (F.col("motorist_injured") >= 0) &
        (F.col("motorist_killed") >= 0),
        fact_collisions,
        "Injury/fatality counts cannot be negative"
    ))
    
    # Test 3-6: Claims tests (only if fact_claims exists)
    if has_claims:
        results.append(run_test(
            "fact_claims: No null claim_id",
            F.col("claim_id").isNotNull(),
            fact_claims,
            "claim_id cannot be null"
        ))
        
        results.append(run_test(
            "fact_claims: No negative claim amounts",
            F.col("claim_amount") >= 0,
            fact_claims,
            "claim_amount cannot be negative"
        ))
        
        results.append(run_test(
            "fact_claims: Valid claim_status values",
            F.col("claim_status").isin(["OPEN", "CLOSED"]),
            fact_claims,
            "claim_status must be 'OPEN' or 'CLOSED'"
        ))
        
        results.append(run_test(
            "fact_claims: days_to_close NULL for OPEN claims",
            (F.col("claim_status") == "OPEN") & (F.col("days_to_close").isNull()) |
            (F.col("claim_status") == "CLOSED") & (F.col("days_to_close").isNotNull()),
            fact_claims,
            "OPEN claims must have NULL days_to_close, CLOSED claims must have value"
        ))
    else:
        print("  ⏭️  Skipping claims tests (fact_claims not found)")
        results.extend([None, None, None, None])  # Placeholders to maintain test count
    
    # ============================================================================
    # DIMENSION TABLE TESTS
    # ============================================================================
    print()
    print("=" * 80)
    print("DIMENSION TABLE TESTS")
    print("=" * 80)
    print()
    
    # Test 7: Dimension keys are unique
    time_duplicates = dim_time.groupBy("time_key").count().filter(F.col("count") > 1).count()
    if time_duplicates == 0:
        print("✅ PASS | dim_time: time_key is unique")
        print(f"   All {dim_time.count()} rows have unique time_key")
        results.append(True)
    else:
        print(f"❌ FAIL | dim_time: time_key is unique")
        print(f"   Found {time_duplicates} duplicate time_key values")
        results.append(False)
    
    location_duplicates = dim_location.groupBy("location_key").count().filter(F.col("count") > 1).count()
    if location_duplicates == 0:
        print("✅ PASS | dim_location: location_key is unique")
        print(f"   All {dim_location.count()} rows have unique location_key")
        results.append(True)
    else:
        print(f"❌ FAIL | dim_location: location_key is unique")
        print(f"   Found {location_duplicates} duplicate location_key values")
        results.append(False)
    
    vehicle_duplicates = dim_vehicle.groupBy("vehicle_key").count().filter(F.col("count") > 1).count()
    if vehicle_duplicates == 0:
        print("✅ PASS | dim_vehicle: vehicle_key is unique")
        print(f"   All {dim_vehicle.count()} rows have unique vehicle_key")
        results.append(True)
    else:
        print(f"❌ FAIL | dim_vehicle: vehicle_key is unique")
        print(f"   Found {vehicle_duplicates} duplicate vehicle_key values")
        results.append(False)
    
    factor_duplicates = dim_factor.groupBy("factor_key").count().filter(F.col("count") > 1).count()
    if factor_duplicates == 0:
        print("✅ PASS | dim_factor: factor_key is unique")
        print(f"   All {dim_factor.count()} rows have unique factor_key")
        results.append(True)
    else:
        print(f"❌ FAIL | dim_factor: factor_key is unique")
        print(f"   Found {factor_duplicates} duplicate factor_key values")
        results.append(False)
    
    # Test 8: Dimension keys are not null
    results.append(run_test(
        "dim_time: time_key is not null",
        F.col("time_key").isNotNull(),
        dim_time,
        "time_key cannot be null"
    ))
    
    results.append(run_test(
        "dim_location: location_key is not null",
        F.col("location_key").isNotNull(),
        dim_location,
        "location_key cannot be null"
    ))
    
    # ============================================================================
    # REFERENTIAL INTEGRITY TESTS
    # ============================================================================
    print()
    print("=" * 80)
    print("REFERENTIAL INTEGRITY TESTS")
    print("=" * 80)
    print()
    
    # Test 9: Fact foreign keys exist in dimensions
    time_keys_in_fact = fact_collisions.select("time_key").distinct()
    time_keys_in_dim = dim_time.select("time_key").distinct()
    missing_time_keys = time_keys_in_fact.join(time_keys_in_dim, "time_key", "left_anti").count()
    if missing_time_keys == 0:
        print("✅ PASS | fact_collisions.time_key exists in dim_time")
        print(f"   All time_key values in fact exist in dim_time")
        results.append(True)
    else:
        print(f"❌ FAIL | fact_collisions.time_key exists in dim_time")
        print(f"   Found {missing_time_keys} time_key values in fact that don't exist in dim_time")
        results.append(False)
    
    location_keys_in_fact = fact_collisions.select("location_key").distinct()
    location_keys_in_dim = dim_location.select("location_key").distinct()
    missing_location_keys = location_keys_in_fact.join(location_keys_in_dim, "location_key", "left_anti").count()
    if missing_location_keys == 0:
        print("✅ PASS | fact_collisions.location_key exists in dim_location")
        print(f"   All location_key values in fact exist in dim_location")
        results.append(True)
    else:
        print(f"❌ FAIL | fact_collisions.location_key exists in dim_location")
        print(f"   Found {missing_location_keys} location_key values in fact that don't exist in dim_location")
        results.append(False)
    
    # Test 10: Claims reference valid collisions (only if fact_claims exists)
    if has_claims:
        collision_ids_in_claims = fact_claims.select("collision_id").distinct()
        collision_ids_in_fact = fact_collisions.select("collision_id").distinct()
        missing_collision_ids = collision_ids_in_claims.join(collision_ids_in_fact, "collision_id", "left_anti").count()
        if missing_collision_ids == 0:
            print("✅ PASS | fact_claims.collision_id exists in fact_collisions")
            print(f"   All collision_id values in claims exist in collisions")
            results.append(True)
        else:
            print(f"❌ FAIL | fact_claims.collision_id exists in fact_collisions")
            print(f"   Found {missing_collision_ids} collision_id values in claims that don't exist in collisions")
            results.append(False)
    else:
        print("  ⏭️  Skipping claims referential integrity test (fact_claims not found)")
        results.append(None)
    
    # ============================================================================
    # DATA CONSISTENCY TESTS
    # ============================================================================
    print()
    print("=" * 80)
    print("DATA CONSISTENCY TESTS")
    print("=" * 80)
    print()
    
    # Test 11: Row counts make sense (Silver vs Gold)
    silver_count = silver_df.count()
    gold_count = fact_collisions.count()
    # Gold should have same or fewer rows (after filtering)
    if gold_count <= silver_count:
        print("✅ PASS | Row count: Gold <= Silver (after filtering)")
        print(f"   Gold: {gold_count:,} rows, Silver: {silver_count:,} rows")
        results.append(True)
    else:
        print(f"❌ FAIL | Row count: Gold <= Silver (after filtering)")
        print(f"   Gold has {gold_count:,} rows but Silver has {silver_count:,} rows (Gold should be <= Silver)")
        results.append(False)
    
    # Test 12: Claims count <= Collisions count (only if fact_claims exists)
    if has_claims:
        claims_count = fact_claims.count()
        collisions_count = fact_collisions.count()
        if claims_count <= collisions_count:
            print("✅ PASS | Row count: Claims <= Collisions")
            print(f"   Claims: {claims_count:,} rows, Collisions: {collisions_count:,} rows")
            results.append(True)
        else:
            print(f"❌ FAIL | Row count: Claims <= Collisions")
            print(f"   Claims has {claims_count:,} rows but Collisions has {collisions_count:,} rows (Claims should be <= Collisions)")
            results.append(False)
    else:
        print("  ⏭️  Skipping claims count test (fact_claims not found)")
        results.append(None)
    
    # Test 13: Total persons affected matches sum of components
    results.append(run_test(
        "fact_collisions: total_persons_affected matches sum of components",
        F.col("total_persons_affected") == (
            F.col("persons_injured") + F.col("persons_killed")
        ),
        fact_collisions,
        "total_persons_affected should equal persons_injured + persons_killed"
    ))
    
    # Test 14: Severity category matches injury counts
    results.append(run_test(
        "fact_collisions: severity_category matches injury counts",
        ((F.col("persons_killed") > 0) & (F.col("severity_category") == "Fatal")) |
        ((F.col("persons_killed") == 0) & (F.col("persons_injured") > 0) & (F.col("severity_category") == "Injury")) |
        ((F.col("persons_killed") == 0) & (F.col("persons_injured") == 0) & (F.col("severity_category") == "Property Damage Only")),
        fact_collisions,
        "severity_category should match actual injury/fatality counts"
    ))
    
    # ============================================================================
    # SUMMARY
    # ============================================================================
    print()
    print("=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print()
    
    # Filter out None results (skipped tests)
    valid_results = [r for r in results if r is not None]
    total_tests = len(valid_results)
    passed_tests = sum(valid_results)
    failed_tests = total_tests - passed_tests
    
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests} ✅")
    print(f"Failed: {failed_tests} ❌")
    print()
    
    if failed_tests == 0:
        print("🎉 ALL TESTS PASSED! Data quality is excellent.")
    else:
        print(f"⚠️  {failed_tests} test(s) failed. Please review the errors above.")
    
    print()
    print("=" * 80)
    
    spark.stop()
    
    # Exit with appropriate code
    exit(0 if failed_tests == 0 else 1)

if __name__ == "__main__":
    main()

