"""
Silver Layer Data Transformation with SCD Type 2 Tracking
Cleans, standardizes, and loads dimensional model with historical tracking
"""

import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, regexp_replace, when, current_timestamp, current_date,
    md5, concat_ws, first, count, sum as spark_sum, avg,
    coalesce, lit, initcap, lower, expr, regexp_extract, to_date
)
from pyspark.sql.types import StringType
from datetime import datetime, date

def load_config(config_path):
    """Load project configuration"""
    with open(config_path, 'r') as f:
        return json.load(f)

def apply_scd2_merge(spark, new_data_df, target_table, business_keys, tracked_columns, config):
    """
    Apply SCD Type 2 merge logic
    
    Args:
        spark: SparkSession
        new_data_df: DataFrame with new/updated records
        target_table: Target table name (fully qualified)
        business_keys: List of business key columns
        tracked_columns: List of columns to track for changes
        config: SCD2 configuration
    """
    effective_date_col = config['scd2']['effective_date_column']
    end_date_col = config['scd2']['end_date_column']
    is_current_col = config['scd2']['is_current_column']
    
    # Check if target table exists
    try:
        existing_df = spark.table(target_table)
        table_exists = True
    except Exception:
        table_exists = False
    
    if not table_exists:
        # First load - all records are current
        result_df = (new_data_df
                     .withColumn(effective_date_col, current_date())
                     .withColumn(end_date_col, lit(None).cast("date"))
                     .withColumn(is_current_col, lit(True)))
        
        result_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        print(f"  ✓ Initial load: {result_df.count():,} records")
        return result_df.count()
    
    # Subsequent loads - perform SCD2 merge
    print(f"  → Applying SCD Type 2 merge...")
    
    # Get current records from target
    current_records = existing_df.filter(col(is_current_col) == True)
    
    # Build join condition
    join_condition = None
    for key in business_keys:
        condition = col(f"new.{key}") == col(f"current.{key}")
        join_condition = condition if join_condition is None else (join_condition & condition)
    
    # Compare for changes in tracked columns
    change_condition = None
    for tracked_col in tracked_columns:
        condition = (
            (col(f"new.{tracked_col}") != col(f"current.{tracked_col}")) |
            (col(f"new.{tracked_col}").isNull() & col(f"current.{tracked_col}").isNotNull()) |
            (col(f"new.{tracked_col}").isNotNull() & col(f"current.{tracked_col}").isNull())
        )
        change_condition = condition if change_condition is None else (change_condition | condition)
    
    # Identify records that changed
    comparison_df = (new_data_df.alias("new")
                     .join(current_records.alias("current"), join_condition, "left"))
    
    # New records (not in current)
    new_records = (comparison_df
                   .filter(col(f"current.{business_keys[0]}").isNull())
                   .select("new.*")
                   .withColumn(effective_date_col, current_date())
                   .withColumn(end_date_col, lit(None).cast("date"))
                   .withColumn(is_current_col, lit(True)))
    
    # Changed records - expire old and create new version
    changed_new_keys = (comparison_df
                        .filter(col(f"current.{business_keys[0]}").isNotNull())
                        .filter(change_condition)
                        .select([col(f"current.{k}").alias(k) for k in business_keys]))
    
    # Expire changed records
    expired_records = (current_records
                       .join(changed_new_keys, business_keys, "inner")
                       .withColumn(end_date_col, current_date())
                       .withColumn(is_current_col, lit(False)))
    
    # New versions of changed records
    new_versions = (comparison_df
                    .filter(col(f"current.{business_keys[0]}").isNotNull())
                    .filter(change_condition)
                    .select("new.*")
                    .withColumn(effective_date_col, current_date())
                    .withColumn(end_date_col, lit(None).cast("date"))
                    .withColumn(is_current_col, lit(True)))
    
    # Unchanged records - keep as is
    unchanged_records = (current_records
                         .join(changed_new_keys, business_keys, "left_anti"))
    
    # Historical records (not current) - keep as is
    historical_records = existing_df.filter(col(is_current_col) == False)
    
    # Union all parts
    final_df = (historical_records
                .unionByName(unchanged_records, allowMissingColumns=True)
                .unionByName(expired_records, allowMissingColumns=True)
                .unionByName(new_records, allowMissingColumns=True)
                .unionByName(new_versions, allowMissingColumns=True))
    
    # Write result
    final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    
    new_count = new_records.count()
    changed_count = new_versions.count()
    total_count = final_df.count()
    
    print(f"  ✓ SCD2 merge complete: {new_count:,} new, {changed_count:,} changed, {total_count:,} total")
    return total_count

def load_silver_layer(spark, config):
    """
    Load and transform data into Silver layer with SCD2 tracking
    
    Args:
        spark: SparkSession
        config: Project configuration dictionary
    """
    print("="*80)
    print("SILVER LAYER DATA TRANSFORMATION (with SCD2)")
    print("="*80)
    
    # Extract config values
    catalog = config['unity_catalog']['catalog']
    bronze_schema = config['unity_catalog']['schemas']['bronze']
    silver_schema = config['unity_catalog']['schemas']['silver']
    landing_schema = config['unity_catalog']['schemas']['landing']
    
    # Table names
    raw_table = f"{catalog}.{bronze_schema}.{config['tables']['bronze']['drug_reviews_raw']}"
    clean_table = f"{catalog}.{bronze_schema}.{config['tables']['bronze']['drug_reviews_clean']}"
    dim_drugs_table = f"{catalog}.{silver_schema}.{config['tables']['silver']['dim_drugs']}"
    dim_conditions_table = f"{catalog}.{silver_schema}.{config['tables']['silver']['dim_conditions']}"
    fact_table = f"{catalog}.{silver_schema}.{config['tables']['silver']['fact_drug_performance']}"
    metadata_table = f"{catalog}.{landing_schema}.{config['tables']['landing']['metadata_tracking']}"
    
    print("\n1. Loading bronze tables...")
    df_raw = spark.table(raw_table)
    df_clean = spark.table(clean_table)
    print(f"✓ Loaded {df_raw.count():,} raw + {df_clean.count():,} clean records")
    
    print("\n2. Cleansing and standardizing data...")
    
    # Clean raw dataset
    df_raw_cleaned = (
        df_raw
        .withColumn("review_count", expr("try_cast(regexp_extract(reviews, '^([0-9]+)', 1) as int)"))
        .withColumn("drug_type_clean", trim(regexp_replace(col("type"), "\\r\\n", "")))
        .withColumn("condition_clean", initcap(trim(col("condition"))))
        .withColumn("drug_clean", trim(col("drug")))
        .withColumn("effective_clean", expr("try_cast(effective as double)"))
        .withColumn("ease_of_use_clean", expr("try_cast(ease_of_use as double)"))
        .withColumn("satisfaction_clean", expr("try_cast(satisfaction as double)"))
    )
    
    # Clean aggregated dataset
    df_clean_cleaned = (
        df_clean
        .withColumn("drug_type_clean", 
                    when(col("type").isNull(), "Unknown").otherwise(trim(col("type"))))
        .withColumn("condition_clean", initcap(trim(col("condition"))))
        .withColumn("drug_clean", trim(col("drug")))
        .withColumn("review_count", col("reviews").cast("int"))
    )
    
    print("✓ Data cleansing complete")
    
    print("\n3. Building dim_drugs dimension with SCD2...")
    
    # Aggregate drug information
    drugs_from_raw = (
        df_raw_cleaned
        .groupBy("drug_clean", "drug_type_clean")
        .agg(
            first("information").alias("drug_information"),
            count("*").alias("mention_count")
        )
    )
    
    drugs_from_clean = (
        df_clean_cleaned
        .groupBy("drug_clean", "drug_type_clean", "form")
        .agg(
            avg("price").alias("average_price"),
            spark_sum("review_count").alias("total_reviews")
        )
    )
    
    # Combine and build final dimension
    dim_drugs_new = (
        drugs_from_raw
        .join(drugs_from_clean, on=["drug_clean", "drug_type_clean"], how="full_outer")
        .select(
            md5(col("drug_clean")).alias("drug_key"),
            col("drug_clean").alias("drug_name"),
            coalesce(col("drug_type_clean"), lit("Unknown")).alias("drug_type"),
            coalesce(col("form"), lit("Unknown")).alias("drug_form"),
            col("average_price"),
            col("drug_information")
        )
        .dropDuplicates(["drug_key"])
    )
    
    # Apply SCD2 merge if enabled
    if config['scd2']['enabled'] and 'dim_drugs' in config['scd2']['tracked_tables']:
        drug_count = apply_scd2_merge(
            spark, dim_drugs_new, dim_drugs_table,
            config['scd2']['business_keys']['dim_drugs'],
            config['scd2']['tracked_columns']['dim_drugs'],
            config
        )
    else:
        dim_drugs_new.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dim_drugs_table)
        drug_count = dim_drugs_new.count()
        print(f"  ✓ Loaded {drug_count:,} drugs (no SCD2)")
    
    print("\n4. Building dim_conditions dimension...")
    
    # Categorize conditions
    def categorize_condition(condition_name):
        condition_lower = condition_name.lower() if condition_name else ""
        if any(word in condition_lower for word in ["pain", "ache", "arthritis"]):
            return "Pain Management"
        elif any(word in condition_lower for word in ["hypertension", "heart", "cardiovascular"]):
            return "Cardiovascular"
        elif any(word in condition_lower for word in ["dermatitis", "skin", "eczema", "psoriasis"]):
            return "Dermatology"
        elif any(word in condition_lower for word in ["diabetes", "glucose"]):
            return "Metabolic"
        elif any(word in condition_lower for word in ["infection", "bacterial", "viral"]):
            return "Infectious Disease"
        elif any(word in condition_lower for word in ["gout", "uric"]):
            return "Rheumatology"
        elif any(word in condition_lower for word in ["reflux", "gerd", "gastro"]):
            return "Gastrointestinal"
        else:
            return "Other"
    
    categorize_udf = spark.udf.register("categorize_condition", categorize_condition, StringType())
    
    conditions_combined = (
        df_raw_cleaned.select("condition_clean", "drug_clean", coalesce(col("review_count"), lit(0)).alias("review_count"))
        .union(df_clean_cleaned.select("condition_clean", "drug_clean", coalesce(col("review_count"), lit(0)).alias("review_count")))
    )
    
    dim_conditions = (
        conditions_combined
        .groupBy("condition_clean")
        .agg(
            count("drug_clean").alias("total_drugs_available"),
            spark_sum("review_count").alias("total_reviews_count")
        )
        .select(
            md5(col("condition_clean")).alias("condition_key"),
            col("condition_clean").alias("condition_name"),
            categorize_udf(col("condition_clean")).alias("condition_category"),
            lit("Standard").alias("indication_type"),
            col("total_drugs_available"),
            col("total_reviews_count"),
            current_timestamp().alias("first_seen_date"),
            current_timestamp().alias("last_updated_date")
        )
        .dropDuplicates(["condition_key"])
    )
    
    dim_conditions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dim_conditions_table)
    condition_count = dim_conditions.count()
    print(f"✓ Loaded {condition_count:,} conditions")
    
    print("\n5. Building fact_drug_performance fact table...")
    
    # Fact from raw
    fact_from_raw = (
        df_raw_cleaned.select(
            md5(col("drug_clean")).alias("drug_key"),
            md5(col("condition_clean")).alias("condition_key"),
            col("effective_clean").alias("effectiveness_score"),
            col("ease_of_use_clean").alias("ease_of_use_score"),
            col("satisfaction_clean").alias("satisfaction_score"),
            col("review_count"),
            lit(None).cast("string").alias("drug_form"),
            col("drug_type_clean").alias("drug_type"),
            lit(None).cast("double").alias("price"),
            lit("raw").alias("data_source"),
            col("ingestion_timestamp").alias("load_timestamp")
        )
        .withColumn("performance_key", md5(concat_ws("||", col("drug_key"), col("condition_key"), col("data_source"))))
    )
    
    # Fact from clean
    fact_from_clean = (
        df_clean_cleaned.select(
            md5(col("drug_clean")).alias("drug_key"),
            md5(col("condition_clean")).alias("condition_key"),
            col("effective").alias("effectiveness_score"),
            col("ease_of_use").alias("ease_of_use_score"),
            col("satisfaction").alias("satisfaction_score"),
            col("review_count"),
            col("form").alias("drug_form"),
            col("drug_type_clean").alias("drug_type"),
            col("price"),
            lit("clean").alias("data_source"),
            col("ingestion_timestamp").alias("load_timestamp")
        )
        .withColumn("performance_key", md5(concat_ws("||", col("drug_key"), col("condition_key"), col("data_source"))))
    )
    
    fact_drug_performance = fact_from_raw.union(fact_from_clean)
    fact_drug_performance.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fact_table)
    fact_count = fact_drug_performance.count()
    print(f"✓ Loaded {fact_count:,} performance records")
    
    # Update metadata
    print("\n6. Updating metadata tracking...")
    metadata_records = [
        (config['tables']['silver']['dim_drugs'], "bronze tables", drug_count, "silver", "success", "Dimension with SCD2 tracking"),
        (config['tables']['silver']['dim_conditions'], "bronze tables", condition_count, "silver", "success", "Dimension table"),
        (config['tables']['silver']['fact_drug_performance'], "bronze tables", fact_count, "silver", "success", "Fact table")
    ]
    
    metadata_df = spark.createDataFrame(
        [(table, file, count, datetime.now(), layer, status, notes) 
         for table, file, count, layer, status, notes in metadata_records],
        ["table_name", "source_file", "record_count", "load_timestamp", "layer", "status", "notes"]
    )
    metadata_df.write.mode("append").saveAsTable(metadata_table)
    print("✓ Metadata tracking updated")
    
    print("\n" + "="*80)
    print("SILVER LAYER TRANSFORMATION COMPLETE")
    print("="*80)
    print(f"✅ dim_drugs: {drug_count:,} records (SCD2 enabled)")
    print(f"✅ dim_conditions: {condition_count:,} records")
    print(f"✅ fact_drug_performance: {fact_count:,} records")
    
    return {"drug_count": drug_count, "condition_count": condition_count, "fact_count": fact_count}

def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: python silver_transform.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    config = load_config(config_path)
    
    spark = SparkSession.builder.appName("Silver Layer Transform").getOrCreate()
    
    try:
        result = load_silver_layer(spark, config)
        print(f"\n✅ Silver layer processing completed successfully")
        return 0
    except Exception as e:
        print(f"\n❌ Error in silver layer processing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
