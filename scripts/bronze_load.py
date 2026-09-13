"""
Bronze Layer Data Ingestion Script
Loads raw CSV files from Unity Catalog volume into Bronze tables
"""

import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, md5, concat_ws, expr
from datetime import datetime

def load_config(config_path):
    """Load project configuration"""
    with open(config_path, 'r') as f:
        return json.load(f)

def load_bronze_layer(spark, config):
    """
    Load raw CSV files into Bronze tables
    
    Args:
        spark: SparkSession
        config: Project configuration dictionary
    """
    print("="*80)
    print("BRONZE LAYER DATA INGESTION")
    print("="*80)
    
    # Extract config values
    catalog = config['unity_catalog']['catalog']
    bronze_schema = config['unity_catalog']['schemas']['bronze']
    landing_schema = config['unity_catalog']['schemas']['landing']
    volume_path = config['unity_catalog']['volume']['path']
    raw_file = config['data_files']['raw_reviews']
    clean_file = config['data_files']['clean_reviews']
    
    # Table names
    raw_table = f"{catalog}.{bronze_schema}.{config['tables']['bronze']['drug_reviews_raw']}"
    clean_table = f"{catalog}.{bronze_schema}.{config['tables']['bronze']['drug_reviews_clean']}"
    metadata_table = f"{catalog}.{landing_schema}.{config['tables']['landing']['metadata_tracking']}"
    
    print(f"\n1. Loading raw reviews from {volume_path}/{raw_file}...")
    
    # Load raw reviews
    df_raw = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(f"{volume_path}/{raw_file}"))
    
    # Add metadata columns
    df_raw = (df_raw
              .withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("source_file", expr("'_metadata.file_path'"))  # Placeholder - actual path in production
              .withColumn("record_id", md5(concat_ws("||", *df_raw.columns))))
    
    # Write to bronze
    df_raw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(raw_table)
    raw_count = df_raw.count()
    print(f"✓ Loaded {raw_count:,} records to {raw_table}")
    
    print(f"\n2. Loading clean reviews from {volume_path}/{clean_file}...")
    
    # Load clean reviews
    df_clean = (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(f"{volume_path}/{clean_file}"))
    
    # Add metadata columns
    df_clean = (df_clean
                .withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("source_file", expr("'_metadata.file_path'"))
                .withColumn("record_id", md5(concat_ws("||", *df_clean.columns))))
    
    # Write to bronze
    df_clean.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(clean_table)
    clean_count = df_clean.count()
    print(f"✓ Loaded {clean_count:,} records to {clean_table}")
    
    # Update metadata tracking
    print("\n3. Updating metadata tracking...")
    metadata_records = [
        (config['tables']['bronze']['drug_reviews_raw'], raw_file, raw_count, "bronze", "success", "Raw drug reviews loaded"),
        (config['tables']['bronze']['drug_reviews_clean'], clean_file, clean_count, "bronze", "success", "Clean drug reviews loaded")
    ]
    
    metadata_df = spark.createDataFrame(
        [(table, file, count, datetime.now(), layer, status, notes) 
         for table, file, count, layer, status, notes in metadata_records],
        ["table_name", "source_file", "record_count", "load_timestamp", "layer", "status", "notes"]
    )
    
    metadata_df.write.mode("append").saveAsTable(metadata_table)
    print("✓ Metadata tracking updated")
    
    # Verification
    print("\n" + "="*80)
    print("BRONZE LAYER INGESTION COMPLETE")
    print("="*80)
    print(f"✅ {raw_table}: {raw_count:,} records")
    print(f"✅ {clean_table}: {clean_count:,} records")
    print(f"✅ Total: {raw_count + clean_count:,} records")
    
    return {"raw_count": raw_count, "clean_count": clean_count}

def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: python bronze_load.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    config = load_config(config_path)
    
    spark = SparkSession.builder.appName("Bronze Layer Load").getOrCreate()
    
    try:
        result = load_bronze_layer(spark, config)
        print(f"\n✅ Bronze layer processing completed successfully")
        return 0
    except Exception as e:
        print(f"\n❌ Error in bronze layer processing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
