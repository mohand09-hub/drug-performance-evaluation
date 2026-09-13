"""
Gold Layer Analytics Transformation
Builds business-ready analytics tables from silver dimensional model
"""

import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, min as spark_min, max as spark_max,
    when, lit, current_timestamp, row_number, dense_rank,
    round as spark_round, coalesce, desc
)
from pyspark.sql.window import Window
from datetime import datetime

def load_config(config_path):
    """Load project configuration"""
    with open(config_path, 'r') as f:
        return json.load(f)

def load_gold_layer(spark, config):
    """
    Build Gold layer analytics tables
    
    Args:
        spark: SparkSession
        config: Project configuration dictionary
    """
    print("="*80)
    print("GOLD LAYER ANALYTICS TRANSFORMATION")
    print("="*80)
    
    # Extract config values
    catalog = config['unity_catalog']['catalog']
    silver_schema = config['unity_catalog']['schemas']['silver']
    gold_schema = config['unity_catalog']['schemas']['gold']
    landing_schema = config['unity_catalog']['schemas']['landing']
    
    # Table names
    dim_drugs_table = f"{catalog}.{silver_schema}.{config['tables']['silver']['dim_drugs']}"
    dim_conditions_table = f"{catalog}.{silver_schema}.{config['tables']['silver']['dim_conditions']}"
    fact_table = f"{catalog}.{silver_schema}.{config['tables']['silver']['fact_drug_performance']}"
    
    ranking_table = f"{catalog}.{gold_schema}.{config['tables']['gold']['drug_effectiveness_ranking']}"
    price_perf_table = f"{catalog}.{gold_schema}.{config['tables']['gold']['price_performance_analysis']}"
    condition_summary_table = f"{catalog}.{gold_schema}.{config['tables']['gold']['condition_treatment_summary']}"
    
    metadata_table = f"{catalog}.{landing_schema}.{config['tables']['landing']['metadata_tracking']}"
    
    print("\n1. Loading silver layer tables...")
    dim_drugs = spark.table(dim_drugs_table)
    dim_conditions = spark.table(dim_conditions_table)
    fact_performance = spark.table(fact_table)
    
    # Get only current records if SCD2 is enabled
    if config['scd2']['enabled']:
        dim_drugs = dim_drugs.filter(col(config['scd2']['is_current_column']) == True)
    
    print(f"✓ Loaded {dim_drugs.count():,} drugs, {dim_conditions.count():,} conditions, {fact_performance.count():,} performance records")
    
    print("\n2. Building drug_effectiveness_ranking analytics...")
    
    # Join facts with dimensions
    performance_enriched = (
        fact_performance.alias("fact")
        .join(dim_drugs.alias("drug"), col("fact.drug_key") == col("drug.drug_key"), "inner")
        .join(dim_conditions.alias("cond"), col("fact.condition_key") == col("cond.condition_key"), "inner")
        .select(
            col("cond.condition_name"),
            col("cond.condition_category"),
            col("drug.drug_name"),
            col("drug.drug_type"),
            col("drug.drug_form"),
            col("drug.average_price").alias("drug_avg_price"),
            col("fact.effectiveness_score"),
            col("fact.ease_of_use_score"),
            col("fact.satisfaction_score"),
            col("fact.review_count"),
            col("fact.price")
        )
    )
    
    # Aggregate by condition and drug
    drug_condition_agg = (
        performance_enriched
        .groupBy("condition_name", "drug_name", "drug_type", "drug_form")
        .agg(
            avg(col("effectiveness_score")).alias("effectiveness_score"),
            avg(col("ease_of_use_score")).alias("ease_of_use_score"),
            avg(col("satisfaction_score")).alias("satisfaction_score"),
            spark_sum(coalesce(col("review_count"), lit(0))).alias("total_reviews"),
            avg(coalesce(col("price"), col("drug_avg_price"))).alias("average_price")
        )
        .withColumn("overall_score",
                    spark_round(
                        (coalesce(col("effectiveness_score"), lit(0)) * 0.4 +
                         coalesce(col("satisfaction_score"), lit(0)) * 0.4 +
                         coalesce(col("ease_of_use_score"), lit(0)) * 0.2), 2))
    )
    
    # Add rankings
    window_effectiveness = Window.partitionBy("condition_name").orderBy(desc("effectiveness_score"))
    window_satisfaction = Window.partitionBy("condition_name").orderBy(desc("satisfaction_score"))
    window_overall = Window.partitionBy("condition_name").orderBy(desc("overall_score"))
    
    drug_effectiveness_ranking = (
        drug_condition_agg
        .withColumn("effectiveness_rank", dense_rank().over(window_effectiveness))
        .withColumn("satisfaction_rank", dense_rank().over(window_satisfaction))
        .withColumn("overall_rank", dense_rank().over(window_overall))
        .withColumn("last_updated", current_timestamp())
        .select(
            "condition_name", "drug_name", "drug_type", "drug_form",
            "effectiveness_score", "ease_of_use_score", "satisfaction_score", "overall_score",
            "effectiveness_rank", "satisfaction_rank", "overall_rank",
            "total_reviews", "average_price", "last_updated"
        )
    )
    
    drug_effectiveness_ranking.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ranking_table)
    ranking_count = drug_effectiveness_ranking.count()
    print(f"✓ Loaded {ranking_count:,} drug-condition rankings")
    
    print("\n3. Building price_performance_analysis analytics...")
    
    # Aggregate drug performance across all conditions
    drug_overall_performance = (
        performance_enriched
        .groupBy("drug_name", "drug_type")
        .agg(
            avg(col("effectiveness_score")).alias("avg_effectiveness"),
            avg(col("ease_of_use_score")).alias("avg_ease_of_use"),
            avg(col("satisfaction_score")).alias("avg_satisfaction"),
            avg(coalesce(col("price"), col("drug_avg_price"))).alias("average_price"),
            spark_sum(coalesce(col("review_count"), lit(0))).alias("total_reviews"),
            count("condition_name").alias("conditions_treated")
        )
        .filter(col("average_price").isNotNull())
    )
    
    # Add price tiers and value scores
    price_performance = (
        drug_overall_performance
        .withColumn("price_tier",
                    when(col("average_price") < 50, "Low (<$50)")
                    .when(col("average_price") < 150, "Medium ($50-$150)")
                    .otherwise("High (>$150)"))
        .withColumn("value_score",
                    spark_round(
                        when(col("average_price") > 0,
                             coalesce(col("avg_effectiveness"), lit(0)) / col("average_price") * 100)
                        .otherwise(0), 4))
    )
    
    # Calculate percentiles
    window_price = Window.orderBy("average_price")
    window_performance = Window.orderBy(desc("avg_effectiveness"))
    
    price_performance_analysis = (
        price_performance
        .withColumn("price_percentile",
                    spark_round((row_number().over(window_price) / count("*").over(Window.partitionBy()) * 100), 0).cast("int"))
        .withColumn("performance_percentile",
                    spark_round((row_number().over(window_performance) / count("*").over(Window.partitionBy()) * 100), 0).cast("int"))
        .withColumn("last_updated", current_timestamp())
        .select(
            "drug_name", "drug_type", "price_tier", "average_price",
            "avg_effectiveness", "avg_ease_of_use", "avg_satisfaction",
            "value_score", "price_percentile", "performance_percentile",
            "total_reviews", "conditions_treated", "last_updated"
        )
    )
    
    price_performance_analysis.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(price_perf_table)
    price_count = price_performance_analysis.count()
    print(f"✓ Loaded {price_count:,} price-performance records")
    
    print("\n4. Building condition_treatment_summary analytics...")
    
    # Aggregate condition-level statistics
    condition_summary = (
        performance_enriched
        .groupBy("condition_name", "condition_category")
        .agg(
            count("drug_name").alias("total_drugs_available"),
            spark_sum(when(col("drug_type") == "RX", 1).otherwise(0)).alias("rx_drugs_count"),
            spark_sum(when(col("drug_type").isin(["OTC", "RX/OTC"]), 1).otherwise(0)).alias("otc_drugs_count"),
            spark_sum(when(col("drug_form").isin(["Tablet", "Capsule"]), 1).otherwise(0)).alias("tablet_options"),
            spark_sum(when(col("drug_form") == "Liquid", 1).otherwise(0)).alias("liquid_options"),
            spark_sum(when(col("drug_form").isin(["Cream", "Gel", "Ointment"]), 1).otherwise(0)).alias("topical_options"),
            spark_sum(when(col("drug_form") == "Injectable", 1).otherwise(0)).alias("injectable_options"),
            avg(col("effectiveness_score")).alias("avg_effectiveness"),
            avg(col("satisfaction_score")).alias("avg_satisfaction"),
            spark_min(coalesce(col("price"), col("drug_avg_price"))).alias("min_price"),
            spark_max(coalesce(col("price"), col("drug_avg_price"))).alias("max_price"),
            avg(coalesce(col("price"), col("drug_avg_price"))).alias("avg_price"),
            spark_sum(coalesce(col("review_count"), lit(0))).alias("total_patient_reviews")
        )
    )
    
    # Get best drug per condition
    window_best_drug = Window.partitionBy("condition_name").orderBy(desc("effectiveness_score"))
    best_drugs = (
        performance_enriched
        .withColumn("rank", row_number().over(window_best_drug))
        .filter(col("rank") == 1)
        .select(col("condition_name"), col("drug_name").alias("best_drug_by_effectiveness"))
    )
    
    condition_treatment_summary = (
        condition_summary
        .join(best_drugs, "condition_name", "left")
        .withColumn("last_updated", current_timestamp())
        .select(
            "condition_name", "condition_category",
            "total_drugs_available", "rx_drugs_count", "otc_drugs_count",
            "tablet_options", "liquid_options", "topical_options", "injectable_options",
            "avg_effectiveness", "avg_satisfaction",
            "best_drug_by_effectiveness",
            "min_price", "max_price", "avg_price",
            "total_patient_reviews",
            "last_updated"
        )
    )
    
    condition_treatment_summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(condition_summary_table)
    condition_count = condition_treatment_summary.count()
    print(f"✓ Loaded {condition_count:,} condition summaries")
    
    # Update metadata
    print("\n5. Updating metadata tracking...")
    metadata_records = [
        (config['tables']['gold']['drug_effectiveness_ranking'], "silver tables", ranking_count, "gold", "success", "Drug rankings by condition"),
        (config['tables']['gold']['price_performance_analysis'], "silver tables", price_count, "gold", "success", "Price vs performance analysis"),
        (config['tables']['gold']['condition_treatment_summary'], "silver tables", condition_count, "gold", "success", "Condition treatment options")
    ]
    
    metadata_df = spark.createDataFrame(
        [(table, file, count, datetime.now(), layer, status, notes) 
         for table, file, count, layer, status, notes in metadata_records],
        ["table_name", "source_file", "record_count", "load_timestamp", "layer", "status", "notes"]
    )
    metadata_df.write.mode("append").saveAsTable(metadata_table)
    print("✓ Metadata tracking updated")
    
    print("\n" + "="*80)
    print("GOLD LAYER TRANSFORMATION COMPLETE")
    print("="*80)
    print(f"✅ drug_effectiveness_ranking: {ranking_count:,} records")
    print(f"✅ price_performance_analysis: {price_count:,} records")
    print(f"✅ condition_treatment_summary: {condition_count:,} records")
    
    return {"ranking_count": ranking_count, "price_count": price_count, "condition_count": condition_count}

def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: python gold_analytics.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    config = load_config(config_path)
    
    spark = SparkSession.builder.appName("Gold Layer Analytics").getOrCreate()
    
    try:
        result = load_gold_layer(spark, config)
        print(f"\n✅ Gold layer processing completed successfully")
        return 0
    except Exception as e:
        print(f"\n❌ Error in gold layer processing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    sys.exit(main())
