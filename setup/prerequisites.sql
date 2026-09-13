-- ============================================================================
-- Drug Performance Evaluation - Unity Catalog Setup
-- Prerequisites for Bronze, Silver, and Gold layers with SCD2 tracking
-- ============================================================================

-- ============================================================================
-- STEP 1: Create Unity Catalog (if not exists)
-- ============================================================================
CREATE CATALOG IF NOT EXISTS drug_evaluation;
USE CATALOG drug_evaluation;

-- ============================================================================
-- STEP 2: Create Schemas (Medallion Architecture)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS landing 
COMMENT 'Landing zone for raw data and metadata tracking';

CREATE SCHEMA IF NOT EXISTS bronze 
COMMENT 'Bronze layer - raw ingested data with minimal transformation';

CREATE SCHEMA IF NOT EXISTS silver 
COMMENT 'Silver layer - cleansed and conformed dimensional model with SCD2 tracking';

CREATE SCHEMA IF NOT EXISTS gold 
COMMENT 'Gold layer - business-ready analytics and aggregations';

-- ============================================================================
-- STEP 3: Create Volume for Raw Data Storage
-- ============================================================================
CREATE VOLUME IF NOT EXISTS landing.raw_data
COMMENT 'Storage for raw CSV files from external sources';

-- ============================================================================
-- STEP 4: Create Metadata Tracking Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS landing.metadata_tracking (
    table_name STRING COMMENT 'Target table name',
    source_file STRING COMMENT 'Source file or table',
    record_count BIGINT COMMENT 'Number of records processed',
    load_timestamp TIMESTAMP COMMENT 'Timestamp of load operation',
    layer STRING COMMENT 'Data layer: landing, bronze, silver, gold',
    status STRING COMMENT 'Load status: success, failed, warning',
    notes STRING COMMENT 'Additional notes or error messages'
) 
COMMENT 'Metadata tracking for data pipeline operations';

-- ============================================================================
-- STEP 5: Create Bronze Tables (Schema definition)
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.drug_reviews_raw (
    condition STRING,
    drug STRING,
    indication STRING,
    type STRING,
    reviews STRING,
    effective STRING,
    ease_of_use STRING,
    satisfaction STRING,
    information STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_id STRING
)
COMMENT 'Raw drug reviews data from Drug.csv';

CREATE TABLE IF NOT EXISTS bronze.drug_reviews_clean (
    condition STRING,
    drug STRING,
    ease_of_use DOUBLE,
    effective DOUBLE,
    form STRING,
    indication STRING,
    price DOUBLE,
    reviews DOUBLE,
    satisfaction DOUBLE,
    type STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    record_id STRING
)
COMMENT 'Cleaned drug reviews data from Drug_clean.csv';

-- ============================================================================
-- STEP 6: Create Silver Tables (with SCD2 support)
-- ============================================================================

-- Dimension: Drugs (with SCD2 tracking)
CREATE TABLE IF NOT EXISTS silver.dim_drugs (
    drug_key STRING COMMENT 'Surrogate key (MD5 hash of drug_name)',
    drug_name STRING COMMENT 'Drug name',
    drug_type STRING COMMENT 'Drug type: RX, OTC, RX/OTC',
    drug_form STRING COMMENT 'Drug form: Tablet, Capsule, Liquid, etc.',
    average_price DOUBLE COMMENT 'Average price across all records',
    drug_information STRING COMMENT 'Additional drug information',
    effective_date DATE COMMENT 'SCD2: Date when this version became effective',
    end_date DATE COMMENT 'SCD2: Date when this version expired (NULL for current)',
    is_current BOOLEAN COMMENT 'SCD2: True if this is the current version'
)
COMMENT 'Dimension table for drugs with SCD Type 2 tracking for price changes';

-- Dimension: Conditions
CREATE TABLE IF NOT EXISTS silver.dim_conditions (
    condition_key STRING COMMENT 'Surrogate key (MD5 hash of condition_name)',
    condition_name STRING COMMENT 'Medical condition name',
    condition_category STRING COMMENT 'Condition category: Pain Management, Cardiovascular, etc.',
    indication_type STRING COMMENT 'Type of indication',
    total_drugs_available BIGINT COMMENT 'Total drugs available for this condition',
    total_reviews_count BIGINT COMMENT 'Total reviews count',
    first_seen_date TIMESTAMP COMMENT 'First time this condition appeared',
    last_updated_date TIMESTAMP COMMENT 'Last update timestamp'
)
COMMENT 'Dimension table for medical conditions';

-- Fact: Drug Performance
CREATE TABLE IF NOT EXISTS silver.fact_drug_performance (
    drug_key STRING COMMENT 'Foreign key to dim_drugs',
    condition_key STRING COMMENT 'Foreign key to dim_conditions',
    effectiveness_score DOUBLE COMMENT 'Effectiveness rating (1-5)',
    ease_of_use_score DOUBLE COMMENT 'Ease of use rating (1-5)',
    satisfaction_score DOUBLE COMMENT 'Satisfaction rating (1-5)',
    review_count INT COMMENT 'Number of reviews',
    drug_form STRING COMMENT 'Drug form for this performance record',
    drug_type STRING COMMENT 'Drug type for this performance record',
    price DOUBLE COMMENT 'Price for this drug-condition combination',
    data_source STRING COMMENT 'Source: raw or clean dataset',
    load_timestamp TIMESTAMP COMMENT 'Load timestamp',
    performance_key STRING COMMENT 'Unique key for this performance record'
)
COMMENT 'Fact table with drug performance metrics';

-- ============================================================================
-- STEP 7: Create Gold Tables (Analytics)
-- ============================================================================

-- Analytics 1: Drug Effectiveness Rankings
CREATE TABLE IF NOT EXISTS gold.drug_effectiveness_ranking (
    condition_name STRING,
    drug_name STRING,
    drug_type STRING,
    drug_form STRING,
    effectiveness_score DOUBLE,
    ease_of_use_score DOUBLE,
    satisfaction_score DOUBLE,
    overall_score DOUBLE COMMENT 'Weighted score (40% effectiveness, 40% satisfaction, 20% ease)',
    effectiveness_rank INT COMMENT 'Rank by effectiveness within condition',
    satisfaction_rank INT COMMENT 'Rank by satisfaction within condition',
    overall_rank INT COMMENT 'Rank by overall score within condition',
    total_reviews BIGINT,
    average_price DOUBLE,
    last_updated TIMESTAMP
)
COMMENT 'Drug rankings by condition with effectiveness, satisfaction, and overall scores';

-- Analytics 2: Price-Performance Analysis
CREATE TABLE IF NOT EXISTS gold.price_performance_analysis (
    drug_name STRING,
    drug_type STRING,
    price_tier STRING COMMENT 'Low (<$50), Medium ($50-$150), High (>$150)',
    average_price DOUBLE,
    avg_effectiveness DOUBLE,
    avg_ease_of_use DOUBLE,
    avg_satisfaction DOUBLE,
    value_score DOUBLE COMMENT 'Effectiveness per dollar * 100',
    price_percentile INT COMMENT 'Price percentile (0-100)',
    performance_percentile INT COMMENT 'Performance percentile (0-100)',
    total_reviews BIGINT,
    conditions_treated BIGINT COMMENT 'Number of conditions this drug treats',
    last_updated TIMESTAMP
)
COMMENT 'Price vs performance analysis with value scores and percentiles';

-- Analytics 3: Condition Treatment Summary
CREATE TABLE IF NOT EXISTS gold.condition_treatment_summary (
    condition_name STRING,
    condition_category STRING,
    total_drugs_available BIGINT,
    rx_drugs_count BIGINT COMMENT 'Count of prescription drugs',
    otc_drugs_count BIGINT COMMENT 'Count of OTC drugs',
    tablet_options BIGINT COMMENT 'Count of tablet/capsule options',
    liquid_options BIGINT,
    topical_options BIGINT COMMENT 'Creams, gels, ointments',
    injectable_options BIGINT,
    avg_effectiveness DOUBLE,
    avg_satisfaction DOUBLE,
    best_drug_by_effectiveness STRING COMMENT 'Top-ranked drug by effectiveness',
    min_price DOUBLE,
    max_price DOUBLE,
    avg_price DOUBLE,
    total_patient_reviews BIGINT,
    last_updated TIMESTAMP
)
COMMENT 'Condition-level treatment options and benchmarks';

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check catalog structure
SHOW SCHEMAS IN drug_evaluation;

-- Check volumes
SHOW VOLUMES IN drug_evaluation.landing;

-- Check tables per schema
SHOW TABLES IN drug_evaluation.landing;
SHOW TABLES IN drug_evaluation.bronze;
SHOW TABLES IN drug_evaluation.silver;
SHOW TABLES IN drug_evaluation.gold;

SELECT '✅ Unity Catalog setup complete!' as status;
