# Drug Performance Evaluation Pipeline

A production-ready data pipeline built on Databricks with Unity Catalog, implementing **Medallion Architecture** (Bronze → Silver → Gold) with **SCD Type 2** historical tracking for dimensional changes.

## 🏗️ Architecture

```
┌─────────────┐
│   Landing   │  Raw CSV files in UC Volume
└──────┬──────┘
       │
       ↓
┌─────────────┐
│   Bronze    │  Raw ingestion with metadata
└──────┬──────┘
       │
       ↓
┌─────────────┐
│   Silver    │  Cleansed dimensional model + SCD2 tracking
└──────┬──────┘
       │
       ↓
┌─────────────┐
│    Gold     │  Business-ready analytics tables
└─────────────┘
```

## 📊 Data Model

### Silver Layer (Dimensional Model with SCD2)
- **`dim_drugs`** - Drug dimension with SCD Type 2 tracking (tracks price changes over time)
  - SCD2 columns: `effective_date`, `end_date`, `is_current`
  - Business key: `drug_name`
  - Tracked columns: `average_price`, `drug_type`, `drug_form`
- **`dim_conditions`** - Medical condition dimension
- **`fact_drug_performance`** - Performance metrics (effectiveness, satisfaction, reviews)

### Gold Layer (Analytics)
- **`drug_effectiveness_ranking`** - Drug rankings by condition with scores
- **`price_performance_analysis`** - Price vs performance with value scores
- **`condition_treatment_summary`** - Treatment options per condition

## 🚀 Quick Start

### Prerequisites
1. Databricks workspace with Unity Catalog enabled
2. Databricks CLI installed and configured
3. Git credentials configured in Databricks

### 1. Setup Unity Catalog

Run the prerequisite SQL script to create catalog, schemas, and tables:

```sql
-- In Databricks SQL Editor or Notebook
-- Run: setup/prerequisites.sql
```

### 2. Upload Data Files

Upload your CSV files to the Unity Catalog volume:

```bash
# Option 1: Via Databricks UI
# Navigate to: Catalog → drug_evaluation → landing → raw_data
# Upload Drug.csv and Drug_clean.csv

# Option 2: Via CLI
databricks fs cp Drug.csv dbfs:/Volumes/drug_evaluation/landing/raw_data/Drug.csv
databricks fs cp Drug_clean.csv dbfs:/Volumes/drug_evaluation/landing/raw_data/Drug_clean.csv
```

### 3. Deploy with Databricks Asset Bundles (DABs)

```bash
# Install/upgrade Databricks CLI
pip install --upgrade databricks-cli

# Authenticate
databricks auth login --host https://your-workspace.cloud.databricks.com

# Validate bundle
databricks bundle validate

# Deploy to DEV environment (default)
databricks bundle deploy

# Deploy to PROD environment
databricks bundle deploy --target prod

# Run the pipeline
databricks bundle run drug_evaluation_pipeline
```

## 📁 Project Structure

```
drug-performance-evaluation/
├── databricks.yml              # Main DABs config (dev/prod targets)
├── config/
│   └── project_config.json     # Pipeline configuration
├── scripts/
│   ├── bronze_load.py          # Bronze layer ingestion
│   ├── silver_transform.py     # Silver transformation with SCD2
│   └── gold_analytics.py       # Gold analytics generation
├── resources/
│   └── pipeline_job.yml        # Job definition with dependencies
├── setup/
│   └── prerequisites.sql       # Unity Catalog setup script
└── README.md                   # This file
```

## ⚙️ Configuration

All pipeline behavior is controlled via `config/project_config.json`:

```json
{
  "unity_catalog": {
    "catalog": "drug_evaluation",
    "schemas": { ... },
    "volume": { ... }
  },
  "scd2": {
    "enabled": true,
    "tracked_tables": ["dim_drugs"],
    "business_keys": {
      "dim_drugs": ["drug_name"]
    },
    "tracked_columns": {
      "dim_drugs": ["average_price", "drug_type", "drug_form"]
    }
  },
  "data_files": {
    "raw_reviews": "Drug.csv",
    "clean_reviews": "Drug_clean.csv"
  }
}
```

## 🔄 SCD Type 2 Tracking

The pipeline automatically tracks historical changes in drug dimensions:

### How It Works
1. **Initial Load**: All records marked as `is_current = true`, `end_date = NULL`
2. **Subsequent Loads**:
   - **New drugs** → Inserted with `is_current = true`
   - **Changed drugs** → Old record expired (`end_date = today`, `is_current = false`), new version inserted
   - **Unchanged drugs** → Kept as-is

### Example Query (Price History)
```sql
SELECT 
  drug_name,
  average_price,
  effective_date,
  end_date,
  is_current
FROM drug_evaluation.silver.dim_drugs
WHERE drug_name = 'Amoxicillin'
ORDER BY effective_date DESC;
```

## 📈 Analytics Use Cases

### 1. Find Best Drug for a Condition
```sql
SELECT 
  drug_name,
  overall_score,
  overall_rank,
  total_reviews,
  average_price
FROM drug_evaluation.gold.drug_effectiveness_ranking
WHERE condition_name = 'Hypertension' 
  AND overall_rank <= 5
ORDER BY overall_rank;
```

### 2. Value Analysis (Best Bang for Buck)
```sql
SELECT 
  drug_name,
  average_price,
  avg_effectiveness,
  value_score,
  price_tier
FROM drug_evaluation.gold.price_performance_analysis
WHERE price_tier = 'Low (<$50)'
ORDER BY value_score DESC
LIMIT 10;
```

### 3. Condition Treatment Options
```sql
SELECT 
  condition_name,
  total_drugs_available,
  rx_drugs_count,
  otc_drugs_count,
  best_drug_by_effectiveness,
  avg_price
FROM drug_evaluation.gold.condition_treatment_summary
ORDER BY total_patient_reviews DESC;
```



## 🤖 Automated CI/CD with GitHub Actions

The project includes automated deployment via GitHub Actions. Every push triggers validation and deployment.

### Quick Setup

1. **Add GitHub Secrets** (Repository → Settings → Secrets):
   ```
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=dapi...
   ```

2. **Branch Strategy**:
   - `develop` → Auto-deploys to **Dev** environment
   - `main` → Auto-deploys to **Production** environment

3. **Workflow Stages**:
   - ✅ Validate DABs bundle
   - 🚀 Deploy to environment
   - 🧪 Run smoke tests (optional)
   - 📧 Send notifications

### Usage

```bash
# Deploy to Dev
git checkout develop
git commit -am "Your changes"
git push origin develop

# Deploy to Prod
git checkout main
git merge develop
git push origin main
```

### Manual Deployment

1. Go to: **Actions** → **Deploy Drug Evaluation Pipeline**
2. Click **Run workflow**
3. Select environment (dev/prod)
4. Click **Run workflow**

📖 **Full Documentation**: See [.github/DEPLOYMENT.md](.github/DEPLOYMENT.md) for complete setup guide.

## 🔧 Development

### Run Individual Stages Locally

```python
# Bronze layer
%run ./scripts/bronze_load.py ./config/project_config.json

# Silver layer (with SCD2)
%run ./scripts/silver_transform.py ./config/project_config.json

# Gold layer
%run ./scripts/gold_analytics.py ./config/project_config.json
```

### Disable SCD2 Tracking

Edit `config/project_config.json`:
```json
{
  "scd2": {
    "enabled": false
  }
}
```

## 🤖 CI/CD with GitHub Actions

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy DAB
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to Databricks
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          pip install databricks-cli
          databricks bundle deploy --target prod
```

## 📊 Monitoring

Check pipeline execution status:

```sql
-- View metadata tracking
SELECT * 
FROM drug_evaluation.landing.metadata_tracking
ORDER BY load_timestamp DESC;

-- Check SCD2 history
SELECT 
  COUNT(*) as total_records,
  SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records,
  COUNT(DISTINCT drug_name) as unique_drugs
FROM drug_evaluation.silver.dim_drugs;
```

## 🎯 Next Steps

1. **Agentic Use Case**: Add Genie Space for natural language queries
2. **ML Recommendations**: Build drug recommendation model
3. **Dashboard**: Create AI/BI dashboard for stakeholders
4. **Alerts**: Set up data quality monitoring and alerts

## 📝 License

This project is for demonstration purposes.

## 👤 Author

**Your Name**
- Email: mrdmohu@gmail.com
- GitHub: [@mohand09-hub](https://github.com/mohand09-hub)

<!-- Last verified: Sun Sep 13 02:34:12 PM UTC 2026 -->
