# 🚀 Complete Deployment Checklist

## ✅ Pre-Deployment Verification

### Local Environment
- [ ] All files staged in git: `git status`
- [ ] Bundle validates locally: `databricks bundle validate`
- [ ] Config files have correct values (catalog, schemas, volume paths)
- [ ] Data files uploaded to UC Volume

### Databricks Workspace
- [ ] Unity Catalog created: `drug_evaluation`
- [ ] Schemas created: landing, bronze, silver, gold
- [ ] Volume created: `drug_evaluation.landing.raw_data`
- [ ] Data files present in volume (Drug.csv, Drug_clean.csv)
- [ ] Personal Access Token generated (for GitHub Actions)

## 🔐 GitHub Repository Setup

### 1. Create Repository
```bash
gh repo create drug-performance-evaluation --public --description "Drug Performance Evaluation with Medallion Architecture, SCD2, and CI/CD"
```

### 2. Add Secrets
```bash
# Add via GitHub CLI
gh secret set DATABRICKS_HOST --body "https://dbc-97d2bf38-c66c.cloud.databricks.com"
gh secret set DATABRICKS_TOKEN --body "dapi..."

# Or via UI: Settings → Secrets and variables → Actions → New repository secret
```

### 3. Configure Environments (Optional but Recommended)
```
Settings → Environments
  → New environment: "development" (no restrictions)
  → New environment: "production" 
     ✅ Required reviewers: [Add team members]
     ✅ Deployment branches: main only
```

## 📤 Push to GitHub

### Initial Commit
```bash
cd /Workspace/Users/mrdmohu@gmail.com/drug-performance-evaluation

# Set branch to main
git branch -M main

# Commit all files
git commit -m "feat: Complete DABs pipeline with SCD2 and GitHub Actions CI/CD

- Medallion architecture (Bronze → Silver → Gold)
- SCD Type 2 tracking for dim_drugs
- Config-driven pipeline (project_config.json)
- 3 transformation scripts (bronze, silver, gold)
- DABs deployment with dev/prod targets
- GitHub Actions automated CI/CD
- Complete documentation and setup guides"

# Add remote (replace with your repo)
git remote add origin https://github.com/mohand09-hub/drug-performance-evaluation.git

# Push to main
git push -u origin main
```

### Create Develop Branch
```bash
# Create and push develop branch
git checkout -b develop
git push -u origin develop

# Set develop as default branch for development (optional)
# GitHub → Settings → Branches → Default branch → develop
```

## 🔄 First Deployment

### Option 1: Automatic (Recommended)
```bash
# Push to develop triggers dev deployment
git checkout develop
git push origin develop

# Watch in: GitHub → Actions → Deploy Drug Evaluation Pipeline
# Verify in: Databricks → Workflows → Jobs → Drug Evaluation Pipeline - DEV
```

### Option 2: Manual via GitHub UI
```
1. Go to: GitHub → Actions → Deploy Drug Evaluation Pipeline
2. Click "Run workflow"
3. Select: Branch = develop, Environment = dev
4. Click "Run workflow"
5. Monitor deployment progress
```

### Option 3: Manual via CLI (Alternative)
```bash
# If you prefer CLI deployment without GitHub Actions
databricks bundle deploy --target dev
databricks bundle run drug_evaluation_pipeline --target dev
```

## 🧪 Verify Deployment

### Check GitHub Actions
```
GitHub → Actions → Latest workflow run
  → Check all steps passed (green checkmarks)
  → Review deployment logs
```

### Check Databricks Workspace
```sql
-- Verify catalog structure
SHOW SCHEMAS IN drug_evaluation;

-- Verify bronze tables
SHOW TABLES IN drug_evaluation.bronze;
SELECT COUNT(*) FROM drug_evaluation.bronze.drug_reviews_raw;
SELECT COUNT(*) FROM drug_evaluation.bronze.drug_reviews_clean;

-- Verify silver tables (with SCD2)
SHOW TABLES IN drug_evaluation.silver;
SELECT COUNT(*) FROM drug_evaluation.silver.dim_drugs WHERE is_current = true;
SELECT COUNT(*) FROM drug_evaluation.silver.dim_conditions;
SELECT COUNT(*) FROM drug_evaluation.silver.fact_drug_performance;

-- Verify gold analytics
SHOW TABLES IN drug_evaluation.gold;
SELECT COUNT(*) FROM drug_evaluation.gold.drug_effectiveness_ranking;
SELECT COUNT(*) FROM drug_evaluation.gold.price_performance_analysis;
SELECT COUNT(*) FROM drug_evaluation.gold.condition_treatment_summary;

-- Check metadata tracking
SELECT * FROM drug_evaluation.landing.metadata_tracking
ORDER BY load_timestamp DESC;
```

### Check Workflows Job
```
Databricks → Workflows → Jobs
  → Find: "Drug Evaluation Pipeline - DEV"
  → Check job configuration
  → View past runs (if any)
```

## 🎯 Production Deployment

### When Ready for Production:
```bash
# 1. Merge develop to main
git checkout main
git pull origin main
git merge develop

# 2. Tag the release (optional)
git tag -a v1.0.0 -m "Release v1.0.0: Initial production deployment"

# 3. Push to main (triggers prod deployment)
git push origin main
git push origin v1.0.0
```

### Post-Production Verification:
```
1. GitHub Actions: Verify production deployment succeeded
2. Databricks: Check "Drug Evaluation Pipeline - PROD" job
3. Run smoke tests: Query gold tables in production catalog
4. Monitor for 24 hours
```

## 🐛 Troubleshooting

### Deployment Failed
```bash
# Check workflow logs
GitHub → Actions → Failed run → View logs

# Common issues:
1. Invalid DATABRICKS_HOST secret (missing https://)
2. Expired DATABRICKS_TOKEN (regenerate in workspace)
3. Bundle validation failed (run: databricks bundle validate)
4. Missing permissions (check run_as configuration)
```

### Job Not Visible in Databricks
```
1. Check workspace URL in DATABRICKS_HOST secret
2. Verify deployment target (dev vs prod)
3. Check job permissions in databricks.yml
4. Look in: Workflows → Jobs (not Workflows → Pipelines)
```

### SCD2 Not Working
```sql
-- Check if SCD2 columns exist
DESCRIBE drug_evaluation.silver.dim_drugs;

-- Verify config
cat config/project_config.json | grep -A 10 '"scd2"'

-- Run silver transformation manually
python scripts/silver_transform.py config/project_config.json
```

## 📊 Monitoring & Maintenance

### Weekly Tasks
- [ ] Review metadata_tracking table for pipeline health
- [ ] Check GitHub Actions for failed deployments
- [ ] Monitor data quality in gold tables

### Monthly Tasks
- [ ] Rotate Databricks token (regenerate and update GitHub secret)
- [ ] Review and optimize job cluster configuration
- [ ] Archive old deployment logs

### As Needed
- [ ] Add new analytics tables (gold layer)
- [ ] Modify SCD2 tracked columns (config update)
- [ ] Scale cluster for larger data volumes

## 🎓 Team Onboarding

For new team members:
1. Share: `.github/DEPLOYMENT.md` (setup guide)
2. Grant: Databricks workspace access
3. Add: GitHub collaborator + environment reviewers
4. Demo: Dev deployment workflow
5. Review: Code structure and configuration

## 📚 Documentation Index

| Document | Purpose |
|----------|---------|
| `README.md` | Main project overview and quick start |
| `.github/DEPLOYMENT.md` | Complete CI/CD setup guide |
| `setup/prerequisites.sql` | Unity Catalog setup script |
| `config/project_config.json` | Pipeline configuration reference |
| `databricks.yml` | DABs configuration and targets |

## ✅ Success Criteria

Deployment is successful when:
- ✅ GitHub Actions workflow completes without errors
- ✅ Databricks job visible in Workflows
- ✅ All bronze/silver/gold tables populated
- ✅ Metadata tracking shows "success" status
- ✅ Sample queries return expected results
- ✅ SCD2 tracking working (effective_date, is_current columns present)

---

**🎉 Congratulations! Your pipeline is production-ready with automated CI/CD!**

For questions or issues, refer to:
- GitHub Issues: https://github.com/mohand09-hub/drug-performance-evaluation/issues
- Documentation: README.md and .github/DEPLOYMENT.md
- Databricks Docs: https://docs.databricks.com
