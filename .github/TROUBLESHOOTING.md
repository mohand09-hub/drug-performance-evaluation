# GitHub Actions Troubleshooting Guide

## Quick Diagnostics

### Step 1: Verify Secrets Configuration

In your GitHub repository, go to **Settings → Secrets and variables → Actions**

Required secrets:
```
DATABRICKS_HOST     = https://dbc-97d2bf38-c66c.cloud.databricks.com
DATABRICKS_TOKEN    = dapi... (from Databricks User Settings → Access Tokens)
```

**Common mistakes:**
- ❌ Missing `https://` in DATABRICKS_HOST
- ❌ Trailing slash in DATABRICKS_HOST (should NOT end with `/`)
- ❌ Expired or invalid DATABRICKS_TOKEN
- ❌ Token without sufficient permissions

### Step 2: Check Workflow Trigger

The workflow triggers on:
- Push to `main` branch → Deploys to **Production**
- Push to `develop` branch → Deploys to **Development**
- Pull request to `main` or `develop` → **Validates only** (no deployment)
- Manual trigger → Choose environment

### Step 3: Common Error Messages

#### Error: "master: not found"
**Cause:** Workflow was configured for `master` branch  
**Fix:** ✅ Already fixed in latest commit (changed to `main`)

#### Error: "DATABRICKS_HOST is not set"
**Cause:** Missing secret in GitHub  
**Fix:** Add secret in GitHub Settings → Secrets → Actions

#### Error: "Authentication failed"
**Cause:** Invalid or expired DATABRICKS_TOKEN  
**Fix:** Generate new token:
1. Databricks workspace → User icon → Settings
2. Developer → Access tokens → Generate new token
3. Copy token (shown only once!)
4. Update GitHub secret

#### Error: "Bundle validation failed"
**Cause:** Invalid databricks.yml or missing files  
**Fix:** Run locally first:
```bash
databricks bundle validate
```
Current status: PASSES ✓

#### Error: "Cannot find ../scripts/bronze_load.py"
**Cause:** Incorrect file paths in resources/pipeline_job.yml  
**Fix:** Paths should be relative to project root, not GitHub Actions runner directory

#### Error: "Error: context canceled"
**Cause:** GitHub Actions runner timeout or network issue  
**Fix:** Retry the workflow manually (GitHub Actions → Re-run failed jobs)

## Viewing Workflow Logs

1. Go to: **GitHub repository → Actions tab**
2. Click on latest workflow run
3. Click on failed job (red ✗)
4. Expand failed step to see full error
5. Look for lines starting with "Error:" or "Failed:"

## Testing Locally Before Push

```bash
# Validate bundle
databricks bundle validate

# Deploy to dev (test before pushing)
databricks bundle deploy --target dev

# If successful, push to GitHub
git push origin main
```

## Manual Workflow Trigger

If automated push doesn't work:
1. GitHub → Actions → Deploy Drug Evaluation Pipeline
2. Click "Run workflow"
3. Select branch: main or develop
4. Select environment: dev or prod
5. Click "Run workflow"

## Getting More Help

If the above doesn't resolve your issue:
1. Copy the exact error message from GitHub Actions logs
2. Note which step failed (Validate / Deploy to Dev / Deploy to Prod)
3. Share both with me for specific troubleshooting

---

## Quick Reference: Workflow Jobs

| Job | Trigger | Purpose |
|-----|---------|---------|
| **validate** | All pushes/PRs | Validates DAB bundle syntax |
| **deploy-dev** | Push to `develop` | Deploys to development environment |
| **deploy-prod** | Push to `main` | Deploys to production environment |
| **notify** | After deploy jobs | Reports deployment status |

## Success Indicators

✅ All jobs should show green checkmarks  
✅ "Validation OK!" message in validate job  
✅ "Deployment complete!" message in deploy job  
✅ Job visible in Databricks Workflows → Jobs

---

**Last Updated:** 2026-09-13  
**Status:** Workflow fixed (main branch support added)
