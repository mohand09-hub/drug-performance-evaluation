# GitHub Actions CI/CD Setup Guide

This guide explains how to configure GitHub Actions for automated deployment of the Drug Performance Evaluation pipeline to Databricks.

## 🔐 Prerequisites

1. **Databricks Personal Access Token**
   - Generate from: Databricks Workspace → User Settings → Access Tokens
   - Permissions: Full workspace access (for DABs deployment)
   
2. **GitHub Repository Secrets**
   - Repository → Settings → Secrets and variables → Actions

## 📋 Required GitHub Secrets

Add these secrets to your GitHub repository:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `DATABRICKS_HOST` | Databricks workspace URL | `https://dbc-97d2bf38-c66c.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal Access Token | `dapi1234567890abcdef...` |

### How to Add Secrets:

```bash
# Via GitHub CLI
gh secret set DATABRICKS_HOST --body "https://your-workspace.cloud.databricks.com"
gh secret set DATABRICKS_TOKEN --body "dapi..."

# Or via GitHub UI:
# 1. Go to: Repository → Settings → Secrets and variables → Actions
# 2. Click "New repository secret"
# 3. Add name and value
# 4. Click "Add secret"
```

## 🚀 Workflow Triggers

The CI/CD pipeline triggers on:

### 1. **Automatic Deployment on Push**

| Branch | Environment | Auto-Deploy |
|--------|-------------|-------------|
| `develop` | Development | ✅ Yes |
| `main` | Production | ✅ Yes |

```bash
# Deploy to Dev
git checkout develop
git push origin develop

# Deploy to Prod
git checkout main
git merge develop
git push origin main
```

### 2. **Pull Request Validation**

- Validates DABs bundle on PRs to `main` or `develop`
- No deployment occurs (validation only)

### 3. **Manual Deployment**

Trigger from GitHub UI:
1. Go to: Actions → Deploy Drug Evaluation Pipeline
2. Click "Run workflow"
3. Select branch and environment (dev/prod)
4. Click "Run workflow"

## 📊 Workflow Stages

### Stage 1: Validate Bundle
```yaml
- Checkout code
- Install Databricks CLI
- Run: databricks bundle validate
```

### Stage 2: Deploy to Environment
```yaml
- Checkout code
- Install Databricks CLI
- Run: databricks bundle deploy --target {dev|prod}
- Run smoke tests (optional)
```

### Stage 3: Notify Status
```yaml
- Check deployment results
- Send notifications (can integrate Slack, email, etc.)
```

## 🔄 Branching Strategy

Recommended Git workflow:

```
main (production)
  ↑
develop (development)
  ↑
feature/xyz (feature branches)
```

### Development Workflow:

```bash
# 1. Create feature branch
git checkout develop
git pull origin develop
git checkout -b feature/add-new-analytics

# 2. Make changes and commit
git add .
git commit -m "feat: Add new analytics table"

# 3. Push and create PR to develop
git push origin feature/add-new-analytics
# Create PR: feature/add-new-analytics → develop

# 4. After PR merge, auto-deploys to Dev

# 5. When ready for prod, create PR: develop → main
# After merge, auto-deploys to Production
```

## 🎯 GitHub Environments (Optional but Recommended)

Configure protection rules:

### Development Environment
```
Repository → Settings → Environments → New environment: "development"
- No protection rules needed
```

### Production Environment
```
Repository → Settings → Environments → New environment: "production"
- ✅ Required reviewers: Add team members
- ✅ Wait timer: 5 minutes (optional)
- ✅ Deployment branches: main only
```

## 🧪 Testing Strategy

### Pre-Deployment Validation
- Bundle syntax validation
- Schema validation (config files)

### Post-Deployment Smoke Tests (Optional)
Add to `.github/workflows/deploy.yml` after deployment:

```yaml
- name: Run smoke test
  run: |
    # Test bronze layer
    databricks workspace export /bronze_validation.py
    
    # Test silver layer  
    databricks workspace export /silver_validation.py
    
    # Test gold layer
    databricks workspace export /gold_validation.py
```

## 📧 Notifications (Optional Integrations)

### Slack Notifications

Add to workflow:

```yaml
- name: Slack Notification
  uses: 8398a7/action-slack@v3
  with:
    status: ${{ job.status }}
    text: 'Deployment to ${{ inputs.environment }} ${{ job.status }}'
    webhook_url: ${{ secrets.SLACK_WEBHOOK }}
  if: always()
```

### Email Notifications

GitHub sends automatic emails for:
- Workflow failures
- Manual workflow approvals (if configured)

## 🐛 Troubleshooting

### Issue: "Invalid DATABRICKS_HOST"
**Solution**: Ensure secret includes `https://` prefix

### Issue: "Authentication failed"
**Solution**: 
1. Verify token hasn't expired
2. Check token has workspace access permissions
3. Regenerate token if needed

### Issue: "Bundle validation failed"
**Solution**:
1. Test locally: `databricks bundle validate`
2. Check `databricks.yml` syntax
3. Verify all referenced files exist

### Issue: "Deployment succeeded but job not visible"
**Solution**:
1. Check workspace permissions
2. Verify `run_as` configuration in `databricks.yml`
3. Check target environment (dev vs prod)

## 📝 Workflow File Location

```
.github/
└── workflows/
    └── deploy.yml
```

## 🔍 Monitoring Deployments

### View Workflow Runs
```
Repository → Actions → Deploy Drug Evaluation Pipeline
```

### Check Deployment Logs
1. Click on workflow run
2. Click on job (e.g., "Deploy to Development")
3. Expand steps to view logs

### Verify in Databricks
```
Databricks → Workflows → Jobs → Drug Evaluation Pipeline - {ENV}
```

## 🎓 Best Practices

1. **Always test in Dev first** before promoting to Prod
2. **Use PR reviews** for production deployments
3. **Tag releases** for production deployments
4. **Monitor deployment logs** for errors
5. **Keep secrets rotated** (every 90 days recommended)
6. **Document changes** in commit messages
7. **Use feature flags** for gradual rollouts (optional)

## 📚 Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)

---

## ✅ Quick Start Checklist

- [ ] Add `DATABRICKS_HOST` secret to GitHub
- [ ] Add `DATABRICKS_TOKEN` secret to GitHub
- [ ] Create `development` environment in GitHub (optional)
- [ ] Create `production` environment in GitHub with protection rules
- [ ] Push code to `develop` branch
- [ ] Verify Dev deployment in GitHub Actions
- [ ] Create PR: develop → main
- [ ] Verify Prod deployment after merge
- [ ] Verify pipeline runs in Databricks Workflows

**Need help?** Check the troubleshooting section or open an issue in the repository.
