# Azure Deployment Runbook (lakehouse-analytics-platform)

## Objective
Run the pipeline on Azure and operationalize data delivery to API, BI, and copilot layers.

## Reference Architecture
- Storage: ADLS Gen2 containers (`raw`, `silver`, `marts`, `exports`)
- Compute: Azure Container Apps job or Azure Databricks job cluster
- Orchestration: Azure Data Factory pipeline (`orchestration/adf/pipeline.json`)
- Secrets: Azure Key Vault
- Monitoring: Azure Monitor + Log Analytics workspaces

## Runtime Setup
1. Build image and publish to Azure Container Registry.
2. Create ADLS Gen2 containers and assign managed identity permissions.
3. Configure pipeline execution target:
- Option A: Container Apps job running `python -m pipeline.run_all`
- Option B: Databricks job invoking `spark_jobs/lakehouse_parity_job.py`
4. Import ADF template and map linked services:
- `ls_az_databricks`
- `ls_snowflake_warehouse`
5. Configure alerts for failed activities and stale exports.

## Validation Checklist
1. Trigger ADF pipeline manually for one test date.
2. Validate all mart exports are present in `exports`.
3. Confirm quality report JSON has all checks passing.
4. Verify dashboard/API consumers can read fresh outputs.

## Rollback
1. Disable pipeline trigger.
2. Revert to previous image tag or Databricks job version.
3. Restore previous `exports` snapshot from ADLS soft delete/versioning.
