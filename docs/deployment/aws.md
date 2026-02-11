# AWS Deployment Runbook (lakehouse-analytics-platform)

## Objective
Run the pipeline on AWS with reproducible daily exports to downstream API/dashboard services.

## Reference Architecture
- Storage: S3 (`raw`, `curated`, `exports`)
- Compute: ECS Fargate task (or AWS Batch job) running `python -m pipeline.run_all`
- Orchestration: EventBridge schedule + Step Functions state machine
- Secrets: AWS Secrets Manager (warehouse credentials, if external warehouse is used)
- Monitoring: CloudWatch Logs + CloudWatch alarms

## Runtime Setup
1. Build and push container image to ECR.
2. Provision S3 buckets and IAM role with least-privilege access.
3. Configure ECS task definition:
- Command: `python -m pipeline.run_all`
- Environment: `PIPELINE_ENV=prod`
- Mount or sync data path to S3 before/after run.
4. Create EventBridge schedule (daily/hourly based on SLA).
5. Attach CloudWatch alarms for:
- Task failures
- Export freshness (missing `daily_kpis.csv` for run date)

## Validation Checklist
1. Run one manual ECS task.
2. Confirm `daily_kpis.csv`, `channel_performance.csv`, `customer_health.csv`, and `experiment_performance.csv` are generated.
3. Verify quality report exists and has no failed checks.
4. Confirm downstream API (`kpi-alert-api`) reads latest exports.

## Rollback
1. Re-run previous stable task definition revision.
2. Restore previous exports snapshot from S3 versioning.
3. Re-run API smoke tests against restored data.
