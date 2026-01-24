# Deployment Guide

This document describes how the components are deployed to Google Cloud Run.

## Deployment Architecture

- **Ingest Component**: Deployed as Cloud Run Job (`smt-ingest-job`)
- **Preprocess Component**: Deployed as Cloud Run Job (`smt-preprocess-job`)
- **Airflow Component**: Deployed as Cloud Run Service (`smt-airflow-service`)

## Automatic Deployment

When you push changes to the `main` branch, GitHub Actions automatically:

1. **Builds the Docker image** and pushes it to Google Artifact Registry
2. **Deploys to Cloud Run** (Job or Service depending on component)

### Workflow Triggers

- `ingest/**` changes → Builds and deploys ingest job
- `preprocess/**` changes → Builds and deploys preprocess job
- `airflow/**` changes → Builds and deploys airflow service

## Cloud Run Jobs (Ingest & Preprocess)

### Ingest Job
- **Name**: `smt-ingest-job`
- **Region**: `us-west2`
- **Image**: `us-west2-docker.pkg.dev/secure-bonbon-463900-u2/soil-moisture-track/smt_ingest:latest`
- **Resources**: 2Gi memory, 2 CPU
- **Timeout**: 3600 seconds (1 hour)
- **Max Retries**: 3

### Preprocess Job
- **Name**: `smt-preprocess-job`
- **Region**: `us-west2`
- **Image**: `us-west2-docker.pkg.dev/secure-bonbon-463900-u2/soil-moisture-track/smt_preprocess:latest`
- **Resources**: 2Gi memory, 2 CPU
- **Timeout**: 3600 seconds (1 hour)
- **Max Retries**: 3

## Cloud Run Service (Airflow)

### Airflow Service
- **Name**: `smt-airflow-service`
- **Region**: `us-west2`
- **Image**: `us-west2-docker.pkg.dev/secure-bonbon-463900-u2/soil-moisture-track/smt_airflow:latest`
- **Resources**: 4Gi memory, 2 CPU
- **Instances**: Min 1, Max 1 (always running)
- **Port**: 8080
- **Access**: Unauthenticated (consider adding authentication for production)

## Environment Variables

### For Airflow DAG

The Airflow DAG needs these environment variables to execute Cloud Run jobs:

```bash
DOWNLOAD_JOB_NAME=smt-ingest-job
PROJECT_ID=secure-bonbon-463900-u2
REGION=us-west2
```

### For Airflow Service

Set these when deploying (or via Cloud Run console):

```bash
# GCP Configuration
PROJECT_ID=secure-bonbon-463900-u2
REGION=us-west2
DOWNLOAD_JOB_NAME=smt-ingest-job

# Airflow Credentials (optional)
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=your-secure-password
AIRFLOW_EMAIL=admin@example.com

# Other environment variables as needed
GSC_BUCKET_NAME=...
```

## Manual Deployment

If you need to deploy manually:

### Deploy Ingest Job
```bash
gcloud run jobs create smt-ingest-job \
  --image=us-west2-docker.pkg.dev/secure-bonbon-463900-u2/soil-moisture-track/smt_ingest:latest \
  --region=us-west2 \
  --project=secure-bonbon-463900-u2 \
  --max-retries=3 \
  --task-timeout=3600 \
  --memory=2Gi \
  --cpu=2
```

### Deploy Preprocess Job
```bash
gcloud run jobs create smt-preprocess-job \
  --image=us-west2-docker.pkg.dev/secure-bonbon-463900-u2/soil-moisture-track/smt_preprocess:latest \
  --region=us-west2 \
  --project=secure-bonbon-463900-u2 \
  --max-retries=3 \
  --task-timeout=3600 \
  --memory=2Gi \
  --cpu=2
```

### Deploy Airflow Service
```bash
gcloud run services create smt-airflow-service \
  --image=us-west2-docker.pkg.dev/secure-bonbon-463900-u2/soil-moisture-track/smt_airflow:latest \
  --region=us-west2 \
  --project=secure-bonbon-463900-u2 \
  --memory=4Gi \
  --cpu=2 \
  --min-instances=1 \
  --max-instances=1 \
  --port=8080 \
  --allow-unauthenticated \
  --set-env-vars="PROJECT_ID=secure-bonbon-463900-u2,REGION=us-west2,DOWNLOAD_JOB_NAME=smt-ingest-job"
```

## Updating Deployments

The workflows automatically update existing deployments. To manually update:

```bash
# Update Job
gcloud run jobs update smt-ingest-job \
  --image=us-west2-docker.pkg.dev/secure-bonbon-463900-u2/soil-moisture-track/smt_ingest:latest \
  --region=us-west2

# Update Service
gcloud run services update smt-airflow-service \
  --image=us-west2-docker.pkg.dev/secure-bonbon-463900-u2/soil-moisture-track/smt_airflow:latest \
  --region=us-west2
```

## Accessing Airflow UI

After deployment, get the service URL:

```bash
gcloud run services describe smt-airflow-service \
  --region=us-west2 \
  --format="value(status.url)"
```

Then access the Airflow UI at that URL (default port 8080).

## Notes

- **Jobs** are for one-time executions (triggered by Airflow)
- **Services** are for long-running applications (Airflow webserver/scheduler)
- Resource limits can be adjusted in the workflows if needed
- Consider adding authentication to the Airflow service for production use
