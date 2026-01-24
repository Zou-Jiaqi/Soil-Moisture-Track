from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)

DownloadJobName = os.getenv("DOWNLOAD_JOB_NAME")
ProjectId = os.getenv("PROJECT_ID")
Region = os.getenv("REGION")

if not DownloadJobName:
    raise ValueError("DOWNLOAD_JOB_NAME environment variable is not set")
if not ProjectId:
    raise ValueError("PROJECT_ID environment variable is not set")
if not Region:
    raise ValueError("REGION environment variable is not set")

logger.info(f"DAG configuration: JobName={DownloadJobName}, ProjectId={ProjectId}, Region={Region}")

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
    "start_date": datetime(2026, 1, 16),
}


# schedule interval: executed at 00:00 every day
# Using schedule parameter (Airflow 2.11.0+) instead of schedule_interval for better compatibility
with DAG(
    "data_pipeline",
    default_args=default_args,
    schedule="0 0 * * *",  # Cron expression: 00:00 UTC every day
    catchup=True,  # Enable catchup to backfill from start_date to current date
    description="Download satellite data daily at 00:00 UTC",
    tags=["satellite", "data-ingestion"],
) as dag:


    # Calculate target date (logical_date - 3 days) and return as string
    def get_target_date(**context):
        # Use logical_date (Airflow 2.11.0+) with fallback to execution_date for compatibility
        logical_date = context.get("logical_date") or context.get("execution_date")
        if logical_date is None:
            raise ValueError("Neither logical_date nor execution_date found in context")
        target_date = logical_date - timedelta(days=3)
        logger.info(f"Calculated target date: {target_date.strftime('%Y-%m-%d')}")
        return target_date.strftime("%Y-%m-%d")

    prepare_date_task = PythonOperator(
        task_id="prepare_target_date",
        python_callable=get_target_date,
    )

    satellite_data_download_task = CloudRunExecuteJobOperator(
        task_id="download_satellite_data",
        project_id=ProjectId,
        region=Region,
        job_name=DownloadJobName,
        gcp_conn_id="google_cloud_default",  # Explicitly use the default GCP connection
        overrides={
            "container_overrides": [
                {
                    "env": [
                        {
                            "name": "DOWNLOAD_DATE",
                            "value": "{{ ti.xcom_pull(task_ids='prepare_target_date') }}",
                        }
                    ]
                }
            ]
        }
    )

    prepare_date_task >> satellite_data_download_task

    # PYSPARK_JOB = {
    #     "reference": {"project_id": "your-project-id"},
    #     "placement": {"cluster_name": "your-cluster-name"},
    #     "pyspark_job": {"main_python_file_uri": "gs://your-bucket/path/to/script.py"},
    # }

    # submit_job = DataprocSubmitJobOperator(
    #     task_id="run_spark_job",
    #     job=PYSPARK_JOB,
    #     region="us-west1",
    #     project_id="your-project-id"
    # )



