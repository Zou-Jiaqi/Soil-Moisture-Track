from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)

ProjectId = os.getenv("PROJECT_ID")
Region = os.getenv("REGION")

# Job names for each component
CYGNSS_INGEST_JOB = os.getenv("CYGNSS_INGEST_JOB_NAME", "cygnss-ingest-job")
SMAP_INGEST_JOB = os.getenv("SMAP_INGEST_JOB_NAME", "smap-ingest-job")
CYGNSS_PREPROCESS_JOB = os.getenv("CYGNSS_PREPROCESS_JOB_NAME", "cygnss-preprocess-job")
SMAP_PREPROCESS_JOB = os.getenv("SMAP_PREPROCESS_JOB_NAME", "smap-preprocess-job")

if not ProjectId:
    raise ValueError("PROJECT_ID environment variable is not set")
if not Region:
    raise ValueError("REGION environment variable is not set")

logger.info(f"DAG configuration: ProjectId={ProjectId}, Region={Region}")
logger.info(f"Jobs: CYGNSS_INGEST={CYGNSS_INGEST_JOB}, SMAP_INGEST={SMAP_INGEST_JOB}")
logger.info(f"Jobs: CYGNSS_PREPROCESS={CYGNSS_PREPROCESS_JOB}, SMAP_PREPROCESS={SMAP_PREPROCESS_JOB}")

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
    "start_date": datetime(2026, 1, 16),
}


# schedule interval: executed at 00:00 every day
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

    # Ingest tasks - run in parallel
    cygnss_ingest_task = CloudRunExecuteJobOperator(
        task_id="cygnss_ingest",
        project_id=ProjectId,
        region=Region,
        job_name=CYGNSS_INGEST_JOB,
        gcp_conn_id="google_cloud_default",
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

    smap_ingest_task = CloudRunExecuteJobOperator(
        task_id="smap_ingest",
        project_id=ProjectId,
        region=Region,
        job_name=SMAP_INGEST_JOB,
        gcp_conn_id="google_cloud_default",
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

    # Preprocess tasks - run after ingest completes
    cygnss_preprocess_task = CloudRunExecuteJobOperator(
        task_id="cygnss_preprocess",
        project_id=ProjectId,
        region=Region,
        job_name=CYGNSS_PREPROCESS_JOB,
        gcp_conn_id="google_cloud_default",
        overrides={
            "container_overrides": [
                {
                    "env": [
                        {
                            "name": "PROCESS_DATE",
                            "value": "{{ ti.xcom_pull(task_ids='prepare_target_date') }}",
                        }
                    ]
                }
            ]
        }
    )

    smap_preprocess_task = CloudRunExecuteJobOperator(
        task_id="smap_preprocess",
        project_id=ProjectId,
        region=Region,
        job_name=SMAP_PREPROCESS_JOB,
        gcp_conn_id="google_cloud_default",
        overrides={
            "container_overrides": [
                {
                    "env": [
                        {
                            "name": "PROCESS_DATE",
                            "value": "{{ ti.xcom_pull(task_ids='prepare_target_date') }}",
                        }
                    ]
                }
            ]
        }
    )

    # Task dependencies
    # Ingest tasks run in parallel after date preparation
    prepare_date_task >> [cygnss_ingest_task, smap_ingest_task]
    
    # Preprocess tasks run after their respective ingest tasks complete
    cygnss_ingest_task >> cygnss_preprocess_task
    smap_ingest_task >> smap_preprocess_task

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



