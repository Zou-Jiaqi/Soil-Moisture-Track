from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta
import os

DownloadJobName = os.getenv("DOWNLOAD_JOB_NAME")
ProjectId = os.getenv("PROJECT_ID")
Region = os.getenv("REGION")

default_args = {
    "owner": "airflow",
    "dag_id": "soil_moisture_track",
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
    "start_date": datetime(2026, 1, 16),
}


# schedule interval: executed every 5 minutes
with DAG("data_pipeline", default_args=default_args, schedule_interval=timedelta(minutes=5), catchup=True) as dag:

    # Calculate target date (execution_date - 3 days) and return as string
    def get_target_date(**context):
        target_date = context["execution_date"] - timedelta(days=3)
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
        overrides={
            "environment": {
                "variables": {
                    "DOWNLOAD_DATE": "{{ ti.xcom_pull(task_ids='prepare_target_date') }}",
                }
            }
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



