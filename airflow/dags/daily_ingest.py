from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta
from scripts import smap_ingester, cygnss_ingester
import os

JobName = os.getenv("DOWNLOAD_JOB_NAME")
ProjectId = os.getenv("PROJECT_ID")
Region = os.getenv("REGION")

default_args = {
    "owner": "airflow",
    "dag_id": "soil_moisture_track",
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
    "start_date": datetime(2025, 1, 1),
}


# schedule interval: executed at 00:00 every day
with DAG("data_pipeline", default_args=default_args, schedule_interval="0 0 * * *", catchup=True) as dag:

    # target date = current date - 3
    def get_target_date(**context):
        return context["execution_date"] - timedelta(days=3)


    satellite_data_download_task = CloudRunExecuteJobOperator(
        task_id="download_satellite_data",
        project_id=ProjectId,
        region=Region,
        job_name=JobName,
        overrides={
            "environment": {
                "variables": {
                    "DOWNLOAD_DATE": get_target_date().strftime("%Y-%m-%d"),
                }
            }
        }
    )

    PYSPARK_JOB = {
        "reference": {"project_id": "your-project-id"},
        "placement": {"cluster_name": "your-cluster-name"},
        "pyspark_job": {"main_python_file_uri": "gs://your-bucket/path/to/script.py"},
    }

    submit_job = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        job=PYSPARK_JOB,
        region="us-west1",
        project_id="your-project-id"
    )



