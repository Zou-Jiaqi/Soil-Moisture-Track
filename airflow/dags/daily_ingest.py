from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts import smap_downloader, cygnss_downloader, smap_ingester, cygnss_ingester, file_uploader

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
    "start_date": datetime(2025, 6, 20),
}

# download and ingest the data of 3 days ago in case data source is not ready
def get_target_date(execution_date):
    return execution_date - timedelta(days=3)

# schedule interval: executed at 00:00 every day
with DAG("daily_ingest", default_args=default_args, schedule_interval="0 0 * * *", catchup=True) as dag:

    def download_smap(**context):
        date = get_target_date(context["execution_date"]).strftime("%Y-%m-%d")
        smap_downloader.download(date, date)


    def ingest_smap(**context):
        filedate = get_target_date(context["execution_date"]).date()
        smap_ingester.ingest(filedate)


    def archive_smap(**context):
        filedate = get_target_date(context["execution_date"]).date()
        file_uploader.upload_smap_raw(filedate)


    def upload_smap(**context):
        filedate = get_target_date(context["execution_date"]).date()
        file_uploader.upload_smap_parquet(filedate)


    def cleanup_smap(**context):
        filedate = get_target_date(context["execution_date"]).date()


    def download_cygnss(**context):
        date = get_target_date(context["execution_date"]).strftime("%Y-%m-%d")
        cygnss_downloader.download(date, date)


    def ingest_cygnss(**context):
        filedate = get_target_date(context["execution_date"]).date()
        cygnss_ingester.ingest(filedate)


    def archive_cygnss(**context):
        filedate = get_target_date(context["execution_date"]).date()
        file_uploader.upload_cygnss_raw(filedate)


    def upload_cygnss(**context):
        filedate = get_target_date(context["execution_date"]).date()
        file_uploader.upload_cygnss_parquet(filedate)


    def cleanup_cygnss(**context):
        filedate = get_target_date(context["execution_date"]).date()


    smap_download_task = PythonOperator(task_id="download_smap", python_callable=download_smap, provide_context=True)
    smap_ingest_task = PythonOperator(task_id=f"ingest_smap", python_callable=ingest_smap, provide_context=True)
    smap_archive_task = PythonOperator(task_id=f"archive_smap", python_callable=archive_smap, provide_context=True)
    smap_upload_task = PythonOperator(task_id=f"upload_smap", python_callable=upload_smap, provide_context=True)
    smap_cleanup_task = PythonOperator(task_id=f"cleanup_smap", python_callable=cleanup_smap, provide_context=True)

    cygnss_download_task = PythonOperator(task_id="download_cygnss", python_callable=download_cygnss, provide_context=True)
    cygnss_ingest_task = PythonOperator(task_id=f"ingest_cygnss", python_callable=ingest_cygnss, provide_context=True)
    cygnss_archive_task = PythonOperator(task_id=f"archive_cygnss", python_callable=archive_cygnss, provide_context=True)
    cygnss_upload_task = PythonOperator(task_id=f"upload_cygnss", python_callable=upload_cygnss, provide_context=True)
    cygnss_cleanup_task = PythonOperator(task_id=f"cleanup_cygnss", python_callable=cleanup_cygnss, provide_context=True)

    smap_download_task >> smap_archive_task >> smap_cleanup_task
    smap_download_task >> smap_ingest_task >> smap_upload_task >> smap_cleanup_task
    cygnss_download_task >> cygnss_archive_task >> cygnss_cleanup_task
    cygnss_download_task >> cygnss_ingest_task >> cygnss_upload_task >> cygnss_cleanup_task






