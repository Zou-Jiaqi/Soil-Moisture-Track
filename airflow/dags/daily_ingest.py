from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from scripts.cygnss_ingester import CYGNSSIngester
from scripts.smap_downloader import SMAPDownloader
from scripts.cygnss_downloader import CYGNSSDownloader
from scripts.smap_ingester import SmapIngester
from scripts.initdb import init_db

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

    def init_database(**context):
        partition_date = get_target_date(context["execution_date"]).date()
        init_db(partition_date)

    def run_smap(**context):
        date = get_target_date(context["execution_date"]).strftime("%Y-%m-%d")
        downloader = SMAPDownloader()
        downloader.download(date, date)


    def ingest_smap(**context):
        ingester = SmapIngester()
        filedate = get_target_date(context["execution_date"]).date()
        ingester.ingest(filedate)


    def run_cygnss(**context):
        date = get_target_date(context["execution_date"]).strftime("%Y-%m-%d")
        downloader = CYGNSSDownloader()
        downloader.download(date, date)


    def ingest_cygnss(**context):
        ingester = CYGNSSIngester()
        filedate = get_target_date(context["execution_date"]).date()
        ingester.ingest(filedate)

    init_db_task = PythonOperator(task_id="init_db", python_callable=init_database, provide_context=True)
    smap_download_task = PythonOperator(task_id="download_smap", python_callable=run_smap, provide_context=True)
    smap_ingest_task = PythonOperator(task_id=f"ingest_smap", python_callable=ingest_smap, provide_context=True)
    cygnss_download_task = PythonOperator(task_id="download_cygnss", python_callable=run_cygnss, provide_context=True)
    cygnss_ingest_task = PythonOperator(task_id=f"ingest_cygnss", python_callable=ingest_cygnss, provide_context=True)
    init_db_task >> smap_ingest_task
    init_db_task >> cygnss_ingest_task
    smap_download_task >> smap_ingest_task
    cygnss_download_task >> cygnss_ingest_task






