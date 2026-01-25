import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

bucket_path = os.getenv("GCS_BUCKET_PATH")

def get_cygnss_files_raw(date):
    raw_cygnss_path = os.getenv("CYGNSS_RAW_PATH")
    download_path = f'{bucket_path}{raw_cygnss_path}/{date}'

    data_path = Path(download_path)

    files = [f.absolute() for f in data_path.iterdir() if f.is_file() and f.name.endswith('.nc')]
    return files


def get_cygnss_files_parquet(date):
    parquet_cygnss_path = os.getenv("CYGNSS_PARQUET_PATH")
    parquet_path = f'{bucket_path}{parquet_cygnss_path}'

    data_path = Path(f'{parquet_path}/CYGNSS.parquet/date={date}')
    files = [f.absolute() for f in data_path.iterdir() if f.is_file()]
    return files
