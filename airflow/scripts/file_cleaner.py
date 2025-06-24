import os
import logging
from pathlib import Path

from scripts import logger_config, file_utils # noqa: F401

logger = logging.getLogger(__name__)
download_path = os.getenv("DOWNLOAD_PATH").rstrip("/")
parquet_path = os.getenv("PARQUET_PATH").rstrip("/")


def clean_smap(filedate):
    raw_files = file_utils.get_smap_files_raw(filedate)
    parquet_files = file_utils.get_smap_files_parquet(filedate)
    parquet_dir = Path(f'{parquet_path}/SMAP.parquet/date={filedate.strftime("%Y-%m-%d")}')
    for raw_file in raw_files:
        os.remove(raw_file)
        logging.info(f"File {raw_file} deleted.")
    for parquet_file in parquet_files:
        os.remove(parquet_file)
        logging.info(f"File {parquet_file} deleted.")
    os.removedirs(parquet_dir)


def clean_cygnss(filedate):
    raw_files = file_utils.get_cygnss_files_raw(filedate)
    parquet_files = file_utils.get_cygnss_files_parquet(filedate)
    parquet_dir = Path(f'{parquet_path}/CYGNSS.parquet/date={filedate.strftime("%Y-%m-%d")}')
    for raw_file in raw_files:
        os.remove(raw_file)
        logging.info(f"File {raw_file} deleted.")
    for parquet_file in parquet_files:
        os.remove(parquet_file)
        logging.info(f"File {parquet_file} deleted.")
    os.removedirs(parquet_dir)