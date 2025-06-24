import logging
import h5py
import os
import re
from pathlib import Path
from scripts import logger_config # noqa: F401

logger = logging.getLogger(__name__)
download_path = os.getenv("DOWNLOAD_PATH").rstrip("/")
parquet_path = os.getenv("PARQUET_PATH").rstrip("/")



def get_smap_files_raw(date=None):
    data_path = Path(download_path)
    pattern = 'SMAP_L3_SM_P_E_'
    if date is not None:
        pattern += date.strftime('%Y%m%d')

    files = [f.name for f in data_path.iterdir() if f.name.startswith(pattern) and f.is_file()]
    return files


def get_cygnss_files_raw(date=None):
    data_path = Path(download_path)
    pattern = 'cyg0[1-8].ddmi.s'
    if date is not None:
        pattern += date.strftime('%Y%m%d')

    files = [f.name for f in data_path.iterdir() if re.match(pattern, f.name) and f.is_file()]
    return files


def get_smap_files_parquet(date=None):
    data_path = Path(parquet_path)
    pattern = 'SMAP_'
    if date is not None:
        pattern += date.strftime('%Y-%m-%d')
    files = [f.name for f in data_path.iterdir() if re.match(pattern, f.name) and f.is_file()]
    return files


def get_cygnss_files_parquet(date=None):
    data_path = Path(parquet_path)
    pattern = 'cygnss_'
    if date is not None:
        pattern += date.strftime('%Y-%m-%d')
    files = [f.name for f in data_path.iterdir() if re.match(pattern, f.name) and f.is_file()]
    return files


def list_all_smap_groups():
    files = get_smap_files_raw()
    if len(files) == 0:
        print("No smap files found")
        return
    file = files[0]
    filepath = download_path + "/" + file
    with h5py.File(filepath, mode='r') as f:
        for key in f.keys():
            dataset = f[key]
            print('key: ', key, '---------------------------------------')
            for item in dataset:
                print('data:', item)


def list_all_groups(self):
    files = self.get_files()
    if len(files) == 0:
        print("No cygnss files found")
        return
    file = files[0]
    filepath = self.download_path + "/" + file
    with h5py.File(filepath, mode='r') as f:
        for key in f.keys():
            print('key: ', key, '---------------------------------------')