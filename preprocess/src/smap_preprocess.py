import os
import logging
import h5py
import pandas as pd
import numpy as np
import file_utils
from pathlib import Path

logger = logging.getLogger(__name__)

bucket_path = os.getenv("GCS_BUCKET_PATH")
parquet_smap_path = os.getenv("SMAP_PARQUET_PATH")

parquet_path = f'{bucket_path}{parquet_smap_path}'

# 9km EASE GRID parameters
rows = 1624
columns = 3856
FILL_VALUE = -9999
VALID_FLAGS = [0, 8]


def preprocess(filedate):
    files = file_utils.get_smap_files_raw(filedate)
    parquet_file_path = f'{parquet_path}/SMAP.parquet'

    curr_date_path = Path(f'{parquet_file_path}/date={filedate}')
    if curr_date_path.exists():
        logger.warning(f"SMAP Partition {curr_date_path.name} already exists.")
        return

    result = np.zeros((rows, columns), dtype=np.float32)
    counts = np.zeros((rows, columns), dtype=np.uint8)

    for file in files:
        with h5py.File(file, mode='r') as f:
            sm_am = f['Soil_Moisture_Retrieval_Data_AM/soil_moisture'][...]
            sm_pm = f['Soil_Moisture_Retrieval_Data_PM/soil_moisture_pm'][...]
            quality_am = f['Soil_Moisture_Retrieval_Data_AM/retrieval_qual_flag'][...]
            quality_pm = f['Soil_Moisture_Retrieval_Data_PM/retrieval_qual_flag_pm'][...]

        valid_am = (sm_am != FILL_VALUE) & np.isin(quality_am, list(VALID_FLAGS))
        valid_pm = (sm_pm != FILL_VALUE) & np.isin(quality_pm, list(VALID_FLAGS))

        result += np.where(valid_am, sm_am, 0)
        counts += valid_am.astype(np.uint8)

        result += np.where(valid_pm, sm_pm, 0)
        counts += valid_pm.astype(np.uint8)

    with np.errstate(divide='ignore', invalid='ignore'):
        soil_moisture = np.where(counts > 0, result / counts, np.nan)

    df = pd.DataFrame({
        "row": np.repeat(np.arange(result.shape[0]), result.shape[1]),
        "column": np.tile(np.arange(result.shape[1]), result.shape[0]),
        "date": filedate.strftime("%Y-%m-%d"),
        "soil_moisture": soil_moisture.flatten(),
    })

    df = df.dropna(subset=["soil_moisture"])
    df.to_parquet(parquet_file_path,
                  index=False,
                  engine="pyarrow",
                  compression="snappy",
                  partition_cols=["date"]
                  )