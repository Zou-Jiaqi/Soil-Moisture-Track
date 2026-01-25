from pathlib import Path

import file_utils
import pandas as pd
import numpy as np
import logging
import h5py
import os

logger = logging.getLogger(__name__)

bucket_path = os.getenv("GCS_BUCKET_PATH")
parquet_cygnss_path = os.getenv("CYGNSS_PARQUET_PATH")

parquet_path = f'{bucket_path}{parquet_cygnss_path}'


def preprocess(filedate):
    files = file_utils.get_cygnss_files_raw(filedate)
    parquet_file_path = f'{parquet_path}/CYGNSS.parquet'

    curr_date_path = Path(f'{parquet_file_path}/date={filedate}')

    if curr_date_path.exists():
        logger.warning(f"CYGNSS Partition {curr_date_path.name} already exists.")
        return

    for file in files:
        # Read CYGNSS records
        try:
            with h5py.File(file, mode='r') as f:
                ref_df = f['reflectivity_peak'][()]
                lat_df = f['sp_lat'][()]
                lon_df = f['sp_lon'][()]
                angle_df = f['sp_inc_angle'][()]
                gain_df = f['gps_ant_gain_db_i'][()]
                delay_df = f['brcs_ddm_peak_bin_delay_row'][()]
                brcs_df = f['brcs'][()]
                ddm_snr_df = f['ddm_snr'][()]

                mask = (
                        (ref_df < 0.1) &
                        (ref_df > 0) &
                        (gain_df > 0) &
                        (angle_df < 60) &
                        (delay_df >= 5) &
                        (delay_df <= 11) & 
                        (ddm_snr_df > 0)
                )

                df = pd.DataFrame({
                    "latitude": lat_df[mask].flatten(),
                    "longitude": lon_df[mask].flatten(),
                    "date": filedate,
                    "reflectivity": ref_df[mask].flatten(),
                    "incident_angle": angle_df[mask].flatten(),
                    "brcs": list(brcs_df[mask])
                })

                df["trailing_edge_slope"] = df["brcs"].apply(compute_tes_from_ddm)
                df.drop(columns=["brcs"], inplace=True)

                df.to_parquet(parquet_file_path,
                              index=False,
                              engine="pyarrow",
                              compression="snappy",
                              partition_cols=["date"]
                              )

        except Exception as e:
            logger.exception(f"Failed to ingest file: {file}")
            logger.exception(e)
            if curr_date_path.exists():
                os.remove(curr_date_path)
            raise e

    logger.info(f"Finished ingesting CYGNSS files.")



def compute_tes_from_ddm(ddm):
    try:
        delay_waveform = np.sum(ddm, axis=1)

        m = np.argmax(delay_waveform)
        if m + 3 >= len(delay_waveform):
            return np.nan  # not enough points after peak

        tes = (delay_waveform[m + 3] - delay_waveform[m]) / 3.0
        return tes
    except Exception:
        return np.nan

