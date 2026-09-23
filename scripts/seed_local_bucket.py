"""
Seeds local_bucket with synthetic *preprocessed-level* CYGNSS/SMAP
parquet data, so integrate.py and retrieval.py can be run end-to-end
locally with plain `python3`, no GCP access, no Docker.

Raw ingest (podaac-data-downloader / earthaccess, both credentialed
and network-dependent) and the raw NetCDF/HDF5 parsing in preprocess/
are NOT simulated here -- they're out of scope for this smoke test.
This script writes directly at the parquet schema that preprocess/
cygnss and preprocess/smap already produce, so it exercises exactly
the same integrate.py / retrieval.py code paths as production.
"""

import os
import sys
import shutil
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "integration" / "src"))
from ease_grid_utils import latlon_to_grid_batch, grid_to_latlon_batch  # noqa: E402

RNG = np.random.default_rng(42)

BBOX_LAT = (30.0, 32.0)
BBOX_LON = (-100.0, -98.0)
POINTS_PER_DAY = 300


def true_soil_moisture(lat, lon):
    """Smooth synthetic soil-moisture field, so the local regression has real signal to find."""
    return 0.25 + 0.1 * np.sin(np.radians(lat) * 6) * np.cos(np.radians(lon) * 4)


def _clean_partition(dataset_path, date_str):
    partition = Path(f"{dataset_path}/date={date_str}")
    if partition.exists():
        shutil.rmtree(partition)


def seed_day(bucket, cygnss_parquet_path, smap_parquet_path, date_str, with_smap):
    lat = RNG.uniform(*BBOX_LAT, POINTS_PER_DAY)
    lon = RNG.uniform(*BBOX_LON, POINTS_PER_DAY)
    sm_true = true_soil_moisture(lat, lon)

    cygnss_df = pd.DataFrame({
        "latitude": lat,
        "longitude": lon,
        "date": date_str,
        "reflectivity": np.clip(
            0.02 + 0.15 * sm_true + RNG.normal(0, 0.005, POINTS_PER_DAY), 0.001, 0.099
        ),
        "incident_angle": RNG.uniform(0, 59, POINTS_PER_DAY),
        "snr": RNG.uniform(1, 20, POINTS_PER_DAY),
        "trailing_edge_slope": RNG.normal(0, 0.3, POINTS_PER_DAY),
    })

    cygnss_dataset = f"{bucket}{cygnss_parquet_path}/CYGNSS.parquet"
    _clean_partition(cygnss_dataset, date_str)
    cygnss_df.to_parquet(
        cygnss_dataset, index=False, engine="pyarrow", compression="snappy", partition_cols=["date"]
    )
    Path(f"{cygnss_dataset}/date={date_str}/_SUCCESS").touch()
    print(f"  CYGNSS: wrote {len(cygnss_df)} records for {date_str}")

    if not with_smap:
        return

    rows, cols = latlon_to_grid_batch(lat, lon)
    grid_df = pd.DataFrame({"row": rows, "column": cols}).drop_duplicates().reset_index(drop=True)
    cell_lat, cell_lon = grid_to_latlon_batch(grid_df["row"].values, grid_df["column"].values)
    cell_sm_true = true_soil_moisture(cell_lat, cell_lon)

    smap_df = pd.DataFrame({
        "row": grid_df["row"],
        "column": grid_df["column"],
        "date": date_str,
        "soil_moisture": cell_sm_true + RNG.normal(0, 0.01, len(grid_df)),
        "vegetation_opacity": RNG.uniform(0, 1, len(grid_df)),
        "roughness_coefficient": RNG.uniform(0, 0.3, len(grid_df)),
    })

    smap_dataset = f"{bucket}{smap_parquet_path}/SMAP.parquet"
    _clean_partition(smap_dataset, date_str)
    smap_df.to_parquet(
        smap_dataset, index=False, engine="pyarrow", compression="snappy", partition_cols=["date"]
    )
    Path(f"{smap_dataset}/date={date_str}/_SUCCESS").touch()
    print(f"  SMAP:    wrote {len(smap_df)} grid cells for {date_str}")


def main():
    bucket = os.environ["GCS_BUCKET_PATH"]
    cygnss_parquet_path = os.environ["CYGNSS_PARQUET_PATH"]
    smap_parquet_path = os.environ["SMAP_PARQUET_PATH"]
    process_date = os.environ["PROCESS_DATE"]
    history_days = int(os.environ.get("SEED_HISTORY_DAYS", "5"))

    end = datetime.strptime(process_date, "%Y-%m-%d")

    for offset in range(history_days, 0, -1):
        day = (end - timedelta(days=offset)).strftime("%Y-%m-%d")
        print(f"Seeding historical day {day} (CYGNSS + SMAP, for integrate.py)")
        seed_day(bucket, cygnss_parquet_path, smap_parquet_path, day, with_smap=True)

    print(f"Seeding {process_date} (CYGNSS only -- this is the retrieval query set)")
    seed_day(bucket, cygnss_parquet_path, smap_parquet_path, process_date, with_smap=False)


if __name__ == "__main__":
    main()
