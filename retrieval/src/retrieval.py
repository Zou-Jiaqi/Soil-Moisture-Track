"""
Soil moisture retrieval via adaptive-bandwidth GWR.

Trains a local spatially weighted regression on the trailing
WINDOW_DAYS of merged CYGNSS+SMAP data (pooled and kept resident in
memory as a single spatial KD-tree), then applies it pointwise to
today's preprocessed CYGNSS data to predict soil moisture at every
CYGNSS point.
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from gwr import build_spatial_coords, adaptive_tricube_weights, batched_local_regression

logger = logging.getLogger(__name__)

bucket_path = os.getenv("GCS_BUCKET_PATH")
parquet_cygnss_path = os.getenv("CYGNSS_PARQUET_PATH")
parquet_integrated_path = os.getenv("INTEGRATED_PARQUET_PATH")
parquet_soil_moisture_path = os.getenv("SOIL_MOISTURE_PARQUET_PATH")

cygnss_parquet_path = f'{bucket_path}{parquet_cygnss_path}/CYGNSS.parquet'
integrated_parquet_path = f'{bucket_path}{parquet_integrated_path}/INTEGRATED.parquet'
soil_moisture_parquet_path = f'{bucket_path}{parquet_soil_moisture_path}/SOIL_MOISTURE.parquet'

# The four CYGNSS-derived independent variables. These are the only
# regression inputs besides spatial position and the soil_moisture
# target, and they're present on both the merged training data and
# the raw (pre-merge) CYGNSS query points.
FEATURE_COLS = ["reflectivity", "incident_angle", "snr", "trailing_edge_slope"]
TARGET_COL = "soil_moisture"

WINDOW_DAYS = 30
K_NEIGHBORS = 30
RIDGE = 1e-6
CHUNK_SIZE = 50_000  # bounds peak memory of the (chunk, k, p) neighbor arrays


def _load_window(process_date, window_days):
    """Load and pool the trailing `window_days` of merged data, starting the day before process_date."""
    end = datetime.strptime(process_date, "%Y-%m-%d")
    frames = []
    for offset in range(1, window_days + 1):
        day = (end - timedelta(days=offset)).strftime("%Y-%m-%d")
        partition = Path(f"{integrated_parquet_path}/date={day}")
        if not partition.exists():
            continue
        frames.append(pd.read_parquet(partition))

    if not frames:
        raise ValueError(f"No merged data found in the {window_days}-day window ending {process_date}")

    return pd.concat(frames, ignore_index=True)


def _load_query(process_date):
    partition = Path(f"{cygnss_parquet_path}/date={process_date}")
    if not partition.exists():
        raise ValueError(f"No CYGNSS data found for date {process_date}")
    files = list(partition.glob("*.parquet"))
    if not files:
        raise ValueError(f"No CYGNSS parquet files found for date {process_date}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def predict(process_date):
    logger.info(f"Loading {WINDOW_DAYS}-day training window ending the day before {process_date}")
    train_df = _load_window(process_date, WINDOW_DAYS)
    train_df = train_df.dropna(subset=FEATURE_COLS + [TARGET_COL])
    logger.info(f"Training set: {len(train_df)} merged records (pooled across the window)")

    logger.info(f"Loading CYGNSS query set for {process_date}")
    query_df = _load_query(process_date)
    query_df = query_df.dropna(subset=FEATURE_COLS)
    logger.info(f"Query set: {len(query_df)} CYGNSS records")

    if len(train_df) <= K_NEIGHBORS:
        raise ValueError(
            f"Training set ({len(train_df)} records) is too small for k={K_NEIGHBORS} neighbors"
        )

    train_coords = build_spatial_coords(train_df["latitude"].values, train_df["longitude"].values)
    query_coords = build_spatial_coords(query_df["latitude"].values, query_df["longitude"].values)

    logger.info("Building spatial KD-tree on the pooled training window")
    tree = cKDTree(train_coords)

    # Standardize features (using training-set stats) before regression.
    # reflectivity, incident_angle, snr and trailing_edge_slope live on
    # very different scales, and RIDGE is a single fixed value added to
    # the diagonal of X'WX — without standardizing, that same ridge value
    # would barely regularize large-scale columns while distorting
    # small-scale ones. Standardizing doesn't change the OLS fit itself,
    # only makes the ridge term's effect comparable across columns.
    train_features = train_df[FEATURE_COLS].values
    feature_mean = train_features.mean(axis=0)
    feature_std = train_features.std(axis=0)
    feature_std = np.where(feature_std == 0, 1.0, feature_std)

    X_train = np.column_stack([np.ones(len(train_df)), (train_features - feature_mean) / feature_std])
    y_train = train_df[TARGET_COL].values
    X_query = np.column_stack([
        np.ones(len(query_df)),
        (query_df[FEATURE_COLS].values - feature_mean) / feature_std,
    ])

    predictions = np.empty(len(query_df), dtype=np.float64)

    for start in range(0, len(query_df), CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, len(query_df))

        distances, indices = tree.query(query_coords[start:end], k=K_NEIGHBORS)
        weights = adaptive_tricube_weights(distances)

        X_neighbors = X_train[indices]  # (chunk, k, p)
        y_neighbors = y_train[indices]  # (chunk, k)

        chunk_pred, _ = batched_local_regression(
            X_neighbors, y_neighbors, weights, X_query[start:end], ridge=RIDGE,
        )
        predictions[start:end] = chunk_pred
        logger.info(f"Processed {end}/{len(query_df)} query points")

    result_df = query_df[["latitude", "longitude"]].copy()
    result_df["soil_moisture_pred"] = predictions
    result_df["date"] = process_date

    Path(soil_moisture_parquet_path).mkdir(parents=True, exist_ok=True)
    result_df.to_parquet(
        soil_moisture_parquet_path,
        index=False,
        engine="pyarrow",
        compression="snappy",
        partition_cols=["date"],
    )
    logger.info(f"Saved {len(result_df)} soil moisture predictions to {soil_moisture_parquet_path}")
