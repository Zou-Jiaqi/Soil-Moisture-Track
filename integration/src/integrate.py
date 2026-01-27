"""
Integration module to merge CYGNSS and SMAP data.

For each CYGNSS data point, converts its (lat, lon) to EASE-Grid (row, col)
and merges with SMAP data if the grid cell exists.
"""

import os
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from ease_grid_utils import latlon_to_grid_batch

logger = logging.getLogger(__name__)

bucket_path = os.getenv("GCS_BUCKET_PATH")
parquet_cygnss_path = os.getenv("CYGNSS_PARQUET_PATH")
parquet_smap_path = os.getenv("SMAP_PARQUET_PATH")
parquet_integrated_path = os.getenv("INTEGRATED_PARQUET_PATH")

cygnss_parquet_path = f'{bucket_path}{parquet_cygnss_path}/CYGNSS.parquet'
smap_parquet_path = f'{bucket_path}{parquet_smap_path}/SMAP.parquet'
integrated_parquet_path = f'{bucket_path}{parquet_integrated_path}/INTEGRATED.parquet'


def integrate(filedate):
    """
    Merge CYGNSS and SMAP data for a given date.
    
    For each CYGNSS data point:
    1. Convert (lat, lon) to EASE-Grid (row, col)
    2. If the grid cell exists in SMAP data, merge the records
    
    Output columns:
    - latitude, longitude (from CYGNSS)
    - reflectivity, incident_angle, snr, trailing_edge_slope (from CYGNSS)
    - vegetation_opacity, roughness_coefficient (from SMAP)
    
    Args:
        filedate: Date string in YYYY-MM-DD format
    """
    logger.info(f"Starting integration for date: {filedate}")
    
    # Check if output partition already exists
    output_partition = Path(f'{integrated_parquet_path}/date={filedate}')
    if output_partition.exists() and any(output_partition.glob("*.parquet")):
        logger.warning(f"Integrated partition {output_partition.name} already exists. Skipping.")
        return
    
    # Load CYGNSS data
    cygnss_path = Path(f'{cygnss_parquet_path}/date={filedate}')
    if not cygnss_path.exists():
        msg = f"CYGNSS data not found for date {filedate}. Skipping integration."
        logger.error(msg)
        raise ValueError(msg)
    
    cygnss_files = list(cygnss_path.glob("*.parquet"))
    if not cygnss_files:
        msg = f"No CYGNSS parquet files found for date {filedate}. Skipping integration."
        logger.error(msg)
        raise ValueError(msg)   
    
    logger.info(f"Loading CYGNSS data from {len(cygnss_files)} file(s)")
    cygnss_df = pd.concat([pd.read_parquet(f) for f in cygnss_files], ignore_index=True)
    logger.info(f"Loaded {len(cygnss_df)} CYGNSS records")
    
    # Load SMAP data
    smap_path = Path(f'{smap_parquet_path}/date={filedate}')
    if not smap_path.exists():
        msg = f"SMAP data not found for date {filedate}. Skipping integration."
        logger.error(msg)
        raise ValueError(msg)
    
    smap_files = list(smap_path.glob("*.parquet"))
    if not smap_files:
        msg = f"No SMAP parquet files found for date {filedate}. Skipping integration."
        logger.error(msg)
        raise ValueError(msg)
    
    logger.info(f"Loading SMAP data from {len(smap_files)} file(s)")
    smap_df = pd.concat([pd.read_parquet(f) for f in smap_files], ignore_index=True)
    logger.info(f"Loaded {len(smap_df)} SMAP records")
    
    # Convert CYGNSS (lat, lon) to EASE-Grid (row, col)
    logger.info("Converting CYGNSS coordinates to EASE-Grid")
    rows, cols = latlon_to_grid_batch(cygnss_df['latitude'].values, cygnss_df['longitude'].values)
    cygnss_df['row'] = rows
    cygnss_df['column'] = cols
    
    # Merge CYGNSS with SMAP data using pandas merge (more efficient than iterrows)
    logger.info("Merging CYGNSS and SMAP data")
    
    # Select only the columns we need from SMAP
    smap_merge = smap_df[['row', 'column', 'vegetation_opacity', 'roughness_coefficient']].copy()
    
    # Perform inner join on row and column to get matching grid cells
    merged_df = cygnss_df.merge(
        smap_merge,
        on=['row', 'column'],
        how='inner',
        suffixes=('', '_smap')
    )
    
    # Add date column for partitioning
    merged_df['date'] = filedate
    
    # Select and reorder columns for output
    # Note: brcs is not available in CYGNSS parquet (dropped during preprocessing)
    # If brcs is needed, it would require modifying the preprocessing step
    output_columns = [
        'latitude',
        'longitude',
        'reflectivity',
        'incident_angle',
        'snr',
        'trailing_edge_slope',
        'vegetation_opacity',
        'roughness_coefficient',
        'date'
    ]
    
    merged_df = merged_df[output_columns].copy()
    logger.info(f"Created {len(merged_df)} merged records")
    
    # Save to parquet with date partitioning
    # pandas will automatically create the date partition directory structure
    Path(integrated_parquet_path).mkdir(parents=True, exist_ok=True)
    merged_df.to_parquet(
        integrated_parquet_path,
        index=False,
        engine="pyarrow",
        compression="snappy",
        partition_cols=["date"]
    )
    
    logger.info(f"Integration completed. Saved {len(merged_df)} records to {integrated_parquet_path} (partitioned by date)")
