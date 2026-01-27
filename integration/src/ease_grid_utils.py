"""
EASE-Grid 2.0 coordinate conversion utilities for SMAP data.

SMAP uses EASE-Grid 2.0 Global (EPSG:6933) with 9km resolution.
Grid dimensions: 1624 rows x 3856 columns
Cell size: ~9,008.05 meters
"""

import numpy as np
from pyproj import Transformer

# EASE-Grid 2.0 Global parameters for 9km resolution (SMAP)
GRID_ROWS = 1624
GRID_COLUMNS = 3856
GRID_CELL_SIZE = 9008.05  # Actual cell size in meters (approximately 9km)

# EASE-Grid 2.0 Global projection (EPSG:6933)
EASE_GRID_PROJ = "EPSG:6933"
WGS84_PROJ = "EPSG:4326"

# Create transformer (EASE-Grid to WGS84)
_transformer_to_wgs84 = Transformer.from_crs(EASE_GRID_PROJ, WGS84_PROJ, always_xy=True)
_transformer_to_ease = Transformer.from_crs(WGS84_PROJ, EASE_GRID_PROJ, always_xy=True)

# EASE-Grid 2.0 Global extent (in meters)
EASE_GRID_X_MIN = -17367530.44
EASE_GRID_X_MAX = 17367530.44
EASE_GRID_Y_MIN = -7314540.83
EASE_GRID_Y_MAX = 7314540.83


def grid_to_latlon(row, col):
    """
    Convert EASE-Grid 2.0 (row, col) to (latitude, longitude) at grid cell center.
    
    Args:
        row: Grid row index (0 to 1623)
        col: Grid column index (0 to 3855)
    
    Returns:
        tuple: (latitude, longitude) in degrees
    
    Raises:
        ValueError: If row or col is out of valid range
    """
    if not (0 <= row < GRID_ROWS):
        raise ValueError(f"Row {row} is out of range [0, {GRID_ROWS-1}]")
    if not (0 <= col < GRID_COLUMNS):
        raise ValueError(f"Column {col} is out of range [0, {GRID_COLUMNS-1}]")
    
    # Convert row/col to EASE-Grid x/y coordinates (in meters)
    # EASE-Grid 2.0 Global origin is at center (0, 0)
    # Row 0 is at top (maximum y), row increases downward
    # Column 0 is at left (minimum x), column increases rightward
    x = EASE_GRID_X_MIN + (col + 0.5) * GRID_CELL_SIZE
    y = EASE_GRID_Y_MAX - (row + 0.5) * GRID_CELL_SIZE
    
    # Transform from EASE-Grid coordinates to WGS84 lat/lon
    lon, lat = _transformer_to_wgs84.transform(x, y)

    return lat, lon


def latlon_to_grid(lat, lon):
    """
    Convert (latitude, longitude) to EASE-Grid 2.0 (row, col).
    
    Args:
        lat: Latitude in degrees (-90 to 90)
        lon: Longitude in degrees (-180 to 180)
    
    Returns:
        tuple: (row, col) grid indices
    
    Raises:
        ValueError: If lat or lon is out of valid range
    """
    if not (-90 <= lat <= 90):
        raise ValueError(f"Latitude {lat} is out of range [-90, 90]")
    if not (-180 <= lon <= 180):
        raise ValueError(f"Longitude {lon} is out of range [-180, 180]")
    
    # Transform from WGS84 lat/lon to EASE-Grid coordinates
    x, y = _transformer_to_ease.transform(lon, lat)
    
    # Convert EASE-Grid x/y to row/col
    col = int((x - EASE_GRID_X_MIN) / GRID_CELL_SIZE)
    row = int((EASE_GRID_Y_MAX - y) / GRID_CELL_SIZE)
    
    # Clamp to valid range (in case of floating point precision issues or points near edges)
    row = max(0, min(row, GRID_ROWS - 1))
    col = max(0, min(col, GRID_COLUMNS - 1))
    
    return row, col


def grid_to_latlon_batch(rows, cols):
    """
    Convert multiple EASE-Grid 2.0 (row, col) pairs to (latitude, longitude) in batch.
    
    Args:
        rows: Array-like of row indices
        cols: Array-like of column indices
    
    Returns:
        tuple: (latitudes, longitudes) as numpy arrays
    """
    rows = np.asarray(rows)
    cols = np.asarray(cols)
    
    # Convert to EASE-Grid coordinates
    x = EASE_GRID_X_MIN + (cols + 0.5) * GRID_CELL_SIZE
    y = EASE_GRID_Y_MAX - (rows + 0.5) * GRID_CELL_SIZE
    
    # Transform to WGS84
    lons, lats = _transformer_to_wgs84.transform(x, y)
    
    return np.asarray(lats), np.asarray(lons)


def latlon_to_grid_batch(lats, lons):
    """
    Convert multiple (latitude, longitude) pairs to EASE-Grid 2.0 (row, col) in batch.
    
    Args:
        lats: Array-like of latitudes
        lons: Array-like of longitudes
    
    Returns:
        tuple: (rows, cols) as numpy arrays
    """
    lats = np.asarray(lats)
    lons = np.asarray(lons)
    
    # Transform to EASE-Grid coordinates
    x, y = _transformer_to_ease.transform(lons, lats)
    
    # Convert to row/col
    cols = ((x - EASE_GRID_X_MIN) / GRID_CELL_SIZE).astype(int)
    rows = ((EASE_GRID_Y_MAX - y) / GRID_CELL_SIZE).astype(int)
    
    # Clamp to valid range
    rows = np.clip(rows, 0, GRID_ROWS - 1)
    cols = np.clip(cols, 0, GRID_COLUMNS - 1)
    
    return rows, cols
