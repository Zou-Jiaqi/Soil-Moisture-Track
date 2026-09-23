"""
GWR (Geographically Weighted Regression) core.

Adaptive-bandwidth spatial KNN, tricube kernel weighting, and a fully
vectorized (batched) local weighted least squares solve — no
per-point Python loop.
"""

import numpy as np
from pyproj import Transformer

EASE_GRID_PROJ = "EPSG:6933"
WGS84_PROJ = "EPSG:4326"
_to_ease_xy = Transformer.from_crs(WGS84_PROJ, EASE_GRID_PROJ, always_xy=True)


def latlon_to_xy_km(lat, lon):
    """Project lat/lon (degrees) to continuous EASE-Grid 2.0 x/y (km)."""
    x, y = _to_ease_xy.transform(np.asarray(lon), np.asarray(lat))
    return x / 1000.0, y / 1000.0


def build_spatial_coords(lat, lon):
    """Project lat/lon into the 2D (x, y) km coordinates used to build the KD-tree."""
    x, y = latlon_to_xy_km(lat, lon)
    return np.column_stack([x, y])


def adaptive_tricube_weights(distances):
    """
    distances: (n_query, k) ascending, as returned by cKDTree.query.

    Bandwidth h_i is the distance to the k-th (farthest) neighbor, so
    it automatically widens in sparse regions and narrows in dense
    ones. Tricube kernel: w = (1 - (d/h)^3)^3 for d < h, else 0.
    """
    h = distances[:, -1:]
    h = np.where(h <= 0, np.finfo(np.float64).eps, h)
    u = np.clip(distances / h, 0.0, 1.0)
    return (1.0 - u ** 3) ** 3


def batched_local_regression(X_neighbors, y_neighbors, weights, X_query, ridge=1e-6):
    """
    Solve one weighted least squares fit per query point, vectorized
    across all query points at once (batched normal equations).

    X_neighbors: (n, k, p) design matrix of each query point's k neighbors
    y_neighbors: (n, k)    target values of those neighbors
    weights:     (n, k)    tricube weights for those neighbors
    X_query:     (n, p)    design row for the query point itself
    ridge:       small L2 term added to the diagonal for numerical
                 stability when a local neighborhood is near-singular
                 (e.g. k close to p, or collinear features)

    Returns: (predictions (n,), coefficients (n, p))
    """
    n, k, p = X_neighbors.shape

    Xw = X_neighbors * weights[:, :, None]                     # (n, k, p)
    XtWX = np.einsum('nkp,nkq->npq', Xw, X_neighbors)          # (n, p, p)
    XtWy = np.einsum('nkp,nk->np', Xw, y_neighbors)            # (n, p)
    XtWX += ridge * np.eye(p)[None, :, :]

    try:
        beta = np.linalg.solve(XtWX, XtWy[:, :, None])[:, :, 0]
    except np.linalg.LinAlgError:
        # A handful of neighborhoods can still be singular even with
        # ridge (e.g. all-zero weights); fall back per-point for those.
        beta = np.empty((n, p))
        for i in range(n):
            try:
                beta[i] = np.linalg.solve(XtWX[i], XtWy[i])
            except np.linalg.LinAlgError:
                beta[i] = np.linalg.lstsq(XtWX[i], XtWy[i], rcond=None)[0]

    predictions = np.einsum('np,np->n', X_query, beta)
    return predictions, beta
