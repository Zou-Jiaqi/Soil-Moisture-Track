import os
import earthaccess
import logging

auth = earthaccess.login()
logger = logging.getLogger(__name__)

short_name = os.getenv("SMAP_SHORTNAME")
version = os.getenv("SMAP_VERSION")

bucket_path = os.getenv("GCS_BUCKET_PATH")
raw_smap_path = os.getenv("SMAP_RAW_PATH")
download_path = f'{bucket_path}{raw_smap_path}'


def ingest(datestr, bounding_box=(-180, -90, 180, 90)):
    start = datestr
    end = datestr
    if not start.endswith("Z"):
        start += "T00:00:00Z"
    if not end.endswith("Z"):
        end += "T23:59:59Z"

    download_target = f'{download_path}/{datestr}/'

    granules = earthaccess.search_data(
        short_name=short_name,
        version=version,
        temporal=(start, end),
        bounding_box=bounding_box,
        cloud_hosted=True
    )
    files = earthaccess.download(granules, local_path=download_target)

    if len(granules) == len(files) and len(files) != 0:
        msg = f"SMAP data download success. Number of files: {len(files)}"
        logger.info(msg)
    elif len(granules) == 0:
        msg = f"No smap data found."
        logger.error(msg)
        raise Exception(msg)
    else:
        msg = f"SMAP data download failed. Number of failed files: {len(granules) - len(files)}"
        logger.error(msg)
        raise Exception(msg)
