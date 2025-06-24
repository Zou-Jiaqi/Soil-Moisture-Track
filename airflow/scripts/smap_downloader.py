import os
import earthaccess
import argparse
import logging
from scripts import logger_config # noqa: F401

auth = earthaccess.login()
logger = logging.getLogger(__name__)
path = os.getenv("DOWNLOAD_PATH")


def download(start, end, download_path=None, bounding_box=None):
    short_name = "SPL3SMP_E",
    version = "006"
    if not start.endswith("Z"):
        start += "T00:00:00Z"
    if not end.endswith("Z"):
        end += "T23:59:59Z"
    if bounding_box is None:
        bounding_box = (-180, -90, 180, 90)
    if not download_path:
        download_path = path

    granules = earthaccess.search_data(
        short_name=short_name,
        version=version,
        temporal=(start, end),
        bounding_box=bounding_box,
        cloud_hosted=True
    )
    files = earthaccess.download(granules, local_path=download_path)

    if len(granules) == len(files) and len(files) != 0:
        logger.info(f"SMAP data download succeeded.")
    elif len(granules) == 0:
        logger.error(f"No smap data found.")
        raise Exception("No smap data found.")
    else:
        logger.error(f"SMAP data download failed.")
        raise Exception("SMAP data download failed.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download satellite data from NSIDC.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("-d", "--directory", required=True, help="Target download directory")
    parser.add_argument("--start-date", required=True, help="Start date")
    parser.add_argument("--end-date", required=True, help="End date")

    return parser.parse_args()


# command line tool api
def execute():
    args = parse_args()
    path = args.directory
    start_date = args.start_date
    end_date = args.end_date
    download(start_date, end_date, path)


if __name__ == "__main__":
    execute()