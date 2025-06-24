import argparse
import logging
import os
import subprocess
from scripts import logger_config  # noqa: F401

logger = logging.getLogger(__name__)
path = os.getenv("DOWNLOAD_PATH")


def download(start, end, download_path=None, bounding_box=None, force=False, quiet=False):
    short_name = "CYGNSS_L1_V3.2"
    if not start.endswith("Z"):
        start += "T00:00:00Z"
    if not end.endswith("Z"):
        end += "T23:59:59Z"
    if bounding_box is None:
        bounding_box = '"-180,-90,180,90"'
    if download_path is None:
        download_path = path

    cmd = (f"podaac-data-downloader -c {short_name} -d {download_path} "
           f"-b={bounding_box} "
           f"--start-date {start} --end-date {end}")

    if force:
        cmd += " --force"
    if quiet:
        cmd += " --quiet"

    logger.info("Command: " + cmd)
    result = subprocess.run(cmd, shell=True)
    if result.returncode == 0:
        logger.info(f"CYGNSS data download succeeded.")
    else:
        logger.error(f"CYGNSS data download failed.")
        raise Exception("CYGNSS data download failed.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download satellite data from PODDAC.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("-d", "--directory", required=True, help="Target download directory")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")

    return parser.parse_args()


# command tool api
def execute():
    args = parse_args()
    path = args.directory
    start_date = args.start_date
    end_date = args.end_date
    download(start_date, end_date, path)


if __name__ == "__main__":
    execute()
