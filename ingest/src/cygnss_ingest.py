import logging
import os
import subprocess

logger = logging.getLogger(__name__)

shortname = os.getenv("CYGNSS_SHORTNAME_VERSION")

bucket_path = os.getenv("GCS_BUCKET_PATH")
raw_cygnss_path = os.getenv('CYGNSS_RAW_PATH')
download_path = f'{bucket_path}{raw_cygnss_path}'


def ingest(datestr, bounding_box='"-180,-90,180,90"', force=False, quiet=False):
    start = datestr
    end = datestr
    if not start.endswith("Z"):
        start += "T00:00:00Z"
    if not end.endswith("Z"):
        end += "T23:59:59Z"

    cmd = (f"podaac-data-downloader -c {shortname} -d {download_path} "
           f"-b={bounding_box} "
           f"--start-date {start} --end-date {end}")

    if force:
        cmd += " --force"
    if quiet:
        cmd += " --quiet"

    logger.info("Command: " + cmd)
    result = subprocess.run(cmd, shell=True)

    if result.returncode == 0:
        msg = f"CYGNSS data download succeeded."
        logger.info(msg)
    else:
        msg = f"CYGNSS data download failed."
        logger.error(msg)
        raise Exception(msg)

