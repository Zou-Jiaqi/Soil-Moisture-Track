from pathlib import Path
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

    download_target = f'{download_path}/{datestr}/'

    netrc_path = Path("~/.netrc").expanduser()
    if not netrc_path.exists():
        logger.info(f"creating netrc at {netrc_path.absolute()}")
        try:
            with open(netrc_path.absolute(), "w") as f:
                f.write("machine urs.earthdata.nasa.gov\n")
                f.write(f"\tlogin {os.getenv('EARTHDATA_USERNAME')}\n")
                f.write(f"\tpassword {os.getenv('EARTHDATA_PASSWORD')}\n")
            os.chmod(netrc_path.absolute(), 0o700)
        except Exception as e:
            logger.error(f"failed to create netrc at {netrc_path.absolute()}")
            raise e

    cmd = (f"podaac-data-downloader -c {shortname} -d {download_target} "
           f"-b={bounding_box} "
           f"--start-date {start} --end-date {end}")

    if force:
        cmd += " --force"
    if quiet:
        cmd += " --quiet"

    logger.info("Command: " + cmd)
    result = subprocess.run(cmd, shell=True)

    citation = f"{download_target}/CYGNSS_L1_V3.2.citation.txt"
    if Path(citation).exists():
        os.remove(citation)

    if result.returncode == 0:
        msg = f"CYGNSS data download succeeded."
        logger.info(msg)
    else:
        msg = f"CYGNSS data download failed."
        logger.error(msg)
        raise Exception(msg)
        
