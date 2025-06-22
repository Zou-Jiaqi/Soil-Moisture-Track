import argparse
import logging
import os
import logger_config # noqa: F401  # side-effect import to configure logging
import property_manager # noqa: F401  # side-effect import to configure logging
import subprocess

class CYGNSSDownloader:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.short_name = "CYGNSS_L1_V3.2"
        self.path = os.getenv("DATA_PATH")


    def download(self, start, end, path=None, bounding_box=None, force=False, quiet=False):
        if not start.endswith("Z"):
            start += "T00:00:00Z"
        if not end.endswith("Z"):
            end += "T23:59:59Z"
        if bounding_box is None:
            bounding_box = '"-180,-90,180,90"'
        if path is None:
            path = self.path

        cmd = (f"podaac-data-downloader -c {self.short_name} -d {path} "
               f"-b={bounding_box} "
               f"--start-date {start} --end-date {end}")

        if force:
            cmd += " --force"
        if quiet:
            cmd += " --quiet"

        self.logger.info("Command: " + cmd)
        result = subprocess.run(cmd, shell=True)
        if result.returncode == 0:
            self.logger.info(f"CYGNSS data download succeeded.")
        else:
            self.logger.error(f"CYGNSS data download failed.")
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

def main(downloader):
    args = parse_args()
    path = args.directory
    start_date = args.start_date
    end_date = args.end_date
    downloader.download(start_date, end_date, path)


if __name__ == "__main__":
    downloader = CYGNSSDownloader()
    main(downloader)