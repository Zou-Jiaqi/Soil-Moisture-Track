import os
import earthaccess
import argparse
import logging
import logger_config # noqa: F401  # side-effect import to configure logging
import property_manager # noqa: F401  # side-effect import to configure logging


class SMAPDownloader:

    def __init__(self):
        self.auth = earthaccess.login()
        self.logger = logging.getLogger(__name__)
        self.short_name = "SPL3SMP_E",
        self.version = "006"
        self.path = os.getenv("DATA_PATH")


    def download(self, start, end, path=None, bounding_box=None):
        if not start.endswith("Z"):
            start += "T00:00:00Z"
        if not end.endswith("Z"):
            end += "T23:59:59Z"
        if bounding_box is None:
            bounding_box = (-180, -90, 180, 90)
        if not path:
            path = self.path

        granules = earthaccess.search_data(
            short_name = self.short_name,
            version = self.version,
            temporal=(start, end),
            bounding_box=bounding_box
        )
        files = earthaccess.download(granules, local_path=path)

        if len(granules) == len(files):
            self.logger.info(f"SMAP data download succeeded.")
        else:
            self.logger.error(f"SMAP data download failed.")
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


def main(downloader):
    args = parse_args()
    path = args.directory
    start_date = args.start_date
    end_date = args.end_date
    downloader.download(start_date, end_date, path)


if __name__ == "__main__":
    downloader = SMAPDownloader()
    main(downloader)