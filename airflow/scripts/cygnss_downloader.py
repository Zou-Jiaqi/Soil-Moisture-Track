import argparse
import logger_config

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download satellite data from PODDAC.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("-d", "--directory", required=True, help="Target download directory")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("-b", "--bounding-box", required=True, help="Bounding box: minLon,minLat,maxLon,maxLat")

    parser.add_argument("-f", "--force", action="store_true", help="Force re-download even if files exist")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress verbose output")
    parser.add_argument("-h", "--help", action="help", help="Show this help message and exit")

    return parser.parse_args()