import earthaccess
import argparse

auth = earthaccess.login()




def download(path, start, end, bounding_box=None):
    granules = earthaccess.search_data(
        short_name="SPL3SMP_E",
        temporal=("2024-01-01", "2024-01-31"),
        bounding_box=(-180, -40, 180, 40)
    )
    files = 
    print(f"Found {len(granules)} granules")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download satellite data from NSIDC.",
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


def main():
    args = parse_args()
    path = args.directory
    start_date = args.start_date
    end_date = args.end_date
    bounding_box = args.bounding_box
    force = args.force
    quiet = args.quiet



if __name__ == "__main__":\
    main()