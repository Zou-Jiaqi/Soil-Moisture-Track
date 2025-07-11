from concurrent.futures import ThreadPoolExecutor
import logging
import sys
import cygnss_ingest
import smap_ingest
import os

download_date = os.getenv("DOWNLOAD_DATE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout, 
)

logger = logging.getLogger(__name__)

def ingest_all():
    executor = ThreadPoolExecutor(2)

    smap_exec = executor.submit(smap_ingest.ingest, download_date)
    cygnss_exec = executor.submit(cygnss_ingest.ingest, download_date)
    smap_exec.result()
    cygnss_exec.result()


if __name__ == '__main__':
    ingest_all()
