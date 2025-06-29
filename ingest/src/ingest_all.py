from concurrent.futures import ThreadPoolExecutor
from google.cloud import pubsub_v1
from fastavro import schemaless_reader, schemaless_writer
import json
import logging
import sys
import cygnss_ingest
import smap_ingest
import os

download_date = os.getenv("DOWNLOAD_DATE")

logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,   # Important! Cloud Run reads from stdout
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
