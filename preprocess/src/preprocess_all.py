from concurrent.futures import ThreadPoolExecutor
import logging
import sys
import cygnss_preprocess
import smap_preprocess
import os

process_date = os.getenv("PROCESS_DATE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout, 
)

logger = logging.getLogger(__name__)


def process_all():
    executor = ThreadPoolExecutor(2)

    smap_exec = executor.submit(smap_preprocess.preprocess, process_date)
    cygnss_exec = executor.submit(cygnss_preprocess.preprocess, process_date)
    smap_exec.result()
    cygnss_exec.result()


if __name__ == '__main__':
    process_all()
