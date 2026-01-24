from concurrent.futures import ThreadPoolExecutor, as_completed
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
    """
    Download both SMAP and CYGNSS data concurrently.
    If either fails, the function will raise an exception,
    causing the container to exit with a non-zero code.
    """
    if not download_date:
        msg = "DOWNLOAD_DATE environment variable is not set"
        logger.error(msg)
        raise ValueError(msg)
    
    logger.info(f"Starting data ingestion for date: {download_date}")
    
    executor = ThreadPoolExecutor(max_workers=2)
    futures = {
        executor.submit(smap_ingest.ingest, download_date): "SMAP",
        executor.submit(cygnss_ingest.ingest, download_date): "CYGNSS"
    }
    
    errors = []
    for future in as_completed(futures):
        source = futures[future]
        try:
            future.result()
            logger.info(f"{source} ingestion completed successfully")
        except Exception as e:
            msg = f"{source} ingestion failed: {str(e)}"
            logger.error(msg)
            errors.append(msg)
    
    executor.shutdown(wait=True)
    
    if errors:
        error_msg = "Data ingestion failed. Errors: " + "; ".join(errors)
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info("All data ingestion completed successfully")


if __name__ == '__main__':
    try:
        ingest_all()
        sys.exit(0) 
    except Exception as e:
        logger.error(f"Fatal error in ingest_all: {str(e)}")
        sys.exit(1)  
