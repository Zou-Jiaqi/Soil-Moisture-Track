import logging
import sys
import cygnss_ingest
import os

download_date = os.getenv("DOWNLOAD_DATE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout, 
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    try:
        if not download_date:
            msg = "DOWNLOAD_DATE environment variable is not set"
            logger.error(msg)
            raise ValueError(msg)
        
        logger.info(f"Starting CYGNSS data ingestion for date: {download_date}")
        cygnss_ingest.ingest(download_date)
        logger.info("CYGNSS data ingestion completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in CYGNSS ingestion: {str(e)}")
        sys.exit(1)
        
