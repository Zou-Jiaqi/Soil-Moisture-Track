import logging
import sys
import smap_preprocess
import os
from datetime import datetime

process_date = os.getenv("PROCESS_DATE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout, 
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    try:
        if not process_date:
            msg = "PROCESS_DATE environment variable is not set"
            logger.error(msg)
            raise ValueError(msg)
        
        
        logger.info(f"Starting SMAP preprocessing for date: {process_date}")
        smap_preprocess.preprocess(process_date)
        logger.info("SMAP preprocessing completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in SMAP preprocessing: {str(e)}")
        sys.exit(1)
