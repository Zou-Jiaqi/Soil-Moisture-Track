import logging
import sys
import cygnss_preprocess
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
        
        # Ensure date is in YYYY-MM-DD format (string)
        if isinstance(process_date, str):
            # Validate format
            datetime.strptime(process_date, "%Y-%m-%d")
            date_str = process_date
        else:
            # If it's a datetime object, convert to string
            date_str = process_date.strftime("%Y-%m-%d")
        
        logger.info(f"Starting CYGNSS preprocessing for date: {date_str}")
        cygnss_preprocess.preprocess(date_str)
        logger.info("CYGNSS preprocessing completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in CYGNSS preprocessing: {str(e)}")
        sys.exit(1)
