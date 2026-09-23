"""
Entrypoint script for the retrieval Cloud Run Job.
"""

import logging
import sys
import os
import retrieval

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

        logger.info(f"Starting retrieval for date: {process_date}")
        retrieval.predict(process_date)
        logger.info("Retrieval completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in retrieval: {str(e)}")
        logger.exception(e)
        sys.exit(1)
