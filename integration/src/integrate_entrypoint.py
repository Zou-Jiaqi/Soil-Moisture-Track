"""
Entrypoint script for the integration Cloud Run Job.
"""

import logging
import sys
import os
import integrate

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
        
        # Validate date format (YYYY-MM-DD)
        if len(process_date) != 10 or process_date.count('-') != 2:
            msg = f"PROCESS_DATE must be in YYYY-MM-DD format, got: {process_date}"
            logger.error(msg)
            raise ValueError(msg)
        
        logger.info(f"Starting integration for date: {process_date}")
        integrate.integrate(process_date)
        logger.info("Integration completed successfully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in integration: {str(e)}")
        logger.exception(e)
        sys.exit(1)
