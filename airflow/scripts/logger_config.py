import logging
import os
from scripts import property_manager # noqa: F401

def setup_global_logging(level=logging.INFO):
    log_dir = os.getenv("LOGGING_PATH")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "pipeline.log")

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )

setup_global_logging()
