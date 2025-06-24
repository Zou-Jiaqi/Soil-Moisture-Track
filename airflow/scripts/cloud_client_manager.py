import logging
from google.cloud import storage
from scripts import logger_config # noqa: F401

client = None

def get_gcs_client():
    global client
    if client is None:
        try:
            client = storage.Client()
        except:
            msg = "Could not connect to Google Cloud Storage"
            logging.error(msg)
            raise Exception(msg)
    return client

