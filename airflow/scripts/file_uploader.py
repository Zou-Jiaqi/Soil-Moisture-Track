import os
import logging
from scripts import logger_config, file_utils  # noqa: F401
from scripts.cloud_client_manager import get_gcs_client

logger = logging.getLogger(__name__)

def upload(file, target):
    client = get_gcs_client()
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    bucket_exist = client.lookup_bucket(bucket_name)

    if not bucket_exist:
        msg = f"Bucket {bucket_name} does not exist."
        logger.error(msg)
        raise Exception(msg)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(target)

    if blob.exists():
        logger.info(f"File {target} already exists.")
        return

    try:
        blob.upload_from_filename(file)
    except Exception as e:
        msg = f"Failed to upload {target} to {bucket_name}."
        logger.error(msg)
        raise Exception(msg)
    logger.info(f"Uploaded {file} to gs://{bucket_name}/{target}")


def upload_smap_raw(filedate):
    files = file_utils.get_smap_files_raw(filedate)
    upload_path = os.getenv("UPLOAD_PATH_SMAP_RAW").rstrip("/")
    for file in files:
        filename = file.split("/")[-1]
        target = upload_path + "/" + filename
        upload(file, target)

def upload_smap_parquet(filedate):
    files = file_utils.get_smap_files_parquet(filedate)
    upload_path = os.getenv("UPLOAD_PATH_PARQUET").rstrip("/")
    for file in files:
        filename = file.split("/")[-1]
        target = upload_path + "/" + filename
        upload(file, target)


def upload_cygnss_raw(filedate):
    files = file_utils.get_cygnss_files_raw(filedate)
    upload_path = os.getenv("UPLOAD_PATH_CYGNSS_RAW").rstrip("/")
    for file in files:
        filename = file.split("/")[-1]
        target = upload_path + "/" + filename
        upload(file, target)

def upload_cygnss_parquet(filedate):
    files = file_utils.get_cygnss_files_parquet(filedate)
    upload_path = os.getenv("UPLOAD_PATH_PARQUET").rstrip("/")
    for file in files:
        filename = file.split("/")[-1]
        target = upload_path + "/" + filename
        upload(file, target)


