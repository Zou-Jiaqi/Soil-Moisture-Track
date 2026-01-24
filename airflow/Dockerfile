FROM apache/airflow:2.9.1

LABEL authors="Jiaqi Zou"

ARG GSC_BUCKET_NAME
ARG UPLOAD_PATH_SMAP_RAW
ARG UPLOAD_PATH_CYGNSS_RAW
ARG UPLOAD_PATH_PARQUET

ENV GSC_BUCKET_NAME=$GSC_BUCKET_NAME
ENV UPLOAD_PATH_SMAP_RAW=$UPLOAD_PATH_SMAP_RAW
ENV UPLOAD_PATH_CYGNSS_RAW=$UPLOAD_PATH_CYGNSS_RAW
ENV UPLOAD_PATH_PARQUET=$UPLOAD_PATH_PARQUET

ENV LOGGING_PATH=/opt/airflow/logs
ENV DOWNLOAD_PATH=/opt/airflow/data
ENV PARQUET_PATH=/opt/airflow/data/parquets
ENV PYTHONPATH=/opt/airflow

COPY ./dags /opt/airflow/dags

COPY ./scripts /opt/airflow/scripts

COPY ./requirements.txt /opt/airflow/requirements.txt

# Install packages with constraint file to avoid conflicts with Airflow dependencies
# Try to use Airflow's constraint file, fallback to regular install if it fails
RUN pip install --upgrade pip setuptools wheel && \
    (pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt \
    -r /opt/airflow/requirements.txt || \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt)

# Set the user to airflow (required by the base image)
USER airflow

# Start Airflow standalone (runs both webserver and scheduler)
# For production, you might want to run these separately or use a process manager
CMD ["airflow", "standalone"]