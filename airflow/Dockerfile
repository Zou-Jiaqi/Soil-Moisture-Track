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

COPY ./requirements.txt /requirements.txt

RUN pip install -r /requirements.txt && rm /requirements.txt

