FROM ubuntu:latest

LABEL authors="Jiaqi Zou"

ENV SMAP_SHORTNAME=SPL3SMP_E
ENV SMAP_VERSION=006
ENV PYTHONPATH=/opt/ingest
ENV GCS_BUCKET_PATH=/bucket
ENV NUMBER_OF_RETRIES=10

ENV PATH="/home/ubuntu/.local/bin:${PATH}"

COPY ./src /opt/ingest

WORKDIR /opt/ingest

COPY ./requirements.txt /opt/ingest/requirements.txt

RUN apt-get update && apt-get install -y python3 python3-pip

USER ubuntu

RUN pip install -r /opt/ingest/requirements.txt --break-system-packages

ENTRYPOINT exec python3 /opt/ingest/smap_ingest_entrypoint.py
