FROM ubuntu:latest

LABEL authors="Jiaqi Zou"

ENV CYGNSS_SHORTNAME_VERSION=CYGNSS_L1_V3.2
ENV PYTHONPATH=/opt/ingest
ENV GCS_BUCKET_PATH=/bucket

ENV PATH="/home/ubuntu/.local/bin:${PATH}"

COPY ./src /opt/ingest

WORKDIR /opt/ingest

COPY ./requirements.txt /opt/ingest/requirements.txt

RUN apt-get update && apt-get install -y python3 python3-pip

USER ubuntu

RUN pip install -r /opt/ingest/requirements.txt --break-system-packages

ENTRYPOINT exec python3 /opt/ingest/cygnss_ingest_entrypoint.py
