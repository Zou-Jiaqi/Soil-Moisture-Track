FROM ubuntu:latest

LABEL authors="Jiaqi Zou"

ARG SMAP_RAW_PATH
ARG CYGNSS_RAW_PATH
ARG EARTH_DATA_AUTH

ENV CYGNSS_SHORTNAME_VERSION=1
ENV SMAP_SHORTNAME=SPL3SMP_E
ENV SMAP_VERSION=006
ENV PYTHONPATH=/opt/ingest
ENV GCS_BUCKET_PATH=/bucket

ENV GCS_BUCKET_PATH=$GCS_BUCKET_PATH
ENV SMAP_RAW_PATH=$SMAP_RAW_PATH
ENV CYGNSS_RAW_PATH=$CYGNSS_RAW_PATH

RUN whoami

COPY ./flask /opt/ingest

WORKDIR /opt/ingest

COPY ./requirements.txt /opt/ingest/requirements.txt

RUN apt install python3 pip3

USER ubuntu

RUN pip install -r /opt/ingest/requirements.txt --break-system-packages

RUN echo $EARTH_DATA_AUTH >> /home/ubuntu/.netrc
