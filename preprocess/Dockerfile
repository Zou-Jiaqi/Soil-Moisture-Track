FROM ubuntu:latest

LABEL authors="Jiaqi Zou"

ENV PYTHONPATH=/opt/preprocess
ENV GCS_BUCKET_PATH=/bucket

COPY ./src /opt/preprocess

WORKDIR /opt/preprocess

COPY ./requirements.txt /opt/preprocess/requirements.txt

RUN apt-get update && apt-get install -y python3 python3-pip

USER ubuntu

RUN pip install -r /opt/preprocess/requirements.txt --break-system-packages

ENTRYPOINT exec python3 /opt/preprocess/process_all.py
