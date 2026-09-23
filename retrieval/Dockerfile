FROM ubuntu:latest

LABEL authors="Jiaqi Zou"

ENV PYTHONPATH=/opt/retrieval
ENV GCS_BUCKET_PATH=/bucket

COPY ./src /opt/retrieval

WORKDIR /opt/retrieval

COPY ./requirements.txt /opt/retrieval/requirements.txt

RUN apt-get update && apt-get install -y python3 python3-pip

USER ubuntu

RUN pip install -r /opt/retrieval/requirements.txt --break-system-packages

ENTRYPOINT exec python3 /opt/retrieval/retrieval_entrypoint.py
