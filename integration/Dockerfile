FROM ubuntu:latest

LABEL authors="Jiaqi Zou"

ENV PYTHONPATH=/opt/integration
ENV GCS_BUCKET_PATH=/bucket

COPY ./src /opt/integration

WORKDIR /opt/integration

COPY ./requirements.txt /opt/integration/requirements.txt

RUN apt-get update && apt-get install -y python3 python3-pip

USER ubuntu

RUN pip install -r /opt/integration/requirements.txt --break-system-packages

ENTRYPOINT exec python3 /opt/integration/integrate_entrypoint.py
