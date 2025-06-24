FROM apache:airflow:2.9.1

LABEL authors="Jiaqi Zou"

COPY ./dags /opt/airflow/dags

COPY ./scripts /opt/airflow/scripts

COPY ./requirements.txt /requirements.txt

RUN pip install -r /requirements.txt && rm /requirements.txt

