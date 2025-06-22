import os
import configparser


def set_env():
    config = configparser.ConfigParser()
    config.read('/opt/airflow/application.properties')

    DB_USER = config['postgresql']['DB_USER']
    DB_PASSWORD = config['postgresql']['DB_PASSWORD']
    DB_HOST = config['postgresql']['DB_HOST']
    DB_PORT = config['postgresql']['DB_PORT']
    DB_NAME = config['postgresql']['DB_NAME']
    LOGGING_PATH = config['path']['LOGGING_PATH']
    DATA_PATH = config['path']['DATA_PATH']

    os.environ['DB_USER'] = DB_USER
    os.environ['DB_PASSWORD'] = DB_PASSWORD
    os.environ['DB_HOST'] = DB_HOST
    os.environ['DB_PORT'] = DB_PORT
    os.environ['DB_NAME'] = DB_NAME
    os.environ['LOGGING_PATH'] = LOGGING_PATH
    os.environ['DATA_PATH'] = DATA_PATH


set_env()
