FROM bitnami/spark:latest

USER root

COPY jars/* /opt/bitnami/spark/jars/

RUN pip install psycopg2-binary python-dotenv

USER 1001
