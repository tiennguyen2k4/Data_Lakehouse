FROM bitnami/spark:latest

USER root

COPY jars/postgresql-42.4.5.jar /opt/bitnami/spark/jars/

RUN pip install psycopg2-binary

USER 1001
