FROM tabulario/spark-iceberg

USER root

RUN apt-get update \
    && apt-get install -y \
    && apt-get clean

# COPY requirements.txt .

RUN mv /home/iceberg/spark_jars/postgresql-42.2.18.jar /opt/spark/jars/postgresql-42.2.18.jar

# WORKDIR /home/jovyan


