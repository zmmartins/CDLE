# One image for every container of Lab 4: Python 3.12, Java 17 (Spark 3.5 runs on Java 8, 11 or 17),
# PySpark (which brings Spark itself and spark-submit), DuckDB to generate the logs and check the answers,
# and pytest for Part C. The lab's scripts are not copied into the image; compose.yaml mounts the lab folder at /lab.
FROM python:3.12-slim-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.9 duckdb==1.5.6 pytest==8.4.2
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    SPARK_CONF_DIR=/lab/conf PYTHONPATH=/lab
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
WORKDIR /lab
