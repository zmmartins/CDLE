# One image for every container of Lab 3: Python with a pinned DuckDB, numpy for the erasure code,
# and the DuckDB extensions the lab needs (avro, httpfs), installed now so that the lab runs offline.
# The lab's scripts are not copied into the image; compose.yaml mounts the lab folder at /lab.
FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb==1.5.6 numpy==2.3.4 \
 && python -c "import duckdb; c = duckdb.connect(); c.install_extension('avro'); c.install_extension('httpfs')"
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /lab
