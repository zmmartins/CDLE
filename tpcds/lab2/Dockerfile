# One image for every container of Lab 2: Python with a pinned DuckDB.
# The lab's scripts are not copied into the image; compose.yaml mounts the lab folder at /lab.
FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb==1.4.5
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /lab
