# Airflow base image (Python 3.11)
FROM apache/airflow:2.9.3-python3.11

# Switch to root to install OS deps if needed
USER root

# Install system deps (if any)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Back to airflow user
USER airflow

# Copy your code into the image
WORKDIR /opt/airflow
COPY ebay_auth.py /opt/airflow/scripts/ebay_auth.py
COPY all_in_one.py /opt/airflow/scripts/all_in_one.py
COPY ebay_dag.py /opt/airflow/dags/ebay_dag.py

# Python libs required by the script
# (requests already in base image, but we pin; add clickhouse-driver)
RUN pip install --no-cache-dir "requests>=2.31.0" "clickhouse-driver>=0.2.9"

# Default command
CMD ["bash", "-lc", "airflow db upgrade && airflow webserver & airflow scheduler"]
