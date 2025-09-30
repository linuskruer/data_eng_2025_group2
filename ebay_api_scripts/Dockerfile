FROM apache/airflow:2.7.0

USER root

# System dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories
RUN mkdir -p /opt/airflow/scripts /opt/airflow/data /opt/airflow/logs