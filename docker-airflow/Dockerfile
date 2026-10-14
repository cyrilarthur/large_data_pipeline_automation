FROM apache/airflow:slim-2.11.2rc1-python3.11

# Switch to root to update OS packages
USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user before installing Python packages
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    pandas \
    mysql-connector-python \
    scikit-learn