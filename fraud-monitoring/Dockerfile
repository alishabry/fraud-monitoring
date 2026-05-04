# Use official Airflow image as base
FROM apache/airflow:2.8.1-python3.9

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        libpq-dev \
        libssl-dev \
        libffi-dev \
        libxml2-dev \
        libxslt1-dev \
        zlib1g-dev \
        gcc \
        g++ \
        curl \
        wget \
        vim \
        git \
        nano \
        postgresql-client \
        unixodbc-dev \
        freetds-dev \
        freetds-bin \
        ssh-client \
        rsync \
        netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create necessary directories and set permissions
RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins /opt/airflow/config && \
    chown -R airflow:root /opt/airflow && \
    chmod -R 775 /opt/airflow

# Switch back to airflow user
USER airflow

# Upgrade pip and install Python dependencies
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Copy entrypoint script with proper permissions
COPY --chown=airflow:root entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy custom configurations
COPY --chown=airflow:root config/airflow.cfg /opt/airflow/airflow.cfg
COPY --chown=airflow:root config/webserver_config.py /opt/airflow/webserver_config.py

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# Expose ports
EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
