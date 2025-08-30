# Custom Airflow image with Pokemon Analytics dependencies
FROM apache/airflow:2.7.2-python3.9

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python dependencies for Pokemon Analytics workflows
# Note: Keep SQLAlchemy compatible with Airflow 2.7.2
RUN pip install --no-cache-dir \
    beautifulsoup4==4.12.2 \
    pandas==2.1.0 \
    psycopg2-binary==2.9.7 \
    requests==2.31.0 \
    lxml==4.9.3

# Add metadata
LABEL maintainer="Pokemon Analytics Team"
LABEL description="Apache Airflow with Pokemon Analytics dependencies"
LABEL version="1.0"