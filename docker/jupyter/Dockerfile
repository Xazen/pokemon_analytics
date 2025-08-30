# Custom Jupyter image with Pokemon Analytics and ML dependencies
FROM jupyter/pyspark-notebook:latest

# Switch to root to install system dependencies
USER root

# Install system dependencies if needed
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Switch back to jovyan user for pip installs
USER jovyan

# Install Python dependencies for advanced analytics
RUN pip install --no-cache-dir \
    scikit-learn==1.3.1 \
    plotly==5.17.0 \
    seaborn==0.13.0 \
    psycopg2-binary==2.9.7 \
    sqlalchemy==2.0.23 \
    beautifulsoup4==4.12.2 \
    requests==2.31.0 \
    matplotlib==3.8.0 \
    numpy==1.24.4 \
    pandas==2.1.0

# Set working directory
WORKDIR /home/jovyan/work

# Add metadata
LABEL maintainer="Pokemon Analytics Team"
LABEL description="Jupyter PySpark with Pokemon Analytics ML dependencies"
LABEL version="1.0"