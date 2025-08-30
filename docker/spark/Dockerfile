# Custom Spark image with Pokemon Analytics dependencies
FROM bitnami/spark:3.5.0

# Switch to root to install packages
USER root

# Install Python dependencies for Pokemon Analytics
RUN pip install --no-cache-dir \
    py4j==0.10.9.7 \
    beautifulsoup4==4.12.2 \
    pandas==2.1.0 \
    psycopg2-binary==2.9.7 \
    requests==2.31.0

# Switch back to default user
USER 1001

# Set working directory
WORKDIR /app

# Add metadata
LABEL maintainer="Pokemon Analytics Team"
LABEL description="Apache Spark with Pokemon Analytics dependencies"
LABEL version="1.0"