#!/bin/bash
# Build script for Pokemon Analytics Docker images
# This script builds all custom Docker images with proper tags

set -e  # Exit on any error

echo "🚀 Building Pokemon Analytics Docker Images"
echo "==========================================="

# Build Spark image
echo "📊 Building Spark image..."
docker build -f docker/spark/Dockerfile -t pokemon-analytics/spark:latest .
echo "✅ Spark image built successfully"

# Build Jupyter image  
echo "📓 Building Jupyter image..."
docker build -f docker/jupyter/Dockerfile -t pokemon-analytics/jupyter:latest .
echo "✅ Jupyter image built successfully"

# Build Airflow image
echo "🌀 Building Airflow image..."
docker build -f docker/airflow/Dockerfile -t pokemon-analytics/airflow:latest .
echo "✅ Airflow image built successfully"

echo ""
echo "🎉 All Docker images built successfully!"
echo ""
echo "📋 Built images:"
docker images | grep pokemon-analytics
echo ""
echo "🚀 You can now run: docker-compose up -d"