# Docker Setup Guide

## Prerequisites

- **Docker Desktop** installed and running
- **Git** for cloning the repository
- **8GB+ RAM** recommended for all services

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/Xazen/pokemon_analytics.git
cd pokemon_analytics
```

### 2. Build Custom Docker Images
```bash
# Build all custom images with dependencies
./build-images.sh
```

### 3. Start All Services
```bash
# Start all containers in background
docker-compose up -d

# Check container status
docker-compose ps
```

### 4. Wait for Initialization
The first startup takes 2-3 minutes for:
- PostgreSQL database initialization
- Airflow metadata setup
- Container health checks

## Service Access

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8081 | admin/admin |
| **Jupyter** | http://localhost:8888 | Check logs for token |
| **Spark UI** | http://localhost:8080 | No auth |
| **pgAdmin** | http://localhost:5050 | admin@pokemon.com/admin |

## Running Data Pipeline

### Manual Data Collection
```bash
# Collect Pokemon data
docker-compose exec jupyter python /home/jovyan/src/collectors/pokemon_collector.py --all

# Scrape competitive data  
docker-compose exec jupyter python /home/jovyan/src/scrapers/showdown_scraper.py --all-formats

# Run ETL pipeline
docker-compose exec spark-master python /app/src/processors/pokemon_etl_pipeline.py

# Run advanced analytics
docker-compose exec jupyter python /home/jovyan/src/analytics/advanced_competitive_analytics.py
```

### Automated Workflows (via Airflow)
1. Go to http://localhost:8081
2. Login with admin/admin
3. Enable DAGs:
   - `pokemon_collection` - Daily Pokemon data updates
   - `competitive_pokemon_scraping` - Weekly competitive analysis

## Container Architecture

### Core Services (Always Running)
- **postgres**: PostgreSQL database
- **pgadmin**: Database management UI
- **redis**: Message broker for Airflow

### Processing Services
- **spark-master**: Spark cluster coordinator
- **spark-worker**: Spark task executor
- **jupyter**: Interactive development environment

### Orchestration Services  
- **airflow-webserver**: Web UI for workflow management
- **airflow-scheduler**: Task scheduling engine
- **airflow-worker**: Task execution engine
- **airflow-init**: One-time setup container

## Environment Detection

All scripts automatically detect if they're running in Docker and adjust:

### Database Connectivity
- **Docker**: Uses `postgres` hostname (container networking)
- **Local**: Uses `localhost` (direct connection)

### File Paths
- **Docker**: Uses mounted volumes (`/home/jovyan/*`, `/app/*`)
- **Local**: Uses relative paths from project root

### Custom Images Include
- **Spark**: PySpark + pandas + psycopg2 + beautifulsoup4
- **Jupyter**: All ML libraries (scikit-learn, plotly, seaborn)  
- **Airflow**: Web scraping and data processing dependencies

## Data Persistence

### Volume Mounts
```yaml
volumes:
  - ./notebooks:/home/jovyan/work     # Jupyter notebooks
  - ./src:/home/jovyan/src           # Source code
  - ./data:/home/jovyan/data         # Data files
  - ./airflow/dags:/opt/airflow/dags # Workflow definitions
```

### PostgreSQL Data
Stored in Docker volume `postgres_data` - persists across container restarts.

## Troubleshooting

### Container Won't Start
```bash
# Check container logs
docker-compose logs <service-name>

# Common issues:
docker-compose logs postgres  # Database initialization
docker-compose logs jupyter   # Python environment
```

### Database Connection Issues  
```bash
# Test database connectivity
docker-compose exec postgres psql -U pokemon_user -d pokemon_analytics -c "SELECT 1;"

# Reset database
docker-compose down -v
docker-compose up -d
```

### Memory Issues
```bash
# Increase Docker Desktop memory allocation to 8GB+
# Or run fewer services:
docker-compose up -d postgres jupyter spark-master
```

### Port Conflicts
If ports are in use, edit `docker-compose.yml`:
```yaml
ports:
  - "8082:8080"  # Change Spark UI port
  - "8889:8888"  # Change Jupyter port
```

## Development Workflow

### Making Changes
1. Edit files on host machine (auto-synced to containers)
2. Restart specific services if needed:
   ```bash
   docker-compose restart jupyter
   ```

### Adding Dependencies
1. Update appropriate Dockerfile in `docker/`
2. Rebuild images:
   ```bash
   ./build-images.sh
   docker-compose up -d --force-recreate
   ```

### Testing Scripts
```bash
# Test any script in appropriate container
docker-compose exec jupyter python /home/jovyan/src/path/to/script.py
docker-compose exec spark-master python /app/src/path/to/script.py
```

## Production Deployment

### Environment Variables
Create `.env` file for production secrets:
```bash
POSTGRES_PASSWORD=secure_password
AIRFLOW_FERNET_KEY=your_32_char_key
AIRFLOW_SECRET_KEY=your_secret_key
```

### Security Considerations
- Change default passwords
- Use secrets management
- Configure firewall rules
- Enable SSL/TLS

### Scaling
```bash
# Scale Spark workers
docker-compose up -d --scale spark-worker=3

# Scale Airflow workers  
docker-compose up -d --scale airflow-worker=2
```

## Cleanup

### Stop All Services
```bash
docker-compose down
```

### Remove All Data
```bash
docker-compose down -v  # WARNING: Deletes all data!
```

### Remove Custom Images
```bash
docker rmi pokemon-analytics/spark:latest
docker rmi pokemon-analytics/jupyter:latest  
docker rmi pokemon-analytics/airflow:latest
```