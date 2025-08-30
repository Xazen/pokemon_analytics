# Pokemon Competitive Analytics

A comprehensive data analytics project for analyzing competitive Pokemon teams and meta trends. This project demonstrates modern data engineering practices using PySpark, PostgreSQL, Docker, and PowerBI.

## 🎯 Project Overview

This portfolio project showcases the complete data pipeline from collection to visualization:
- **Data Collection**: Pokemon API + competitive usage scraping from Pokemon Showdown
- **Big Data Processing**: PySpark for ETL and transformations  
- **Workflow Orchestration**: Apache Airflow for automated data pipelines
- **Data Storage**: PostgreSQL data warehouse with dimensional modeling
- **Competitive Intelligence**: Real-time meta analysis and trend detection
- **Visualization**: PowerBI dashboards
- **Deployment**: Docker Compose for easy setup

## 🛠️ Tech Stack

- **Data Processing**: PySpark, Python, pandas, numpy
- **Database**: PostgreSQL with dimensional modeling
- **Workflow Orchestration**: Apache Airflow
- **Containerization**: Docker & Docker Compose (9 services)
- **Web Scraping**: BeautifulSoup, requests (Pokemon Showdown scraper)
- **Analytics**: scikit-learn, matplotlib, seaborn, plotly
- **Dashboard**: PowerBI
- **Version Control**: Git

## 📁 Project Structure

```
pokemon_analytics/
├── data/                  # Data storage (gitignored)
│   ├── raw/              # Raw API and scraped data
│   ├── processed/        # Cleaned and transformed data
│   └── warehouse/        # Final analytical datasets
├── src/                  # Source code
│   ├── collectors/       # API data collection scripts
│   ├── processors/       # PySpark ETL pipelines
│   ├── scrapers/         # Web scraping modules
│   └── utils/            # Helper functions and utilities
├── notebooks/            # Jupyter analysis notebooks
├── docker/              # Docker configurations
├── sql/                 # Database schemas and queries
├── dashboards/          # PowerBI files and exports
├── docs/                # Project documentation
└── PROJECT_PLAN.md      # Detailed implementation plan
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git
- PowerBI Desktop (for dashboard development)

### Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/pokemon-analytics.git
cd pokemon-analytics

# Start the services
docker-compose up -d

# Run data collection
python src/collectors/pokemon_collector.py

# Process data with PySpark
docker-compose exec spark-master python /app/src/processors/etl_pipeline.py
```

## 📊 Analytics Features

- **Team Composition Analysis**: Popular team archetypes and synergies
- **Usage Statistics**: Pokemon popularity across competitive formats
- **Meta Evolution**: Tracking competitive landscape changes over time
- **Type Coverage Analysis**: Team type distribution effectiveness
- **Move Analysis**: Popular moves and combinations
- **Tier Performance**: Analysis across different competitive tiers
- **Viability Scoring**: Algorithmic Pokemon competitiveness rating

## 🔄 Development Phases

This project is built in testable phases:

1. **Foundation**: Docker setup + PostgreSQL
2. **Data Collection**: PokeAPI integration  
3. **PySpark Integration**: Big data processing setup
4. **Web Scraping**: Competitive data collection
5. **ETL Pipeline**: Data cleaning and transformation
6. **Analytics**: Advanced insights generation
7. **Dashboard**: PowerBI visualization
8. **Documentation**: Production-ready deployment

## 📈 Expected Outcomes

- Automated end-to-end data pipeline
- Professional PowerBI dashboard
- Meaningful competitive insights
- Reusable analytics framework
- Portfolio-quality documentation

## 🤝 Contributing

This is a portfolio project, but suggestions and feedback are welcome! Please see `PROJECT_PLAN.md` for detailed implementation details.

## 📄 License

MIT License - see LICENSE file for details.

---
*Last Updated: August 2025*