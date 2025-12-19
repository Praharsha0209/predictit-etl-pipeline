# PredictIt Market Data Pipeline

A production-grade ETL pipeline that extracts political prediction market data from PredictIt API, stores it in AWS S3, loads it into Snowflake data warehouse, and visualizes insights in Tableau dashboards.

![Pipeline Architecture](docs/architecture_diagram.png)

## Features

- **Automated Data Extraction**: Pulls data from PredictIt API every 6 hours
- **Cloud Storage**: Date-partitioned storage in AWS S3
- **Data Warehouse**: Snowflake with RAW_DATA and ANALYTICS schemas
- **Orchestration**: Apache Airflow with Docker for scheduling
- **Data Transformations**: SQL-based analytics table population
- **Quality Checks**: Automated data validation
- **Visualizations**: Interactive Tableau dashboards

## Dashboard Highlights

- **187 active markets** tracked
- **627 live contracts** with real-time pricing
- **6 interactive visualizations** including:
  - Market overview and rankings
  - Price distribution analysis
  - Trading opportunity identification
  - Market volatility metrics
  - Contract status breakdown
  - Time-series analysis

## Architecture
```
PredictIt API → Python Extractor → AWS S3 → Snowflake → Tableau
                     ↑                                       ↑
                 Airflow Scheduler                    Analytics Layer
```

### Technology Stack

- **Python 3.9+**: Data extraction and loading
- **Apache Airflow 2.x**: Workflow orchestration
- **AWS S3**: Cloud data lake
- **Snowflake**: Cloud data warehouse
- **Tableau**: Business intelligence and visualization
- **Docker**: Containerization
- **PostgreSQL**: Airflow metadata database

## Project Structure
```
├── dags/                   # Airflow DAG definitions
├── src/                    # Source code modules
│   ├── extractors/        # API data extraction
│   ├── loaders/           # S3 and Snowflake loaders
│   └── utils/             # Configuration and helpers
├── sql/                    # SQL scripts
├── tableau/                # Tableau workbooks
└── docs/                   # Documentation and screenshots
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- AWS Account with S3 access
- Snowflake Account
- Tableau Desktop (for dashboards)
- Python 3.9+

### Installation

1. **Clone the repository**
```bash
   git clone https://github.com/yourusername/predictit-etl-pipeline.git
   cd predictit-etl-pipeline
```

2. **Set up environment variables**
```bash
   cp .env.example .env
   # Edit .env with your credentials
```

3. **Start Airflow with Docker**
```bash
   docker-compose up -d
```

4. **Access Airflow UI**
```
   URL: http://localhost:8080
   Username: airflow
   Password: airflow
```

5. **Enable the DAG**
   - Navigate to Airflow UI
   - Toggle the `predictit_etl_pipeline` DAG to ON
   - Pipeline will run every 6 hours automatically

### Configuration

Edit `.env` file with your credentials:
```bash
# AWS Credentials
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name

# Snowflake Credentials
SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ETL_DB
SNOWFLAKE_SCHEMA=RAW_DATA
SNOWFLAKE_ROLE=ACCOUNTADMIN

# PredictIt API
API_BASE_URL=https://www.predictit.org/api/marketdata/all/
```

## 📊 Data Schema

### RAW_DATA.PREDICTIT_RAW

| Column | Type | Description |
|--------|------|-------------|
| MARKET_ID | INTEGER | Unique market identifier |
| MARKET_NAME | VARCHAR | Full market name |
| SHORT_NAME | VARCHAR | Abbreviated market name |
| MARKET_URL | VARCHAR | Market URL |
| MARKET_STATUS | VARCHAR | Market status (Open/Closed) |
| CONTRACT_DATA | VARIANT | JSON array of contracts |
| EXTRACTION_TIMESTAMP | TIMESTAMP | Data extraction time |
| LOADED_AT | TIMESTAMP | Data load time |

### ANALYTICS Schema

1. **MARKET_SUMMARY**: Aggregated market metrics
2. **CONTRACT_DETAILS**: Individual contract data with pricing
3. **DAILY_MARKET_METRICS**: Daily performance statistics

## 🔄 Pipeline Flow

1. **Extract**: Python script fetches data from PredictIt API
2. **Store**: JSON files saved to local temp directory
3. **Upload**: Files uploaded to S3 with date partitioning (year/month/day)
4. **Stage**: Snowflake external stage created pointing to S3
5. **Load**: Raw JSON loaded and parsed into PREDICTIT_RAW table
6. **Transform**: SQL transformations populate analytics tables
7. **Validate**: Data quality checks ensure completeness

## 📈 Analytics Queries

Sample queries available in `sql/analysis_queries.sql`:

- Top markets by contract volume
- Price distribution analysis
- Trading opportunity identification
- Market volatility calculations
- Contract status breakdown
- Time-series trends

## 🎨 Tableau Dashboards

Connect Tableau to Snowflake:
- Server: `your-account.snowflakecomputing.com`
- Database: `ETL_DB`
- Schema: `ANALYTICS`

Available visualizations:
1. Market Overview (Top Markets by Volume)
2. Price Distribution (Histogram)
3. Contract Status Breakdown (Pie Chart)
4. Trading Opportunities (Spread Analysis)
5. Market Volatility (Bar Chart)
6. Time-Series Activity (Line Chart)

## 🧪 Testing
```bash
# Run unit tests
python -m pytest tests/

# Test API extraction
python src/extractors/predictit_extractor.py

# Validate SQL
snowsql -f sql/create_tables.sql --dry-run
```

## 📅 Schedule

Pipeline runs automatically every 6 hours:
- 00:00 UTC
- 06:00 UTC
- 12:00 UTC
- 18:00 UTC

## 🐛 Troubleshooting

### Airflow DAG not appearing
```bash
docker-compose restart
docker-compose logs airflow-scheduler
```

### Snowflake connection issues
- Verify credentials in `.env`
- Check network access rules in Snowflake
- Ensure warehouse is running

### S3 upload failures
- Verify AWS credentials
- Check S3 bucket permissions
- Ensure bucket exists

## 📚 Documentation

- [Setup Guide](docs/setup_guide.md)
- [Architecture Diagram](docs/architecture_diagram.png)
- [SQL Schema](sql/create_tables.sql)
- [API Documentation](https://www.predictit.org/api/)

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## 👤 Author

**Your Name**
- GitHub: [@praharsha0209](https://github.com/Praharsha0209)
- LinkedIn: [praharsha prateek](https://www.linkedin.com/in/praharsha-prateek-784186190/)
- Email: your.email@example.com

## 🙏 Acknowledgments

- PredictIt API for providing market data
- Apache Airflow community
- Snowflake documentation
- Tableau community resources

## 📊 Project Stats

- **Lines of Code**: ~2,500
- **Data Volume**: 187 markets, 627 contracts
- **Update Frequency**: Every 6 hours
- **Technologies**: 7 (Python, Airflow, S3, Snowflake, Docker, SQL, Tableau)
- **Completion Time**: [Your timeframe]

---

⭐ **Star this repository** if you found it helpful!

📧 **Questions?** Open an issue or reach out directly.