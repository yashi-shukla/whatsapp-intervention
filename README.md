# WhatsApp Healthcare Data Analytics Platform

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/your-repo/whatsapp-intervention)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-ready-blue)](https://www.docker.com/)

A robust ETL pipeline and analytics platform designed to process and analyze WhatsApp messaging data from Whatsapp intervention programs. The system provides comprehensive insights into user engagement, message delivery performance, and communication effectiveness.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Pipeline](#data-pipeline)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

## Overview

This platform processes WhatsApp messaging data to generate actionable insights for Whatsapp interventions. It handles complex message status tracking, performs data quality validation, and provides analytics on communication effectiveness.

The system processes messages and status updates daily, with comprehensive tracking of delivery states and user engagement metrics.

## Architecture


### Technology Stack

- **Extract**: Python with pandas, Google Sheets API
- **Transform**: Python ETL pipeline with Pydantic validation
- **Load**: Google BigQuery with automated schema management
- **Containerization**: Docker with multi-stage builds
- **Orchestration**: Makefile-based deployment scripts
- **Data Quality**: Automated validation and deduplication logic

## Key Features

### 📊 Data Processing
- **Real-time Message Tracking**: Comprehensive status monitoring (sent, delivered, read, failed, deleted)
- **Automated Deduplication**: Intelligent duplicate detection and resolution
- **Data Quality Validation**: Multi-layer validation with configurable rules

### 🔧 Technical Capabilities
- **Containerized Deployment**: Docker-based pipeline with optimized builds
- **Scalable Architecture**: Modular design supporting horizontal scaling
- **Error Handling**: Exception management and logging
- **Configuration Management**: Environment-based configuration system

## Prerequisites

### System Requirements
- **Python**: 3.11 or higher
- **Docker**: 20.10+ with Docker Compose

### Google Cloud Setup
1. **Google Cloud Project** with BigQuery enabled
2. **Service Account** with BigQuery Data Editor permissions
3. **Google Sheets API** enabled
4. **Service Account Key** (JSON format)

### Network Access
- Access to Google APIs
- Access to Google Sheets (public sharing or authenticated access)
- BigQuery API endpoints

## Installation

### Method 1: Docker Deployment (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd whatsapp-intervention/etl_pipeline

# Build the Docker image
make build

# Run the ETL pipeline
make run
```

### Method 2: Local Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd whatsapp-intervention/etl_pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python run_etl.py
```

### Method 3: Docker Compose

```bash
# Using docker-compose.yml
docker-compose up

# Or for detached mode
docker-compose up -d
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# Google Cloud Configuration
GOOGLE_APPLICATION_CREDENTIALS=/app/service-key.json
GOOGLE_CLOUD_PROJECT=your-project-id

# BigQuery Configuration
BQ_DATASET=whatsapp_healthcare
BQ_LOCATION=US

# Google Sheets Configuration
MESSAGES_SHEET_URL=your-google-sheet-url
MESSAGES_GID=tab-id
STATUSES_GID=tab-id

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=/app/logs/etl.log

# Output Configuration
OUTPUT_DIR=/app/output
SAVE_LOCAL=true
```

### Google Cloud Setup

1. **Create Service Account**:
   ```bash
   gcloud iam service-accounts create whatsapp-etl \
     --description="ETL pipeline service account" \
     --display-name="WhatsApp ETL Service"
   ```

2. **Grant Permissions**:
   ```bash
   gcloud projects add-iam-policy-binding your-project-id \
     --member="serviceAccount:whatsapp-etl@your-project-id.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataEditor"
   ```

3. **Generate Key**:
   ```bash
   gcloud iam service-accounts keys create service-key.json \
     --iam-account=whatsapp-etl@your-project-id.iam.gserviceaccount.com
   ```

## Usage

### Basic Pipeline Execution

```bash
# Docker deployment
make build && make run

# With custom environment
make run-with-env

# Interactive debugging
make run-interactive
```

### Advanced Usage

#### Custom Configuration
```bash
# Run with specific environment file
docker run --rm \
  --name whatsapp-etl-custom \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/custom.env:/app/.env \
  whatsapp-etl
```

#### Monitoring Pipeline Health
```bash
# Check container logs
docker logs whatsapp-etl-container

# Monitor resource usage
docker stats whatsapp-etl-container
```

### Output Files

The pipeline generates the following outputs:

```
output/
├── messages_raw.jsonl      # Raw message data
├── statuses_raw.jsonl      # Raw status data
├── unified_messages.jsonl  # Processed unified data
├── duplicates.jsonl        # Identified duplicates
└── quality_report.json     # Data quality metrics
```

## Data Pipeline

### Extract Phase
- **Source**: Google Sheets with multiple tabs (Messages, Statuses)
- **Authentication**: Service account or public access
- **Data Types**: Structured tabular data with timestamps

### Transform Phase
- **Data Cleaning**: Null value handling, type conversion
- **Validation**: Pydantic model validation with custom rules
- **Deduplication**: Content-based duplicate detection

### Load Phase
- **Destination**: Google BigQuery
- **Schema**: Automated schema management
- **Indexing**: Optimized for analytical queries

### Quality Assurance
- **Validation Rules**: Configurable data quality checks
- **Error Handling**: Exception management and logging
- **Monitoring**: Pipeline health and performance metrics

## Project Structure

```
whatsapp-intervention/
├── etl_pipeline/                    # Main ETL pipeline
│   ├── adapters/                   # Data adapters and connectors
│   │   ├── extract_data.py        # Google Sheets extraction
│   │   └── upload_to_bigquery.py  # BigQuery loading
│   ├── transform/                 # Data transformation logic
│   │   ├── transform_data.py      # Core transformation
│   │   └── sql_transform.py       # SQL transformations
│   ├── models.py                  # Pydantic data models
│   ├── run_etl.py                 # Main orchestration script
│   ├── requirements.txt           # Python dependencies
│   ├── Dockerfile                 # Container definition
│   ├── docker-compose.yml         # Multi-container setup
│   └── Makefile                   # Build and deployment scripts
└── README.md                     # This documentation
```

## Troubleshooting

### Common Issues

#### Docker Build Failures
```bash
# Clear Docker cache
docker system prune -f

# Rebuild without cache
docker build --no-cache -t whatsapp-etl .
```

#### BigQuery Connection Issues
```bash
# Verify service account key
gcloud auth activate-service-account --key-file=service-key.json

# Test BigQuery access
bq ls your-project-id:whatsapp_healthcare
```

#### Google Sheets Access Issues
```bash
# Verify sheet permissions (must be publicly accessible or authenticated)
curl -s "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/export?format=csv&gid=YOUR_GID"
```

### Performance Optimization

#### Memory Usage
- Monitor container memory with `docker stats`
- Adjust Docker memory limits if needed
- Consider data chunking for large datasets

#### Network Performance
- Use regional BigQuery locations close to your deployment
- Implement retry logic for transient failures
- Monitor API quota usage

### Logging and Debugging

#### Enable Debug Logging
```bash
export LOG_LEVEL=DEBUG
make run
```

#### View Detailed Logs
```bash
# Container logs
docker logs whatsapp-etl-container

# Application logs
tail -f /app/logs/etl.log
```







