# Simple ETL Pipeline for WhatsApp Healthcare Analytics

A lightweight, Python-based ETL pipeline that's **much simpler than Meltano** for CSV-to-database workflows.

## 🚀 Quick Start

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the complete ETL pipeline
python run_etl.py
```

### Docker Deployment
```bash
# Build the Docker image
make build

# Run the ETL pipeline in a container
make run

# Or use docker-compose
docker-compose up
```

### Available Docker Commands
```bash
make build           # Build the Docker image
make run             # Run the ETL pipeline
make run-with-env    # Run with custom environment file
make run-interactive # Run container with bash for debugging
make stop            # Stop running container
make clean           # Remove Docker image
make help            # Show all available commands
```

## 📁 Project Structure

```
whatsapp-intervention/
├── data/
│   ├── raw/              # Source data files (fallback)
│   │   ├── messages.csv  # WhatsApp messages (fallback)
│   │   └── statuses.csv  # Message status updates (fallback)
│   └── transformed/      # ETL pipeline outputs
├── etl_pipeline/
│   ├── adapters/         # Service layer components
│   │   ├── extract_data.py       # Google Sheets data extraction
│   │   └── upload_to_bigquery.py # BigQuery data loading
│   ├── extract/          # Legacy extraction (deprecated)
│   ├── transform/        # Data transformation logic
│   ├── run_etl.py        # Main orchestration script
│   ├── models.py         # Pydantic data models
│   ├── requirements.txt  # Dependencies
│   └── README.md         # This file
```

## 🔧 Configuration

### Environment Variables (.env file)

The pipeline uses environment variables for configuration. Create a `.env` file with these values:

```bash
# GCP Configuration
GCP_PROJECT_ID=your-project-id
BIGQUERY_DATASET=whatsapp_healthcare
GCP_SERVICE_KEY_PATH=../service-key.json
BIGQUERY_LOCATION=asia-south2

# Google Sheets Configuration (Simplified)
GOOGLE_SHEET_URL=https://docs.google.com/spreadsheets/d/1XC0YaSQ4WjLwhzCB96RxF23-NjFga1Fisr-9lX_7hmk/export?format=csv

# Data Paths (fallback for local files)
DATA_RAW_DIR=data/raw
DATA_TRANSFORMED_DIR=data/transformed

# Pipeline Settings
LOG_LEVEL=INFO
ENABLE_BIGQUERY_UPLOAD=true
DUPLICATE_TIME_WINDOW_MINUTES=1
```

### Configuration Files

- **`.env`** - Main configuration file with all settings
- **`service-key.json`** - Google Cloud service account credentials
- **`extract/extract_data.py`** - Reads from **Google Sheets** using `GOOGLE_SHEET_URL` (simplified pandas approach)
- **`adapters/upload_to_bigquery.py`** - Uses GCP environment variables

## ✅ What This Does

1. **Extract**: Reads data from **Google Sheets** (publicly accessible)
2. **Transform**: Combines messages + statuses, detects duplicates, validates data with Pydantic
3. **Load**: Saves to BigQuery or local JSONL files in `../data/transformed/`

## 📊 Results

- `unified_messages.jsonl` - Combined messages with latest status (JSON Lines format)
- `duplicates.jsonl` - Potential duplicate records (JSON Lines format)
- `quality_report.json` - Comprehensive data quality analysis

## 🛡️ Type Safety & Validation

Built with **Pydantic** for enterprise-grade data validation:

- **Field Validators**: Automatic type checking and conversion
- **Enum Validation**: Strict validation of categorical data
- **Data Quality Reports**: Automated quality scoring and issue detection
- **Runtime Safety**: Prevents invalid data from propagating through the pipeline
- **Self-Documenting**: Models serve as living documentation of data structure

## 🎯 Why This Instead of Meltano

| Aspect | Meltano | This Pipeline |
|--------|---------|---------------|
| **Setup** | Complex YAML configs | Simple Python scripts |
| **Dependencies** | 100+ packages | 6 core packages |
| **Learning Curve** | Singer spec + Docker | Basic Python only |
| **Type Safety** | Limited | Full Pydantic validation |
| **Flexibility** | Pre-built plugins | Complete custom control |
| **Maintenance** | Plugin conflicts | Direct dependency management |
| **Performance** | Container overhead | Native Python speed |

## 🏃‍♂️ Run It

```bash
cd etl_pipeline
python run_etl.py
```

That's it! No Docker, no complex configurations, no Singer specs. Just pure Python doing exactly what you need.
