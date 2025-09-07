# 🚔 Phoenix Crime Data Dashboard

A full-stack data pipeline and interactive dashboard for analyzing Phoenix crime data.

## Overview

This project consists of three main components:

- **Data Updater**: Pulls incremental crime records from the City of Phoenix Open Data Portal via the CKAN API and stores them in a PostgreSQL database.
- **Flask API**: Provides REST endpoints to query and serve crime data to clients.
- **Streamlit Dashboard**: Interactive visualization dashboard for real-time analysis.

### 📌 Key Features

- Incremental data ingestion from Phoenix Open Data (no need to download huge CSV files).
- PostgreSQL database backend for scalable storage.
- Flask REST API for crime stats, filtering, and update monitoring.
- Streamlit dashboard with:
  - 📊 Top crime types
  - 📈 Crime trends by year/month
  - 🏘️ Crimes by ZIP code
  - 🕐 Recent incidents
  - 🔄 Live filters (year, crime type, ZIP code)

## 🏗️ System Architecture

Phoenix CKAN API → Updater → PostgreSQL DB → Flask API → Streamlit Dashboard

- `phoenix_api_updater.py`: Scheduled updater script (fetch + clean + insert data).
- `main.py` (Flask API): Exposes REST endpoints for crime data.
- `dashboard.py` (Streamlit): Frontend dashboard for visualization.

### ⚙️ Installation & Setup

#### 1. Clone Repository
```bash
git clone https://github.com/yourusername/phoenix-crime-dashboard.git
cd phoenix-crime-dashboard
```

#### 2. Create & Activate Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows
```

#### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

#### 4. Setup PostgreSQL

Create database:
```sql
CREATE DATABASE phoenix_crime_db;
```

Set environment variable for DB password:
```bash
export DB_PASSWORD="yourpassword"   # Mac/Linux
set DB_PASSWORD=yourpassword        # Windows
```

#### 5. Run Updater

Fetch and insert latest crime data:
```bash
python phoenix_api_updater.py
```

Start the scheduler:
```bash
python phoenix_api_updater.py  # then choose option 3
```

#### 6. Run Flask API
```bash
python main.py
```

By default, API runs on `http://localhost:5000`.

#### 7. Run Streamlit Dashboard
```bash
streamlit run dashboard.py
```

Dashboard will be available at `http://localhost:8501`.

### 🔌 API Endpoints

- `/api/health`: Health check
- `/api/stats`: Overall crime stats
- `/api/crimes`: Paginated crimes with filters (crime_type, year, zip_code)
- `/api/crime-types`: List of all crime types
- `/api/crimes/by-zip`: Crimes aggregated by ZIP
- `/api/crimes/timeline`: Monthly crime trends
- `/api/crimes/recent`: Most recent crimes
- `/api/update-status`: Info on last update
- `/api/trigger-update`: Manually trigger update
- `/api/data-freshness`: Data recency/freshness check

### 📊 Dashboard Preview

- **Top 10 Crime Types**
- **Crimes by Year Trend**
- **Crimes by ZIP Code**
- **Monthly Timeline**
- **Recent Crimes Table with Filters**

### 📝 Logging

- Updater logs stored in `phoenix_api_updater.log`.
- Update status saved in `api_update_status.json`.

## 🚀 Future Enhancements

- Dockerize for easy deployment
- Add map-based visualizations (crime hotspots)
- Authentication for sensitive API endpoints
- Advanced filtering (premise type, time of day, etc.)

## 📄 License

MIT License.
