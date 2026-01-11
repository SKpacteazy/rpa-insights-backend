# UiPath ETL & Dashboard Backend

This project is a comprehensive backend solution that extracts operational data from UiPath Orchestrator, transforms it, and loads it into a MySQL database. It also provides a Flask-based API to serve this data to the frontend dashboard.

## Overview

The system consists of three main components:
1.  **ETL Pipeline (Airflow)**: Orchestrates scheduled jobs to fetch `Queue Items` and `Jobs` from UiPath.
2.  **Database (MySQL)**: Stores the historical and operational data.
3.  **API Service (Flask)**: Exposes endpoints for the UI to consume metrics (e.g., Queue Volume, SLA compliance).

## Architecture

-   **uipath_etl/**: Core logic for interacting with UiPath and the Database.
    -   `client.py`: Handles UiPath API authentication and data fetching.
    -   `modules/`: specialized modules for different dashboard features (Operational, SLA, Jobs).
-   **dags/**: Airflow DAG definitions that schedule the ETL tasks.
-   **docker-compose.yaml**: Orchestrates the Airflow and Service containers.

## Prerequisites

-   **Docker Desktop**: Must be installed and running.
-   **UiPath Orchestrator Access**: Credentials configured in the environment variables.

## Getting Started

### 1. Start the Services
Run the following command in the project root to start Airflow, the API, and the Database:

```bash
docker-compose up -d
```

### 2. Access the Interfaces

-   **Airflow UI**: [http://localhost:8080](http://localhost:8080)
    -   **User/Pass**: `airflow` / `airflow`
    -   Use this to monitor and trigger ETL jobs manually.
    
-   **Backend API**: [http://localhost:5000/api/docs](http://localhost:5000/api/docs)
    -   Swagger UI availability depends on configuration, but APIs are served at port 5000.

### 3. Key DAGs

-   `uipath_etl_pipeline`: The main DAG that runs the extraction logic defined in `uipath_etl/client.py`.

## Data Flow

1.  **Extract**: `UiPathClient` authenticates with Orchestrator and fetches data.
2.  **Load**: Data is upserted into `queue_items` and other tables in the MySQL database.
3.  **Serve**: The React Frontend requests data via the Flask API, which queries the MySQL database.
