# 🌤️ Airflow Weather Data Ingestion Pipeline

## 🚀 Overview
This project sets up an **Apache Airflow pipeline** using **Docker Compose** to automatically ingest and validate weather data from multiple East Coast cities via the **Open-Meteo API**.  

It’s designed as part of a modular data platform that can later feed data into **ClickHouse** and **dbt** for analytics.

---

## 📂 Project Structure
```
airflow/
├── dags/
│   ├── weather_ingestion_dag.py      # Main Airflow DAG
│   ├── utils/
│   │   ├── __init__.py
│   │   └── weather_api.py            # Weather ingestion script
│   └── data/                         # CSV outputs (saved here)
├── logs/                             # Airflow logs
├── requirements.txt                  # Python dependencies
├── docker-compose.yml                # Airflow setup
└── README.md                         # This file
```

---

## ⚙️ Prerequisites
Make sure you have:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Python 3.8+](https://www.python.org/downloads/) (optional, for local testing)

---

## 🧩 Setup Instructions


### 1️⃣ Initialize Airflow
This sets up the Airflow database, installs requirements, and creates the admin user.

```bash
docker compose up airflow-init
```

Wait for the message:
```
User admin created
Initialization complete
```

### 2️⃣ Start all Airflow services
```bash
docker compose up -d
```

This starts:
- **airflow-webserver** (UI)
- **airflow-scheduler**
- **airflow-db** (Postgres metadata DB)

Check running containers:
```bash
docker ps
```

---

## 🌐 Airflow Web Interface
Go to:  
👉 [http://localhost:8080](http://localhost:8080)

Login:
```
Username: admin
Password: admin
```

---

## 🧠 Running the Pipeline

1. In the Airflow UI, find your DAG:
   ```
   weather_data_ingestion
   ```
2. Toggle it **ON** (switch turns green).  
3. Click ▶️ to **trigger** it manually or wait for its daily schedule.

---

## 📊 Output
After the DAG runs, you’ll find:
```
./dags/data/east_coast_weather_all_cities.csv
```

Each record includes:
- weather_code  
- temperature  
- humidity  
- cloudcover  
- rain  
- sunshine duration  
- wind speed  
- postal code / prefix / city  

Cities included:
- New York  
- Philadelphia  
- Boston  
- Jacksonville  
- Miami  

---

## 🧪 Data Quality Checks
The DAG performs automatic validation:
- Checks for **missing (null)** values  
- Detects **duplicate** rows  

If problems are found, the task fails and retries automatically.

---

## 🧰 Useful Commands

**Restart everything**
```bash
docker compose down && docker compose up -d
```

**Re-initialize Airflow**
```bash
docker compose up airflow-init
```

**View logs**
```bash
docker compose logs -f airflow-webserver
```

**Stop all containers**
```bash
docker compose down
```

---

## 🧼 .gitignore Example
Add this to your `.gitignore` file:
```
# Airflow & Docker files
logs/
__pycache__/
*.pyc
*.pid
*.db

# Data outputs
dags/data/*.csv

# System files
.env
.DS_Store
```

---

## 🧭 Next Steps
Once ingestion is stable:
- Connect **ClickHouse** to load the data from `/dags/data/`
- Use **dbt** for transformations and analytics

---

✅ **You’re ready!**  
Run the DAG, check your data, and enjoy an automated weather ingestion workflow built with Airflow.
