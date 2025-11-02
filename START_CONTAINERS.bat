@echo off
echo ============================================
echo Starting Weather & eBay Data Pipeline
echo ============================================
echo.

cd project\airflow

echo Step 1: Stopping existing containers...
docker compose down

echo.
echo Step 2: Initializing Airflow (if needed)...
docker compose up airflow-init

echo.
echo Step 3: Starting all services...
docker compose up -d

echo.
echo ============================================
echo Containers started!
echo ============================================
echo.
echo Airflow UI: http://localhost:8080
echo   Username: admin
echo   Password: admin
echo.
echo ClickHouse: http://localhost:8123
echo.
echo Checking container status...
docker ps

echo.
echo To view logs: docker compose logs -f
echo To stop: docker compose stop
echo.

pause






