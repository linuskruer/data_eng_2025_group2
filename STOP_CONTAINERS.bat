@echo off
echo ============================================
echo Stopping Weather & eBay Data Pipeline
echo ============================================
echo.

cd project\airflow

echo Stopping all containers...
docker compose down

echo.
echo Containers stopped!
echo.
echo To remove all data (fresh start): docker compose down -v
echo.

pause





