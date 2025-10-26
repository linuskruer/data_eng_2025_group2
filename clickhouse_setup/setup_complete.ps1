# Complete Setup Script for ClickHouse Medallion Architecture
# This script sets up the entire data pipeline from Airflow to ClickHouse with dbt

Write-Host "🚀 Starting ClickHouse Medallion Architecture Setup" -ForegroundColor Green
Write-Host "=" * 60 -ForegroundColor Green

function Test-Command {
    param($Command, $Description)
    Write-Host "🔄 $Description..." -ForegroundColor Yellow
    try {
        Invoke-Expression $Command | Out-Null
        Write-Host "✅ $Description completed successfully" -ForegroundColor Green
        return $true
    }
    catch {
        Write-Host "❌ $Description failed: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Check-Prerequisites {
    Write-Host "`n🔍 Checking prerequisites..." -ForegroundColor Cyan
    
    if (-not (Test-Command "docker --version" "Checking Docker")) {
        Write-Host "❌ Docker is not installed. Please install Docker first." -ForegroundColor Red
        return $false
    }
    
    if (-not (Test-Command "python --version" "Checking Python")) {
        Write-Host "❌ Python is not installed. Please install Python first." -ForegroundColor Red
        return $false
    }
    
    Write-Host "✅ All prerequisites are met" -ForegroundColor Green
    return $true
}

function Setup-ClickHouse {
    Write-Host "`n🏗️ Setting up ClickHouse..." -ForegroundColor Cyan
    
    if (-not (Test-Command "docker-compose -f docker-compose-clickhouse-simple.yaml up -d" "Starting ClickHouse")) {
        return $false
    }
    
    Write-Host "⏳ Waiting for ClickHouse to be ready..." -ForegroundColor Yellow
    Start-Sleep -Seconds 20
    
    if (-not (Test-Command 'docker exec clickhouse-server clickhouse-client --query "CREATE DATABASE IF NOT EXISTS bronze; CREATE DATABASE IF NOT EXISTS silver; CREATE DATABASE IF NOT EXISTS gold;"' "Creating databases")) {
        return $false
    }
    
    if (-not (Test-Command "Get-Content sql\bronze_tables_updated.sql | docker exec -i clickhouse-server clickhouse-client --multiquery" "Creating Bronze layer tables")) {
        return $false
    }
    
    Write-Host "✅ ClickHouse setup completed" -ForegroundColor Green
    return $true
}

function Install-Dependencies {
    Write-Host "`n📚 Installing dependencies..." -ForegroundColor Cyan
    
    $dependencies = @("clickhouse-connect", "pandas", "dbt-clickhouse")
    
    foreach ($dep in $dependencies) {
        if (-not (Test-Command "pip install $dep" "Installing $dep")) {
            Write-Host "⚠️ Warning: Failed to install $dep" -ForegroundColor Yellow
        }
    }
    
    Write-Host "✅ Dependencies installation completed" -ForegroundColor Green
    return $true
}

function Load-Data {
    Write-Host "`n📊 Loading data into ClickHouse..." -ForegroundColor Cyan
    
    if (-not (Test-Command "python load_data_final.py" "Loading data")) {
        return $false
    }
    
    Write-Host "✅ Data loading completed" -ForegroundColor Green
    return $true
}

function Setup-dbt {
    Write-Host "`n🔧 Setting up dbt..." -ForegroundColor Cyan
    
    if (-not (Test-Command "dbt --version" "Checking dbt installation")) {
        Write-Host "❌ dbt is not installed. Please install dbt-clickhouse first." -ForegroundColor Red
        return $false
    }
    
    if (-not (Test-Path "dbt")) {
        Write-Host "📁 dbt directory not found. Please ensure dbt models are in place." -ForegroundColor Yellow
        return $false
    }
    
    Write-Host "✅ dbt setup completed" -ForegroundColor Green
    return $true
}

function Verify-Setup {
    Write-Host "`n🔍 Verifying setup..." -ForegroundColor Cyan
    
    if (-not (Test-Command 'docker exec clickhouse-server clickhouse-client --query "SELECT version()"' "Testing ClickHouse connection")) {
        return $false
    }
    
    if (-not (Test-Command 'docker exec clickhouse-server clickhouse-client --query "SELECT COUNT(*) FROM bronze.ebay_raw_data"' "Checking eBay data")) {
        return $false
    }
    
    if (-not (Test-Command 'docker exec clickhouse-server clickhouse-client --query "SELECT COUNT(*) FROM bronze.weather_raw_data"' "Checking weather data")) {
        return $false
    }
    
    Write-Host "✅ Setup verification completed" -ForegroundColor Green
    return $true
}

function Show-NextSteps {
    Write-Host "`n🎉 Setup completed successfully!" -ForegroundColor Green
    Write-Host "`n📋 Next Steps:" -ForegroundColor Cyan
    Write-Host "1. 🌐 Access ClickHouse Web UI: http://localhost:8123" -ForegroundColor White
    Write-Host "2. 🔧 Use ClickHouse client: docker exec -it clickhouse-server clickhouse-client" -ForegroundColor White
    Write-Host "3. 📊 Run analytical queries: Get-Content sql\analytical_queries.sql | docker exec -i clickhouse-server clickhouse-client --multiquery" -ForegroundColor White
    Write-Host "4. 🔄 Set up dbt models: cd dbt && dbt run" -ForegroundColor White
    Write-Host "5. 📈 Monitor data quality: Check bronze.data_quality_logs table" -ForegroundColor White
    
    Write-Host "`n📚 Documentation:" -ForegroundColor Cyan
    Write-Host "- Main README: README.md" -ForegroundColor White
    Write-Host "- Implementation Summary: IMPLEMENTATION_SUMMARY.md" -ForegroundColor White
    Write-Host "- dbt Quick Start: dbt/DBT_QUICK_START.md" -ForegroundColor White
    
    Write-Host "`n🔗 Integration with Existing Code:" -ForegroundColor Cyan
    Write-Host "- Airflow DAGs: ../ebay_api_scripts/dags/" -ForegroundColor White
    Write-Host "- Data Loading: airflow_dags/clickhouse_data_loader.py" -ForegroundColor White
    Write-Host "- Bronze Layer: sql/bronze_tables_updated.sql" -ForegroundColor White
}

# Main execution
$steps = @(
    @{Name="Prerequisites Check"; Function={Check-Prerequisites}},
    @{Name="ClickHouse Setup"; Function={Setup-ClickHouse}},
    @{Name="Dependencies Installation"; Function={Install-Dependencies}},
    @{Name="Data Loading"; Function={Load-Data}},
    @{Name="dbt Setup"; Function={Setup-dbt}},
    @{Name="Setup Verification"; Function={Verify-Setup}}
)

foreach ($step in $steps) {
    Write-Host "`n📋 $($step.Name)" -ForegroundColor Cyan
    Write-Host "-" * 40 -ForegroundColor Gray
    
    if (-not (& $step.Function)) {
        Write-Host "`n❌ Setup failed at step: $($step.Name)" -ForegroundColor Red
        Write-Host "Please check the error messages above and try again." -ForegroundColor Red
        exit 1
    }
}

Show-NextSteps
Write-Host "`n✅ Setup completed successfully!" -ForegroundColor Green
