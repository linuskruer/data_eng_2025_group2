# SQL Pipeline Testing Guide

## Overview
This guide provides comprehensive instructions for testing the weather x eBay analytics SQL pipeline. The testing framework includes automated setup, validation, and reporting.

## Quick Start

### 1. Prerequisites
- PostgreSQL 12+ installed and running
- Python 3.8+ with pip
- Access to your data files (CSV format)

### 2. Quick Test (No Database Required)
```bash
# Test SQL files and dependencies without database
python quick_test.py
```

### 3. Environment Setup (For Full Testing)
```bash
# Copy environment template
cp env_example.txt .env

# Edit .env with your database credentials
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=weather_ebay_analytics
# DB_USER=postgres
# DB_PASSWORD=your_password

# Install test dependencies
pip install -r requirements_test.txt
```

### 4. Install PostgreSQL (Optional - for full testing)
1. Download from: https://www.postgresql.org/download/
2. Install with default settings
3. Add PostgreSQL bin to PATH
4. Restart command prompt

### 5. Run Complete Test Suite
```bash
python test_runner.py
```

This will:
- Check prerequisites
- Set up the database schema
- Load sample data
- Run all SQL queries
- Generate comprehensive reports

## Manual Testing Steps

### Step 1: Database Setup
```bash
python setup_database.py
```

### Step 2: Run SQL Tests
```bash
python test_sql_pipeline.py
```

### Step 3: Individual Query Testing
```bash
# Test specific queries
psql -d weather_ebay_analytics -f "sql_queries/impact_listings_by_weather.sql"
psql -d weather_ebay_analytics -f "sql_queries/pricing_behavior_by_weather_and_product.sql"
```

## Test Components

### 1. Database Setup (`setup_database.py`)
- Creates star schema tables
- Loads dimension data
- Imports weather and eBay data from CSV files
- Creates performance indexes

### 2. SQL Pipeline Tester (`test_sql_pipeline.py`)
- Validates database schema
- Checks data quality
- Tests all 7 SQL queries
- Measures performance
- Generates detailed results

### 3. Test Runner (`test_runner.py`)
- Orchestrates complete test suite
- Checks prerequisites
- Runs all components
- Generates comprehensive reports

## Expected Results

### Schema Validation
- ✅ All 10 required tables exist
- ✅ Proper foreign key relationships
- ✅ Performance indexes created

### Data Quality Checks
- ✅ Minimum 100 fact_listings records
- ✅ Minimum 10 weather records
- ✅ Successful joins between fact and weather tables
- ✅ Valid date ranges

### Query Results
Each query should return:
1. **Impact Listings by Weather**: Daily listing counts by weather bucket
2. **Pricing Behavior**: Average prices by product type and weather
3. **Shipping Choices**: Free shipping rates by weather
4. **Category Demand Shifts**: Category demand by weather
5. **Seller Performance**: Seller activity during bad weather
6. **Listing Quality**: Title length and buying options by weather
7. **Zip Prefix Variation**: Price variability by zip code and weather

## Troubleshooting

### Common Issues

#### 1. psycopg2 Installation Error
```
Error: Failed building wheel for psycopg2
```
**Solution**: Install the binary version
```bash
pip install psycopg2-binary
```

#### 2. PostgreSQL Not Found
```
Error: PostgreSQL not installed or not in PATH
```
**Solution**: Install PostgreSQL or use quick test
```bash
# Quick test without database
python quick_test.py
```

#### 3. Missing CSV Files
```
Warning: No weather CSV files found
```
**Solution**: Ensure CSV files exist in `weather_data/` and `ebay_data/` directories

#### 4. Schema Validation Failed
```
Error: Table fact_listings missing
```
**Solution**: Run `setup_database.py` first to create schema

#### 5. Query Execution Failed
```
Error: Column w.date does not exist
```
**Solution**: Check your weather table has correct column names matching the data dictionary

### Performance Issues

#### Slow Query Execution
- Add indexes on join columns
- Check data volume (queries may be slow with millions of rows)
- Consider partitioning large tables

#### Memory Issues
- Increase PostgreSQL memory settings
- Process data in batches
- Use LIMIT clauses for testing

## Test Reports

### Generated Files
- `test_results.log`: Detailed execution log
- `test_results.json`: Machine-readable results
- `test_report.md`: Human-readable summary

### Report Contents
- Schema validation status
- Data quality metrics
- Query execution results
- Performance measurements
- Error details and recommendations

## Customization

### Adding New Queries
1. Create SQL file in `sql_queries/`
2. Add to `query_tests` list in `test_sql_pipeline.py`
3. Update documentation

### Modifying Data Sources
1. Update CSV file paths in `setup_database.py`
2. Adjust column mappings as needed
3. Validate data format matches expectations

### Changing Database
1. Update connection strings in test files
2. Modify SQL syntax for your database
3. Adjust schema creation scripts

## Best Practices

### Before Testing
- Backup existing database
- Verify data file integrity
- Check available disk space
- Ensure sufficient memory

### During Testing
- Monitor system resources
- Check logs for warnings
- Validate sample results manually
- Document any issues

### After Testing
- Review all generated reports
- Archive test results
- Update documentation
- Plan performance optimizations

## Support

### Getting Help
1. Check generated logs for detailed error messages
2. Verify prerequisites are met
3. Review data file formats
4. Test database connectivity manually

### Reporting Issues
Include:
- Error messages from logs
- Database version and configuration
- Data file sample (first few rows)
- System specifications

## Advanced Testing

### Load Testing
```bash
# Test with larger datasets
python test_sql_pipeline.py --large-dataset

# Performance benchmarking
python test_sql_pipeline.py --benchmark
```

### Continuous Integration
```bash
# Automated testing in CI/CD
python test_runner.py --ci-mode
```

### Custom Validation
```python
# Add custom checks
def custom_validation():
    # Your validation logic here
    pass
```
