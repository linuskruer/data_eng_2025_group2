# SQL Analysis Tools

This folder contains all the tools and scripts for analyzing the weather-eBay data using SQL queries.

## 📁 **Contents**

### **Core Analysis Scripts**
- `run_sql_queries.py` - Main script to run individual or all SQL queries
- `data_analysis.py` - Comprehensive data structure analysis
- `validate_real_data_queries.py` - Validates SQL queries against real API data

### **Testing Framework**
- `test_runner.py` - Orchestrates the complete test suite
- `test_sql_pipeline.py` - Main test suite for schema validation and query execution
- `setup_database.py` - Creates star schema and loads sample data
- `quick_test.py` - Quick testing without database connection

### **Documentation & Reports**
- `SQL_QUERY_ANALYSIS_SUMMARY.md` - Complete analysis summary with business insights
- `TESTING_GUIDE.md` - Comprehensive testing instructions
- `data_analysis_report.json` - Detailed data structure analysis results
- `real_data_validation_report.json` - Query validation results

### **Configuration**
- `requirements_test.txt` - Python dependencies for testing

## 🚀 **Quick Start**

### **Run SQL Queries**
```bash
# Run all 7 queries
python run_sql_queries.py

# Run individual queries
python run_sql_queries.py 1    # Weather impact on daily listings
python run_sql_queries.py 2    # Pricing behavior by weather and product
python run_sql_queries.py 3    # Shipping choices vs weather
python run_sql_queries.py 4    # Category demand shifts by weather
python run_sql_queries.py 5    # Seller performance vs weather
python run_sql_queries.py 6    # Listing quality vs weather
python run_sql_queries.py 7    # Zip prefix variation
```

### **Data Analysis**
```bash
# Analyze data structure
python data_analysis.py

# Validate queries against real data
python validate_real_data_queries.py
```

### **Testing**
```bash
# Quick test (no database required)
python quick_test.py

# Full test suite (requires PostgreSQL)
python test_runner.py
```

## 📊 **Query Results**

All 7 SQL queries have been validated against real API data:
- **Total queries**: 7
- **Success rate**: 100%
- **Data coverage**: 113 eBay records, 672 weather records
- **Production ready**: ✅ All queries provide accurate business insights

## 📋 **Business Questions Answered**

1. **Weather impact on daily listings** - Query 1
2. **Pricing behavior by weather and product** - Query 2
3. **Shipping choices vs weather** - Query 3
4. **Category demand shifts by weather** - Query 4
5. **Seller performance vs weather** - Query 5
6. **Listing quality vs weather** - Query 6 (Bonus)
7. **Zip code price variation** - Query 7 (Bonus)

## 🔧 **Requirements**

Install testing dependencies:
```bash
pip install -r requirements_test.txt
```

For database testing, you'll also need:
- PostgreSQL installed and running
- Database credentials configured in `.env` file

See `TESTING_GUIDE.md` for detailed setup instructions.
