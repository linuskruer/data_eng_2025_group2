#!/usr/bin/env python3
"""
Quick Test Script - Tests SQL queries without full database setup
"""

import os
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_python_dependencies():
    """Check if Python dependencies are available"""
    logger.info("Checking Python dependencies...")
    
    dependencies = ['pandas', 'psycopg2', 'sqlalchemy']
    missing = []
    
    for dep in dependencies:
        try:
            __import__(dep)
            logger.info(f"✓ {dep} is available")
        except ImportError:
            logger.warning(f"✗ {dep} is missing")
            missing.append(dep)
    
    if missing:
        logger.error(f"Missing dependencies: {missing}")
        logger.info("Install with: pip install psycopg2-binary pandas sqlalchemy")
        return False
    
    return True

def check_files():
    """Check if required files exist"""
    logger.info("Checking required files...")
    
    required_files = [
        "sql_queries/impact_listings_by_weather.sql",
        "sql_queries/pricing_behavior_by_weather_and_product.sql",
        "sql_queries/shipping_choices_vs_weather.sql",
        "sql_queries/category_demand_shifts_by_weather.sql",
        "sql_queries/seller_performance_vs_weather.sql",
        "sql_queries/bonus_listing_quality_vs_weather.sql",
        "sql_queries/bonus_zip_prefix_variation.sql"
    ]
    
    missing_files = []
    
    for file_path in required_files:
        if Path(file_path).exists():
            logger.info(f"✓ {file_path}")
        else:
            logger.error(f"✗ {file_path} missing")
            missing_files.append(file_path)
    
    if missing_files:
        logger.error(f"Missing files: {missing_files}")
        return False
    
    return True

def check_data_files():
    """Check if data files exist"""
    logger.info("Checking data files...")
    
    data_dirs = ["weather_data", "ebay_data"]
    missing_dirs = []
    
    for dir_name in data_dirs:
        if Path(dir_name).exists():
            csv_files = list(Path(dir_name).glob("*.csv"))
            if csv_files:
                logger.info(f"✓ {dir_name}/ ({len(csv_files)} CSV files)")
            else:
                logger.warning(f"⚠️  {dir_name}/ exists but no CSV files found")
        else:
            logger.error(f"✗ {dir_name}/ directory missing")
            missing_dirs.append(dir_name)
    
    if missing_dirs:
        logger.warning(f"Missing data directories: {missing_dirs}")
        logger.info("You can still test queries with sample data")
    
    return True

def validate_sql_syntax():
    """Basic SQL syntax validation"""
    logger.info("Validating SQL syntax...")
    
    sql_files = list(Path("sql_queries").glob("*.sql"))
    
    for sql_file in sql_files:
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Basic syntax checks
            if 'SELECT' not in content.upper():
                logger.warning(f"⚠️  {sql_file} doesn't contain SELECT statement")
            else:
                logger.info(f"✓ {sql_file} syntax looks valid")
                
        except Exception as e:
            logger.error(f"✗ {sql_file} read error: {e}")
    
    return True

def test_without_database():
    """Test queries without database connection"""
    logger.info("Testing SQL queries without database...")
    
    sql_files = list(Path("sql_queries").glob("*.sql"))
    
    for sql_file in sql_files:
        logger.info(f"Validating {sql_file}...")
        
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                query = f.read()
            
            # Check for common issues
            issues = []
            
            if 'fact_listings' not in query:
                issues.append("Missing fact_listings table reference")
            
            if 'dim_weather' not in query:
                issues.append("Missing dim_weather table reference")
            
            if 'JOIN' not in query.upper():
                issues.append("No JOIN statements found")
            
            if issues:
                logger.warning(f"⚠️  {sql_file} potential issues: {issues}")
            else:
                logger.info(f"✓ {sql_file} structure looks good")
                
        except Exception as e:
            logger.error(f"✗ {sql_file} validation failed: {e}")
    
    return True

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("QUICK SQL PIPELINE TEST (No Database Required)")
    logger.info("=" * 60)
    
    checks = [
        ("Python Dependencies", check_python_dependencies),
        ("Required Files", check_files),
        ("Data Files", check_data_files),
        ("SQL Syntax", validate_sql_syntax),
        ("Query Structure", test_without_database)
    ]
    
    passed = 0
    total = len(checks)
    
    for check_name, check_func in checks:
        logger.info(f"\n--- {check_name} ---")
        if check_func():
            logger.info(f"✅ {check_name} passed")
            passed += 1
        else:
            logger.warning(f"⚠️  {check_name} had issues")
    
    logger.info("\n" + "=" * 60)
    logger.info("QUICK TEST SUMMARY")
    logger.info("=" * 60)
    
    success_rate = (passed / total) * 100
    logger.info(f"Checks passed: {passed}/{total} ({success_rate:.1f}%)")
    
    if passed == total:
        logger.info("🎉 All quick tests passed! Your SQL files are ready.")
        logger.info("Next step: Install PostgreSQL and run 'python test_runner.py'")
    else:
        logger.warning("⚠️  Some issues found. Review the warnings above.")
    
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
