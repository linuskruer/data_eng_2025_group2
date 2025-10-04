#!/usr/bin/env python3
"""
SQL Pipeline Test Suite
Tests all SQL queries in the weather x eBay analytics pipeline
"""

import os
import sys
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_results.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SQLPipelineTester:
    def __init__(self, db_config):
        """Initialize the tester with database configuration"""
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.test_results = {}
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def validate_schema(self):
        """Validate that all required tables and columns exist"""
        logger.info("Validating database schema...")
        
        required_tables = [
            'fact_listings', 'dim_weather', 'dim_date', 'dim_location',
            'dim_product', 'dim_seller', 'dim_marketplace', 'dim_currency',
            'dim_condition', 'dim_buying_option'
        ]
        
        schema_valid = True
        missing_tables = []
        
        for table in required_tables:
            try:
                self.cursor.execute(f"SELECT 1 FROM {table} LIMIT 1")
                logger.info(f"✓ Table {table} exists")
            except Exception as e:
                logger.error(f"✗ Table {table} missing: {e}")
                missing_tables.append(table)
                schema_valid = False
        
        if not schema_valid:
            logger.error(f"Schema validation failed. Missing tables: {missing_tables}")
            return False
        
        logger.info("Schema validation passed")
        return True
    
    def check_data_quality(self):
        """Check data quality and availability"""
        logger.info("Checking data quality...")
        
        quality_checks = {
            'fact_listings_count': "SELECT COUNT(*) FROM fact_listings",
            'weather_records_count': "SELECT COUNT(*) FROM dim_weather",
            'location_records_count': "SELECT COUNT(*) FROM dim_location",
            'product_records_count': "SELECT COUNT(*) FROM dim_product",
            'date_range_listings': "SELECT MIN(date_key), MAX(date_key) FROM fact_listings",
            'date_range_weather': "SELECT MIN(date), MAX(date) FROM dim_weather",
            'join_sanity_check': """
                SELECT COUNT(*) FROM fact_listings f
                JOIN dim_weather w ON f.location_key = w.location_key AND f.date_key = w.date
            """
        }
        
        quality_results = {}
        
        for check_name, query in quality_checks.items():
            try:
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                quality_results[check_name] = result[0] if len(result) == 1 else result
                logger.info(f"✓ {check_name}: {result}")
            except Exception as e:
                logger.error(f"✗ {check_name} failed: {e}")
                quality_results[check_name] = None
        
        # Validate minimum data requirements
        if quality_results.get('fact_listings_count', 0) < 100:
            logger.warning("Low fact_listings count - may affect query results")
        
        if quality_results.get('weather_records_count', 0) < 10:
            logger.warning("Low weather records count - may affect query results")
        
        if quality_results.get('join_sanity_check', 0) == 0:
            logger.error("No joinable records between fact_listings and weather - queries will fail")
            return False
        
        self.test_results['data_quality'] = quality_results
        logger.info("Data quality check completed")
        return True
    
    def test_query(self, query_file, query_name):
        """Test a single SQL query"""
        logger.info(f"Testing {query_name}...")
        
        try:
            # Read query file
            query_path = Path("sql_queries") / query_file
            if not query_path.exists():
                logger.error(f"Query file not found: {query_path}")
                return False
            
            with open(query_path, 'r', encoding='utf-8') as f:
                query = f.read()
            
            # Execute query
            start_time = datetime.now()
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Validate results
            if not results:
                logger.warning(f"{query_name} returned no results")
                return False
            
            # Check result structure
            column_count = len(self.cursor.description)
            row_count = len(results)
            
            logger.info(f"✓ {query_name} executed successfully")
            logger.info(f"  - Rows returned: {row_count}")
            logger.info(f"  - Columns: {column_count}")
            logger.info(f"  - Execution time: {execution_time:.2f}s")
            
            # Store results
            self.test_results[query_name] = {
                'status': 'success',
                'row_count': row_count,
                'column_count': column_count,
                'execution_time': execution_time,
                'sample_result': results[0] if results else None
            }
            
            return True
            
        except Exception as e:
            logger.error(f"✗ {query_name} failed: {e}")
            self.test_results[query_name] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def run_all_tests(self):
        """Run all SQL query tests"""
        logger.info("Starting SQL pipeline tests...")
        
        if not self.connect_db():
            return False
        
        try:
            # Validate schema
            if not self.validate_schema():
                return False
            
            # Check data quality
            if not self.check_data_quality():
                return False
            
            # Test all queries
            query_tests = [
                ("impact_listings_by_weather.sql", "Impact Listings by Weather"),
                ("pricing_behavior_by_weather_and_product.sql", "Pricing Behavior by Weather and Product"),
                ("shipping_choices_vs_weather.sql", "Shipping Choices vs Weather"),
                ("category_demand_shifts_by_weather.sql", "Category Demand Shifts by Weather"),
                ("seller_performance_vs_weather.sql", "Seller Performance vs Weather"),
                ("bonus_listing_quality_vs_weather.sql", "Listing Quality vs Weather"),
                ("bonus_zip_prefix_variation.sql", "Zip Prefix Variation")
            ]
            
            passed_tests = 0
            total_tests = len(query_tests)
            
            for query_file, query_name in query_tests:
                if self.test_query(query_file, query_name):
                    passed_tests += 1
            
            # Generate summary
            self.generate_test_summary(passed_tests, total_tests)
            
            return passed_tests == total_tests
            
        finally:
            self.disconnect_db()
    
    def generate_test_summary(self, passed_tests, total_tests):
        """Generate comprehensive test summary"""
        logger.info("=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)
        
        success_rate = (passed_tests / total_tests) * 100
        logger.info(f"Tests passed: {passed_tests}/{total_tests} ({success_rate:.1f}%)")
        
        if passed_tests == total_tests:
            logger.info("🎉 All tests passed! Pipeline is ready for production.")
        else:
            logger.warning(f"⚠️  {total_tests - passed_tests} tests failed. Review errors above.")
        
        # Save detailed results to JSON
        with open('test_results.json', 'w') as f:
            json.dump(self.test_results, f, indent=2, default=str)
        
        logger.info("Detailed results saved to test_results.json")
        logger.info("=" * 60)

def main():
    """Main execution function"""
    # Database configuration - update these values
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'weather_ebay_analytics'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'password'),
        'port': os.getenv('DB_PORT', '5432')
    }
    
    logger.info("Starting SQL Pipeline Test Suite")
    logger.info(f"Database: {db_config['database']}@{db_config['host']}:{db_config['port']}")
    
    # Create tester instance
    tester = SQLPipelineTester(db_config)
    
    # Run tests
    success = tester.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
