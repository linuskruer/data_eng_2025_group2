#!/usr/bin/env python3
"""
Test Runner - Orchestrates the complete testing pipeline
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestRunner:
    def __init__(self):
        """Initialize test runner"""
        self.test_results = {}
        
    def check_prerequisites(self):
        """Check if all prerequisites are met"""
        logger.info("Checking prerequisites...")
        
        # Check if PostgreSQL is running
        try:
            result = subprocess.run(['psql', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"✓ PostgreSQL found: {result.stdout.strip()}")
            else:
                logger.error("✗ PostgreSQL not found or not accessible")
                return False
        except FileNotFoundError:
            logger.error("✗ PostgreSQL not installed or not in PATH")
            return False
        
        # Check if required directories exist
        required_dirs = ['sql_queries', 'ebay_data', 'weather_data']
        for dir_name in required_dirs:
            if Path(dir_name).exists():
                logger.info(f"✓ Directory {dir_name} exists")
            else:
                logger.warning(f"⚠️  Directory {dir_name} not found")
        
        # Check if SQL query files exist
        sql_files = list(Path("sql_queries").glob("*.sql"))
        if sql_files:
            logger.info(f"✓ Found {len(sql_files)} SQL query files")
        else:
            logger.error("✗ No SQL query files found in sql_queries/")
            return False
        
        return True
    
    def setup_environment(self):
        """Set up testing environment"""
        logger.info("Setting up testing environment...")
        
        # Install test requirements
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements_test.txt'], 
                         check=True, capture_output=True)
            logger.info("✓ Test requirements installed")
        except subprocess.CalledProcessError as e:
            logger.error(f"✗ Failed to install test requirements: {e}")
            return False
        
        return True
    
    def setup_database(self):
        """Set up database schema and data"""
        logger.info("Setting up database...")
        
        try:
            result = subprocess.run([sys.executable, 'setup_database.py'], 
                                  capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("✓ Database setup completed successfully")
                return True
            else:
                logger.error(f"✗ Database setup failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"✗ Database setup failed: {e}")
            return False
    
    def run_sql_tests(self):
        """Run SQL pipeline tests"""
        logger.info("Running SQL pipeline tests...")
        
        try:
            result = subprocess.run([sys.executable, 'test_sql_pipeline.py'], 
                                  capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("✓ SQL pipeline tests completed successfully")
                return True
            else:
                logger.error(f"✗ SQL pipeline tests failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"✗ SQL pipeline tests failed: {e}")
            return False
    
    def generate_report(self):
        """Generate comprehensive test report"""
        logger.info("Generating test report...")
        
        report_content = """
# SQL Pipeline Test Report

## Overview
This report summarizes the results of testing the weather x eBay analytics SQL pipeline.

## Test Results
"""
        
        # Check if test results file exists
        if Path("test_results.json").exists():
            import json
            with open("test_results.json", 'r') as f:
                results = json.load(f)
            
            report_content += f"""
## Data Quality Metrics
- Fact Listings: {results.get('data_quality', {}).get('fact_listings_count', 'N/A')}
- Weather Records: {results.get('data_quality', {}).get('weather_records_count', 'N/A')}
- Location Records: {results.get('data_quality', {}).get('location_records_count', 'N/A')}
- Product Records: {results.get('data_quality', {}).get('product_records_count', 'N/A')}

## Query Test Results
"""
            
            for query_name, result in results.items():
                if query_name != 'data_quality':
                    status = result.get('status', 'unknown')
                    if status == 'success':
                        report_content += f"- ✅ {query_name}: {result.get('row_count', 'N/A')} rows, {result.get('execution_time', 'N/A')}s\n"
                    else:
                        report_content += f"- ❌ {query_name}: {result.get('error', 'Unknown error')}\n"
        
        report_content += """
## Next Steps
1. Review any failed tests and fix issues
2. Validate data quality metrics
3. Run queries manually to verify results
4. Consider performance optimization if needed

## Files Generated
- test_results.log: Detailed test execution log
- test_results.json: Machine-readable test results
- test_report.md: This report
"""
        
        # Write report
        with open("test_report.md", 'w') as f:
            f.write(report_content)
        
        logger.info("✓ Test report generated: test_report.md")
    
    def run_all_tests(self):
        """Run complete test suite"""
        logger.info("=" * 60)
        logger.info("STARTING COMPLETE TEST SUITE")
        logger.info("=" * 60)
        
        steps = [
            ("Check Prerequisites", self.check_prerequisites),
            ("Setup Environment", self.setup_environment),
            ("Setup Database", self.setup_database),
            ("Run SQL Tests", self.run_sql_tests),
            ("Generate Report", self.generate_report)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"\n--- {step_name} ---")
            if not step_func():
                logger.error(f"❌ {step_name} failed. Stopping test suite.")
                return False
            logger.info(f"✅ {step_name} completed successfully")
        
        logger.info("\n" + "=" * 60)
        logger.info("🎉 ALL TESTS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info("Check test_report.md for detailed results")
        
        return True

def main():
    """Main execution function"""
    runner = TestRunner()
    success = runner.run_all_tests()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
