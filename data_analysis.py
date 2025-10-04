#!/usr/bin/env python3
"""
Data Analysis Script
Analyzes actual API data and validates SQL queries against real data structure
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataAnalyzer:
    def __init__(self):
        """Initialize data analyzer"""
        self.ebay_data = None
        self.weather_data = None
        self.analysis_results = {}
        
    def load_data(self):
        """Load and analyze the actual data files"""
        logger.info("Loading and analyzing data files...")
        
        # Load eBay data
        ebay_file = Path("ebay_data/east_coast_weather_data.csv")
        if ebay_file.exists():
            self.ebay_data = pd.read_csv(ebay_file)
            logger.info(f"✓ Loaded eBay data: {len(self.ebay_data)} records")
        else:
            logger.error("✗ eBay data file not found")
            return False
        
        # Load weather data
        weather_file = Path("weather_data/new_york_city_hourly_weather_data.csv")
        if weather_file.exists():
            # Handle the multi-line CSV format
            try:
                self.weather_data = pd.read_csv(weather_file, skiprows=3)  # Skip header lines
                logger.info(f"✓ Loaded weather data: {len(self.weather_data)} records")
            except Exception as e:
                logger.warning(f"Standard CSV read failed: {e}")
                # Try reading with different parameters
                try:
                    self.weather_data = pd.read_csv(weather_file, skiprows=3, on_bad_lines='skip')
                    logger.info(f"✓ Loaded weather data (with error handling): {len(self.weather_data)} records")
                except Exception as e2:
                    logger.error(f"✗ Failed to load weather data: {e2}")
                    return False
        else:
            logger.error("✗ Weather data file not found")
            return False
        
        return True
    
    def analyze_ebay_data(self):
        """Analyze eBay data structure and content"""
        logger.info("Analyzing eBay data structure...")
        
        if self.ebay_data is None:
            return False
        
        analysis = {
            'total_records': len(self.ebay_data),
            'columns': list(self.ebay_data.columns),
            'data_types': self.ebay_data.dtypes.to_dict(),
            'null_counts': self.ebay_data.isnull().sum().to_dict(),
            'date_range': {},
            'product_analysis': {},
            'location_analysis': {},
            'price_analysis': {},
            'weather_category_analysis': {}
        }
        
        # Date range analysis
        if 'collection_timestamp' in self.ebay_data.columns:
            self.ebay_data['collection_timestamp'] = pd.to_datetime(self.ebay_data['collection_timestamp'])
            analysis['date_range'] = {
                'earliest': self.ebay_data['collection_timestamp'].min(),
                'latest': self.ebay_data['collection_timestamp'].max(),
                'span_days': (self.ebay_data['collection_timestamp'].max() - self.ebay_data['collection_timestamp'].min()).days
            }
        
        # Product type analysis
        if 'product_type' in self.ebay_data.columns:
            product_counts = self.ebay_data['product_type'].value_counts()
            analysis['product_analysis'] = {
                'unique_products': len(product_counts),
                'top_products': product_counts.head(10).to_dict(),
                'product_distribution': (product_counts / len(self.ebay_data) * 100).round(2).to_dict()
            }
        
        # Location analysis
        if 'item_location' in self.ebay_data.columns:
            # Extract zip codes from location data
            locations = self.ebay_data['item_location'].dropna()
            zip_codes = []
            for loc in locations:
                if isinstance(loc, str) and 'postalCode' in loc:
                    try:
                        import re
                        zip_match = re.search(r"'(\d{3})\*\*'", loc)
                        if zip_match:
                            zip_codes.append(zip_match.group(1))
                    except:
                        pass
            
            analysis['location_analysis'] = {
                'unique_locations': len(locations.unique()),
                'zip_codes_found': len(set(zip_codes)),
                'top_zip_codes': pd.Series(zip_codes).value_counts().head(10).to_dict()
            }
        
        # Price analysis
        if 'price' in self.ebay_data.columns:
            prices = pd.to_numeric(self.ebay_data['price'], errors='coerce')
            analysis['price_analysis'] = {
                'min_price': prices.min(),
                'max_price': prices.max(),
                'avg_price': prices.mean(),
                'median_price': prices.median(),
                'std_price': prices.std(),
                'price_range': prices.max() - prices.min()
            }
        
        # Weather category analysis
        if 'weather_category' in self.ebay_data.columns:
            weather_cats = self.ebay_data['weather_category'].value_counts()
            analysis['weather_category_analysis'] = {
                'unique_categories': len(weather_cats),
                'category_distribution': weather_cats.to_dict(),
                'category_percentages': (weather_cats / len(self.ebay_data) * 100).round(2).to_dict()
            }
        
        self.analysis_results['ebay_data'] = analysis
        logger.info("✓ eBay data analysis completed")
        return True
    
    def analyze_weather_data(self):
        """Analyze weather data structure and content"""
        logger.info("Analyzing weather data structure...")
        
        if self.weather_data is None:
            return False
        
        analysis = {
            'total_records': len(self.weather_data),
            'columns': list(self.weather_data.columns),
            'data_types': self.weather_data.dtypes.to_dict(),
            'null_counts': self.weather_data.isnull().sum().to_dict(),
            'date_range': {},
            'temperature_analysis': {},
            'precipitation_analysis': {},
            'weather_code_analysis': {}
        }
        
        # Date range analysis
        if 'time' in self.weather_data.columns:
            self.weather_data['time'] = pd.to_datetime(self.weather_data['time'])
            analysis['date_range'] = {
                'earliest': self.weather_data['time'].min(),
                'latest': self.weather_data['time'].max(),
                'span_days': (self.weather_data['time'].max() - self.weather_data['time'].min()).days
            }
        
        # Temperature analysis
        temp_col = 'temperature_2m (°C)'
        if temp_col in self.weather_data.columns:
            temps = pd.to_numeric(self.weather_data[temp_col], errors='coerce')
            analysis['temperature_analysis'] = {
                'min_temp': temps.min(),
                'max_temp': temps.max(),
                'avg_temp': temps.mean(),
                'median_temp': temps.median(),
                'std_temp': temps.std(),
                'temp_range': temps.max() - temps.min()
            }
        
        # Precipitation analysis
        rain_col = 'rain (mm)'
        if rain_col in self.weather_data.columns:
            rain = pd.to_numeric(self.weather_data[rain_col], errors='coerce')
            analysis['precipitation_analysis'] = {
                'min_rain': rain.min(),
                'max_rain': rain.max(),
                'avg_rain': rain.mean(),
                'median_rain': rain.median(),
                'rainy_days': (rain > 0).sum(),
                'heavy_rain_days': (rain > 10).sum()
            }
        
        # Weather code analysis
        if 'weather_code (wmo code)' in self.weather_data.columns:
            weather_codes = self.weather_data['weather_code (wmo code)'].value_counts()
            analysis['weather_code_analysis'] = {
                'unique_codes': len(weather_codes),
                'code_distribution': weather_codes.head(10).to_dict()
            }
        
        self.analysis_results['weather_data'] = analysis
        logger.info("✓ Weather data analysis completed")
        return True
    
    def validate_sql_queries(self):
        """Validate SQL queries against actual data structure"""
        logger.info("Validating SQL queries against actual data...")
        
        validation_results = {}
        
        # Check if required columns exist in actual data
        required_ebay_cols = [
            'collection_timestamp', 'product_type', 'price', 'currency',
            'seller_feedback_percentage', 'seller_feedback_score',
            'item_location', 'shipping_cost', 'free_shipping', 'condition',
            'buying_options', 'title_length', 'item_id', 'marketplace_id'
        ]
        
        required_weather_cols = [
            'time', 'weather_code (wmo code)', 'temperature_2m (°C)',
            'relative_humidity_2m (%)', 'cloudcover (%)', 'rain (mm)',
            'sunshine_duration (s)', 'windspeed_10m (km/h)'
        ]
        
        # Check eBay data columns
        ebay_missing = []
        ebay_available = []
        for col in required_ebay_cols:
            if col in self.ebay_data.columns:
                ebay_available.append(col)
            else:
                ebay_missing.append(col)
        
        # Check weather data columns
        weather_missing = []
        weather_available = []
        for col in required_weather_cols:
            if col in self.weather_data.columns:
                weather_available.append(col)
            else:
                weather_missing.append(col)
        
        validation_results['column_validation'] = {
            'ebay_available': ebay_available,
            'ebay_missing': ebay_missing,
            'weather_available': weather_available,
            'weather_missing': weather_missing,
            'ebay_coverage': len(ebay_available) / len(required_ebay_cols) * 100,
            'weather_coverage': len(weather_available) / len(required_weather_cols) * 100
        }
        
        # Test data joinability
        join_analysis = self.analyze_data_joinability()
        validation_results['join_analysis'] = join_analysis
        
        # Test weather bucket logic
        bucket_analysis = self.test_weather_buckets()
        validation_results['bucket_analysis'] = bucket_analysis
        
        self.analysis_results['sql_validation'] = validation_results
        logger.info("✓ SQL query validation completed")
        return True
    
    def analyze_data_joinability(self):
        """Analyze how well eBay and weather data can be joined"""
        logger.info("Analyzing data joinability...")
        
        if self.ebay_data is None or self.weather_data is None:
            return {'error': 'Data not loaded'}
        
        # Extract dates from both datasets
        ebay_dates = pd.to_datetime(self.ebay_data['collection_timestamp']).dt.date
        weather_dates = pd.to_datetime(self.weather_data['time']).dt.date
        
        # Find overlapping dates
        ebay_unique_dates = set(ebay_dates)
        weather_unique_dates = set(weather_dates)
        overlapping_dates = ebay_unique_dates.intersection(weather_unique_dates)
        
        return {
            'ebay_unique_dates': len(ebay_unique_dates),
            'weather_unique_dates': len(weather_unique_dates),
            'overlapping_dates': len(overlapping_dates),
            'join_coverage': len(overlapping_dates) / len(ebay_unique_dates) * 100 if ebay_unique_dates else 0,
            'sample_overlapping_dates': list(overlapping_dates)[:5]
        }
    
    def test_weather_buckets(self):
        """Test weather bucket logic against actual data"""
        logger.info("Testing weather bucket logic...")
        
        if self.weather_data is None:
            return {'error': 'Weather data not loaded'}
        
        # Test bucket logic
        temp_col = 'temperature_2m (°C)'
        rain_col = 'rain (mm)'
        
        if temp_col in self.weather_data.columns and rain_col in self.weather_data.columns:
            temps = pd.to_numeric(self.weather_data[temp_col], errors='coerce')
            rain = pd.to_numeric(self.weather_data[rain_col], errors='coerce')
            
            # Apply bucket logic
            buckets = []
            for i, (temp, rain_val) in enumerate(zip(temps, rain)):
                if pd.isna(temp) or pd.isna(rain_val):
                    buckets.append('Unknown')
                elif rain_val > 10:
                    buckets.append('Precipitation-Heavy')
                elif temp >= 32:
                    buckets.append('Extreme Heat')
                elif temp <= -5:
                    buckets.append('Extreme Cold')
                else:
                    buckets.append('Normal')
            
            bucket_counts = pd.Series(buckets).value_counts()
            
            return {
                'bucket_distribution': bucket_counts.to_dict(),
                'bucket_percentages': (bucket_counts / len(buckets) * 100).round(2).to_dict(),
                'total_classified': len(buckets),
                'unknown_count': bucket_counts.get('Unknown', 0)
            }
        
        return {'error': 'Required weather columns not found'}
    
    def generate_recommendations(self):
        """Generate recommendations based on data analysis"""
        logger.info("Generating recommendations...")
        
        recommendations = []
        
        # Column mapping recommendations
        if 'sql_validation' in self.analysis_results:
            validation = self.analysis_results['sql_validation']
            
            if validation['column_validation']['ebay_missing']:
                recommendations.append({
                    'type': 'data_mapping',
                    'priority': 'high',
                    'message': f"Missing eBay columns: {validation['column_validation']['ebay_missing']}",
                    'action': 'Update SQL queries to use available columns or add missing data'
                })
            
            if validation['column_validation']['weather_missing']:
                recommendations.append({
                    'type': 'data_mapping',
                    'priority': 'high',
                    'message': f"Missing weather columns: {validation['column_validation']['weather_missing']}",
                    'action': 'Update SQL queries to use available columns or add missing data'
                })
        
        # Join analysis recommendations
        if 'sql_validation' in self.analysis_results:
            join_analysis = self.analysis_results['sql_validation']['join_analysis']
            
            if join_analysis['join_coverage'] < 50:
                recommendations.append({
                    'type': 'data_quality',
                    'priority': 'medium',
                    'message': f"Low join coverage: {join_analysis['join_coverage']:.1f}%",
                    'action': 'Consider data alignment or additional data sources'
                })
        
        # Weather bucket recommendations
        if 'sql_validation' in self.analysis_results:
            bucket_analysis = self.analysis_results['sql_validation']['bucket_analysis']
            
            if bucket_analysis.get('unknown_count', 0) > 0:
                recommendations.append({
                    'type': 'data_quality',
                    'priority': 'low',
                    'message': f"Unknown weather conditions: {bucket_analysis['unknown_count']} records",
                    'action': 'Review weather data quality and bucket logic'
                })
        
        self.analysis_results['recommendations'] = recommendations
        logger.info(f"✓ Generated {len(recommendations)} recommendations")
        return True
    
    def generate_report(self):
        """Generate comprehensive analysis report"""
        logger.info("Generating analysis report...")
        
        report = {
            'analysis_timestamp': datetime.now().isoformat(),
            'data_summary': {
                'ebay_records': len(self.ebay_data) if self.ebay_data is not None else 0,
                'weather_records': len(self.weather_data) if self.weather_data is not None else 0
            },
            'detailed_analysis': self.analysis_results,
            'summary': {
                'data_quality_score': self.calculate_data_quality_score(),
                'sql_compatibility_score': self.calculate_sql_compatibility_score(),
                'overall_readiness': self.assess_overall_readiness()
            }
        }
        
        # Save report
        with open('data_analysis_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info("✓ Analysis report saved to data_analysis_report.json")
        return report
    
    def calculate_data_quality_score(self):
        """Calculate data quality score (0-100)"""
        score = 100
        
        # Penalize for missing columns
        if 'sql_validation' in self.analysis_results:
            validation = self.analysis_results['sql_validation']['column_validation']
            missing_ebay = len(validation['ebay_missing'])
            missing_weather = len(validation['weather_missing'])
            score -= (missing_ebay + missing_weather) * 5
        
        # Penalize for low join coverage
        if 'sql_validation' in self.analysis_results:
            join_coverage = self.analysis_results['sql_validation']['join_analysis']['join_coverage']
            if join_coverage < 50:
                score -= (50 - join_coverage) * 0.5
        
        return max(0, min(100, score))
    
    def calculate_sql_compatibility_score(self):
        """Calculate SQL compatibility score (0-100)"""
        if 'sql_validation' not in self.analysis_results:
            return 0
        
        validation = self.analysis_results['sql_validation']['column_validation']
        ebay_coverage = validation['ebay_coverage']
        weather_coverage = validation['weather_coverage']
        
        return (ebay_coverage + weather_coverage) / 2
    
    def assess_overall_readiness(self):
        """Assess overall readiness for SQL queries"""
        data_quality = self.calculate_data_quality_score()
        sql_compatibility = self.calculate_sql_compatibility_score()
        
        if data_quality >= 80 and sql_compatibility >= 80:
            return 'Ready'
        elif data_quality >= 60 and sql_compatibility >= 60:
            return 'Needs Minor Adjustments'
        else:
            return 'Needs Major Work'
    
    def run_complete_analysis(self):
        """Run complete data analysis"""
        logger.info("=" * 60)
        logger.info("STARTING COMPLETE DATA ANALYSIS")
        logger.info("=" * 60)
        
        steps = [
            ("Load Data", self.load_data),
            ("Analyze eBay Data", self.analyze_ebay_data),
            ("Analyze Weather Data", self.analyze_weather_data),
            ("Validate SQL Queries", self.validate_sql_queries),
            ("Generate Recommendations", self.generate_recommendations),
            ("Generate Report", self.generate_report)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"\n--- {step_name} ---")
            if not step_func():
                logger.error(f"❌ {step_name} failed")
                return False
            logger.info(f"✅ {step_name} completed successfully")
        
        logger.info("\n" + "=" * 60)
        logger.info("🎉 DATA ANALYSIS COMPLETED!")
        logger.info("=" * 60)
        
        # Print summary
        if 'summary' in self.analysis_results:
            summary = self.analysis_results['summary']
            logger.info(f"Data Quality Score: {summary['data_quality_score']:.1f}/100")
            logger.info(f"SQL Compatibility Score: {summary['sql_compatibility_score']:.1f}/100")
            logger.info(f"Overall Readiness: {summary['overall_readiness']}")
        
        return True

def main():
    """Main execution function"""
    analyzer = DataAnalyzer()
    success = analyzer.run_complete_analysis()
    
    if success:
        print("\n📊 Analysis complete! Check data_analysis_report.json for detailed results.")
    else:
        print("\n❌ Analysis failed. Check logs for details.")

if __name__ == "__main__":
    main()
