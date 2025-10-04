#!/usr/bin/env python3
"""
Data Validation Analyzer
Analyzes actual API data structure and validates SQL query compatibility
"""

import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataValidationAnalyzer:
    def __init__(self):
        """Initialize the data validation analyzer"""
        self.ebay_data = None
        self.weather_data = None
        self.validation_results = {}
        
    def load_data(self):
        """Load and analyze the actual data files"""
        logger.info("Loading and analyzing data files...")
        
        # Load eBay data
        ebay_files = list(Path("ebay_data").glob("*.csv"))
        if ebay_files:
            try:
                self.ebay_data = pd.read_csv(ebay_files[0])
                logger.info(f"✓ Loaded eBay data: {len(self.ebay_data)} records from {ebay_files[0].name}")
            except Exception as e:
                logger.error(f"✗ Failed to load eBay data: {e}")
                return False
        
        # Load weather data
        weather_files = list(Path("weather_data").glob("*.csv"))
        if weather_files:
            try:
                self.weather_data = pd.read_csv(weather_files[0])
                logger.info(f"✓ Loaded weather data: {len(self.weather_data)} records from {weather_files[0].name}")
            except Exception as e:
                logger.error(f"✗ Failed to load weather data: {e}")
                return False
        
        return True
    
    def analyze_ebay_data_structure(self):
        """Analyze eBay data structure and compatibility with SQL queries"""
        logger.info("Analyzing eBay data structure...")
        
        if self.ebay_data is None:
            logger.error("No eBay data loaded")
            return False
        
        analysis = {
            'total_records': len(self.ebay_data),
            'columns': list(self.ebay_data.columns),
            'data_types': self.ebay_data.dtypes.to_dict(),
            'null_counts': self.ebay_data.isnull().sum().to_dict(),
            'sample_data': self.ebay_data.head(3).to_dict(),
            'issues': []
        }
        
        # Check required columns for SQL queries
        required_columns = [
            'collection_timestamp', 'product_type', 'price', 'item_id',
            'item_location', 'seller_feedback_score', 'seller_feedback_percentage',
            'shipping_cost', 'free_shipping', 'condition', 'buying_options'
        ]
        
        missing_columns = [col for col in required_columns if col not in self.ebay_data.columns]
        if missing_columns:
            analysis['issues'].append(f"Missing required columns: {missing_columns}")
        
        # Check data quality issues
        if 'collection_timestamp' in self.ebay_data.columns:
            try:
                self.ebay_data['collection_timestamp'] = pd.to_datetime(self.ebay_data['collection_timestamp'])
                analysis['date_range'] = {
                    'earliest': self.ebay_data['collection_timestamp'].min(),
                    'latest': self.ebay_data['collection_timestamp'].max()
                }
            except Exception as e:
                analysis['issues'].append(f"Date parsing error: {e}")
        
        # Check location data format
        if 'item_location' in self.ebay_data.columns:
            location_sample = self.ebay_data['item_location'].dropna().iloc[0] if len(self.ebay_data['item_location'].dropna()) > 0 else None
            if location_sample and isinstance(location_sample, str) and location_sample.startswith('{'):
                analysis['issues'].append("item_location is JSON string - needs parsing")
                analysis['location_format'] = 'JSON'
            else:
                analysis['location_format'] = 'TEXT'
        
        # Check product types
        if 'product_type' in self.ebay_data.columns:
            analysis['product_types'] = self.ebay_data['product_type'].value_counts().to_dict()
        
        # Check weather categories
        if 'weather_category' in self.ebay_data.columns:
            analysis['weather_categories'] = self.ebay_data['weather_category'].value_counts().to_dict()
        
        self.validation_results['ebay_analysis'] = analysis
        logger.info(f"eBay data analysis completed: {len(analysis['issues'])} issues found")
        return True
    
    def analyze_weather_data_structure(self):
        """Analyze weather data structure and compatibility with SQL queries"""
        logger.info("Analyzing weather data structure...")
        
        if self.weather_data is None:
            logger.error("No weather data loaded")
            return False
        
        analysis = {
            'total_records': len(self.weather_data),
            'columns': list(self.weather_data.columns),
            'data_types': self.weather_data.dtypes.to_dict(),
            'null_counts': self.weather_data.isnull().sum().to_dict(),
            'sample_data': self.weather_data.head(3).to_dict(),
            'issues': []
        }
        
        # Check required columns for SQL queries
        required_columns = ['time', 'temperature_2m', 'rain', 'sunshine_duration']
        missing_columns = [col for col in required_columns if col not in self.weather_data.columns]
        if missing_columns:
            analysis['issues'].append(f"Missing required columns: {missing_columns}")
        
        # Check time format
        if 'time' in self.weather_data.columns:
            try:
                self.weather_data['time'] = pd.to_datetime(self.weather_data['time'])
                analysis['date_range'] = {
                    'earliest': self.weather_data['time'].min(),
                    'latest': self.weather_data['time'].max()
                }
                analysis['time_format'] = 'ISO_DATETIME'
            except Exception as e:
                analysis['issues'].append(f"Time parsing error: {e}")
        
        # Check temperature data
        if 'temperature_2m' in self.weather_data.columns:
            temp_stats = self.weather_data['temperature_2m'].describe()
            analysis['temperature_stats'] = {
                'min': temp_stats['min'],
                'max': temp_stats['max'],
                'mean': temp_stats['mean'],
                'std': temp_stats['std']
            }
        
        # Check rain data
        if 'rain' in self.weather_data.columns:
            rain_stats = self.weather_data['rain'].describe()
            analysis['rain_stats'] = {
                'min': rain_stats['min'],
                'max': rain_stats['max'],
                'mean': rain_stats['mean'],
                'non_zero_count': (self.weather_data['rain'] > 0).sum()
            }
        
        self.validation_results['weather_analysis'] = analysis
        logger.info(f"Weather data analysis completed: {len(analysis['issues'])} issues found")
        return True
    
    def validate_sql_query_compatibility(self):
        """Validate if SQL queries will work with actual data structure"""
        logger.info("Validating SQL query compatibility...")
        
        compatibility_issues = []
        
        # Check if we can create the required joins
        if self.ebay_data is not None and self.weather_data is not None:
            # Check date compatibility
            ebay_dates = pd.to_datetime(self.ebay_data['collection_timestamp']).dt.date.unique()
            weather_dates = pd.to_datetime(self.weather_data['time']).dt.date.unique()
            
            common_dates = set(ebay_dates) & set(weather_dates)
            if len(common_dates) == 0:
                compatibility_issues.append("No common dates between eBay and weather data")
            else:
                logger.info(f"Found {len(common_dates)} common dates for joining")
        
        # Check location mapping
        if self.ebay_data is not None and 'item_location' in self.ebay_data.columns:
            location_sample = self.ebay_data['item_location'].dropna().iloc[0] if len(self.ebay_data['item_location'].dropna()) > 0 else None
            if location_sample and location_sample.startswith('{'):
                compatibility_issues.append("item_location is JSON - needs parsing for location_key mapping")
        
        # Check weather bucket logic
        if self.weather_data is not None:
            if 'temperature_2m' in self.weather_data.columns:
                extreme_heat = (self.weather_data['temperature_2m'] >= 32).sum()
                extreme_cold = (self.weather_data['temperature_2m'] <= -5).sum()
                logger.info(f"Weather buckets: {extreme_heat} extreme heat, {extreme_cold} extreme cold records")
            
            if 'rain' in self.weather_data.columns:
                heavy_rain = (self.weather_data['rain'] > 10).sum()
                logger.info(f"Weather buckets: {heavy_rain} heavy rain records")
        
        self.validation_results['compatibility_issues'] = compatibility_issues
        return len(compatibility_issues) == 0
    
    def generate_data_transformation_recommendations(self):
        """Generate recommendations for data transformation"""
        logger.info("Generating data transformation recommendations...")
        
        recommendations = []
        
        # eBay data transformations
        if self.ebay_data is not None:
            if 'item_location' in self.ebay_data.columns:
                location_sample = self.ebay_data['item_location'].dropna().iloc[0] if len(self.ebay_data['item_location'].dropna()) > 0 else None
                if location_sample and location_sample.startswith('{'):
                    recommendations.append({
                        'type': 'ebay_location_parsing',
                        'description': 'Parse JSON item_location to extract postal code and city',
                        'code': '''
import json
import re

def parse_location(location_str):
    if pd.isna(location_str):
        return None, None, None
    
    try:
        location_data = json.loads(location_str)
        postal_code = location_data.get('postalCode', '')
        country = location_data.get('country', '')
        
        # Extract city from postal code
        if postal_code:
            zip_prefix = postal_code[:3]
            city_map = {
                '100': 'New York', '101': 'New York', '102': 'New York',
                '331': 'Miami', '332': 'Miami',
                '021': 'Boston', '022': 'Boston',
                '200': 'Washington', '202': 'Washington',
                '191': 'Philadelphia', '192': 'Philadelphia'
            }
            city = city_map.get(zip_prefix, 'Unknown')
            return city, zip_prefix, country
    except:
        pass
    
    return None, None, None

# Apply to DataFrame
self.ebay_data[['city', 'zip_prefix', 'country']] = self.ebay_data['item_location'].apply(
    lambda x: pd.Series(parse_location(x))
)
                        '''
                    })
        
        # Weather data transformations
        if self.weather_data is not None:
            recommendations.append({
                'type': 'weather_daily_aggregation',
                'description': 'Aggregate hourly weather data to daily for star schema',
                'code': '''
# Aggregate weather data to daily
daily_weather = self.weather_data.groupby(self.weather_data['time'].dt.date).agg({
    'temperature_2m': ['mean', 'min', 'max'],
    'rain': 'sum',
    'sunshine_duration': 'sum',
    'windspeed_10m': 'mean'
}).reset_index()

# Flatten column names
daily_weather.columns = ['date', 'temperature_mean', 'temperature_min', 'temperature_max', 
                        'rain_sum', 'sunshine_duration', 'wind_speed_10m']
                        '''
            })
        
        self.validation_results['recommendations'] = recommendations
        return recommendations
    
    def create_corrected_sql_queries(self):
        """Create corrected SQL queries based on actual data structure"""
        logger.info("Creating corrected SQL queries...")
        
        corrected_queries = {}
        
        # Sample corrected query for impact_listings_by_weather
        corrected_queries['impact_listings_by_weather'] = '''
-- CORRECTED: Impact Listings by Weather (based on actual data structure)
WITH wx_bucket AS (
  SELECT
    w.location_key,
    DATE(w.time) as date_key,
    CASE
      WHEN w.rain > 10 THEN 'Precipitation-Heavy'
      WHEN w.temperature_2m >= 32 THEN 'Extreme Heat'
      WHEN w.temperature_2m <= -5 THEN 'Extreme Cold'
      ELSE 'Normal'
    END AS weather_bucket
  FROM dim_weather w
  JOIN dim_location l ON w.location_key = l.location_key
  WHERE l.country_code = 'US'
    AND l.state_code IN ('NY', 'FL', 'MA', 'DC', 'PA', 'VA', 'NC', 'SC', 'GA')
),
ebay_daily AS (
  SELECT
    DATE(collection_timestamp) as date_key,
    city,
    COUNT(*) as listings_count
  FROM fact_listings
  GROUP BY DATE(collection_timestamp), city
)
SELECT
  e.date_key,
  e.city,
  COALESCE(w.weather_bucket, 'Unknown') as weather_bucket,
  e.listings_count
FROM ebay_daily e
LEFT JOIN wx_bucket w ON e.date_key = w.date_key AND e.city = w.location_name
ORDER BY e.date_key, e.city;
        '''
        
        self.validation_results['corrected_queries'] = corrected_queries
        return corrected_queries
    
    def run_complete_analysis(self):
        """Run complete data validation analysis"""
        logger.info("=" * 60)
        logger.info("STARTING DATA VALIDATION ANALYSIS")
        logger.info("=" * 60)
        
        if not self.load_data():
            return False
        
        # Run all analyses
        self.analyze_ebay_data_structure()
        self.analyze_weather_data_structure()
        self.validate_sql_query_compatibility()
        self.generate_data_transformation_recommendations()
        self.create_corrected_sql_queries()
        
        # Generate comprehensive report
        self.generate_validation_report()
        
        logger.info("=" * 60)
        logger.info("DATA VALIDATION ANALYSIS COMPLETED")
        logger.info("=" * 60)
        
        return True
    
    def generate_validation_report(self):
        """Generate comprehensive validation report"""
        logger.info("Generating validation report...")
        
        report = f"""
# Data Validation Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Executive Summary
This report analyzes the actual API data structure and validates compatibility with the SQL queries.

## eBay Data Analysis
- **Total Records**: {self.validation_results.get('ebay_analysis', {}).get('total_records', 'N/A')}
- **Columns**: {len(self.validation_results.get('ebay_analysis', {}).get('columns', []))}
- **Issues Found**: {len(self.validation_results.get('ebay_analysis', {}).get('issues', []))}

### Key Issues:
"""
        
        for issue in self.validation_results.get('ebay_analysis', {}).get('issues', []):
            report += f"- {issue}\n"
        
        report += f"""
## Weather Data Analysis
- **Total Records**: {self.validation_results.get('weather_analysis', {}).get('total_records', 'N/A')}
- **Columns**: {len(self.validation_results.get('weather_analysis', {}).get('columns', []))}
- **Issues Found**: {len(self.validation_results.get('weather_analysis', {}).get('issues', []))}

### Key Issues:
"""
        
        for issue in self.validation_results.get('weather_analysis', {}).get('issues', []):
            report += f"- {issue}\n"
        
        report += f"""
## SQL Query Compatibility
- **Compatibility Issues**: {len(self.validation_results.get('compatibility_issues', []))}

### Issues:
"""
        
        for issue in self.validation_results.get('compatibility_issues', []):
            report += f"- {issue}\n"
        
        report += """
## Recommendations
"""
        
        for rec in self.validation_results.get('recommendations', []):
            report += f"### {rec['type']}\n{rec['description']}\n\n"
        
        # Save report
        with open('data_validation_report.md', 'w') as f:
            f.write(report)
        
        # Save detailed results
        with open('data_validation_results.json', 'w') as f:
            json.dump(self.validation_results, f, indent=2, default=str)
        
        logger.info("✓ Validation report saved: data_validation_report.md")
        logger.info("✓ Detailed results saved: data_validation_results.json")

def main():
    """Main execution function"""
    analyzer = DataValidationAnalyzer()
    success = analyzer.run_complete_analysis()
    
    if success:
        logger.info("🎉 Data validation analysis completed successfully!")
        logger.info("Check data_validation_report.md for detailed findings")
    else:
        logger.error("❌ Data validation analysis failed")
    
    return success

if __name__ == "__main__":
    main()
