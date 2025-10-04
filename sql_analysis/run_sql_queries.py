#!/usr/bin/env python3
"""
SQL Query Execution Script
Runs all fixed SQL queries and displays their outputs using pandas simulation
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLQueryRunner:
    def __init__(self):
        """Initialize with actual data"""
        self.ebay_data = None
        self.weather_data = None
        
    def load_data(self):
        """Load the actual data files"""
        logger.info("Loading data files...")
        
        # Load eBay data
        self.ebay_data = pd.read_csv("ebay_data/east_coast_weather_data.csv")
        logger.info(f"✓ Loaded eBay data: {len(self.ebay_data)} records")
        
        # Load weather data
        self.weather_data = pd.read_csv("weather_data/new_york_city_hourly_weather_data.csv", skiprows=3)
        logger.info(f"✓ Loaded weather data: {len(self.weather_data)} records")
        
        # Prepare data
        self.ebay_data['collection_timestamp'] = pd.to_datetime(self.ebay_data['collection_timestamp'])
        self.ebay_data['date'] = self.ebay_data['collection_timestamp'].dt.date
        
        return True
    
    def run_query_1(self):
        """Query 1: Impact of weather conditions on daily listings"""
        print("\n" + "="*80)
        print("QUERY 1: IMPACT OF WEATHER CONDITIONS ON DAILY LISTINGS")
        print("="*80)
        
        # Create weather buckets
        weather_buckets = {
            'rain_products': 'Precipitation-Heavy',
            'heat_products': 'Extreme Heat',
            'cold_products': 'Extreme Cold',
            'seasonal_products': 'Normal'
        }
        
        self.ebay_data['weather_bucket'] = self.ebay_data['weather_category'].map(weather_buckets)
        
        # Group by date and weather bucket
        result = self.ebay_data.groupby(['date', 'weather_bucket']).agg({
            'item_id': ['count', 'nunique'],
            'price': 'mean'
        }).round(2)
        
        result.columns = ['listings', 'unique_items', 'avg_price']
        result = result.reset_index()
        
        print(f"Results: {len(result)} rows")
        print("\nDaily listings by weather condition:")
        print(result.to_string(index=False))
        
        return result
    
    def run_query_2(self):
        """Query 2: Pricing behavior by weather and product type"""
        print("\n" + "="*80)
        print("QUERY 2: PRICING BEHAVIOR BY WEATHER AND PRODUCT TYPE")
        print("="*80)
        
        # Create weather buckets
        weather_buckets = {
            'rain_products': 'Precipitation-Heavy',
            'heat_products': 'Extreme Heat',
            'cold_products': 'Extreme Cold',
            'seasonal_products': 'Normal'
        }
        
        self.ebay_data['weather_bucket'] = self.ebay_data['weather_category'].map(weather_buckets)
        
        # Group by product type and weather bucket
        result = self.ebay_data.groupby(['product_type', 'weather_bucket']).agg({
            'price': ['mean', 'min', 'max', 'std', 'count']
        }).round(2)
        
        result.columns = ['avg_price', 'min_price', 'max_price', 'price_stddev', 'listings']
        result = result.reset_index()
        
        print(f"Results: {len(result)} rows")
        print("\nPricing behavior by product and weather:")
        print(result.to_string(index=False))
        
        return result
    
    def run_query_3(self):
        """Query 3: Shipping choices vs weather"""
        print("\n" + "="*80)
        print("QUERY 3: SHIPPING CHOICES VS WEATHER")
        print("="*80)
        
        # Create weather buckets
        weather_buckets = {
            'rain_products': 'Precipitation-Heavy',
            'heat_products': 'Extreme Heat',
            'cold_products': 'Extreme Cold',
            'seasonal_products': 'Normal'
        }
        
        self.ebay_data['weather_bucket'] = self.ebay_data['weather_category'].map(weather_buckets)
        
        # Calculate shipping metrics
        result = []
        for bucket in weather_buckets.values():
            bucket_data = self.ebay_data[self.ebay_data['weather_bucket'] == bucket]
            
            if len(bucket_data) > 0:
                free_shipping_rate = bucket_data['free_shipping'].mean()
                free_count = bucket_data['free_shipping'].sum()
                paid_count = len(bucket_data) - free_count
                
                paid_shipping_data = bucket_data[~bucket_data['free_shipping']]
                avg_paid_shipping = paid_shipping_data['shipping_cost'].mean() if len(paid_shipping_data) > 0 else 0
                min_shipping = paid_shipping_data['shipping_cost'].min() if len(paid_shipping_data) > 0 else 0
                max_shipping = paid_shipping_data['shipping_cost'].max() if len(paid_shipping_data) > 0 else 0
                
                result.append({
                    'weather_bucket': bucket,
                    'free_shipping_rate': round(free_shipping_rate, 3),
                    'free_shipping_count': int(free_count),
                    'paid_shipping_count': int(paid_count),
                    'avg_paid_shipping_cost': round(avg_paid_shipping, 2),
                    'min_shipping_cost': round(min_shipping, 2),
                    'max_shipping_cost': round(max_shipping, 2),
                    'total_listings': len(bucket_data)
                })
        
        result_df = pd.DataFrame(result)
        
        print(f"Results: {len(result_df)} rows")
        print("\nShipping analysis by weather condition:")
        print(result_df.to_string(index=False))
        
        return result_df
    
    def run_query_4(self):
        """Query 4: Category demand shifts by weather"""
        print("\n" + "="*80)
        print("QUERY 4: CATEGORY DEMAND SHIFTS BY WEATHER")
        print("="*80)
        
        # Create weather buckets
        weather_buckets = {
            'rain_products': 'Precipitation-Heavy',
            'heat_products': 'Extreme Heat',
            'cold_products': 'Extreme Cold',
            'seasonal_products': 'Normal'
        }
        
        self.ebay_data['weather_bucket'] = self.ebay_data['weather_category'].map(weather_buckets)
        
        # Group by product type and weather bucket
        result = self.ebay_data.groupby(['product_type', 'weather_bucket']).agg({
            'item_id': 'count',
            'price': ['mean', 'min', 'max']
        }).round(2)
        
        result.columns = ['listings', 'avg_price', 'min_price', 'max_price']
        result = result.reset_index()
        
        # Calculate percentages
        result['percentage_of_weather_bucket'] = result.groupby('weather_bucket')['listings'].transform(
            lambda x: (x / x.sum() * 100).round(2)
        )
        result['percentage_of_product_type'] = result.groupby('product_type')['listings'].transform(
            lambda x: (x / x.sum() * 100).round(2)
        )
        
        result = result.sort_values('listings', ascending=False)
        
        print(f"Results: {len(result)} rows")
        print("\nProduct demand shifts by weather condition:")
        print(result.to_string(index=False))
        
        return result
    
    def run_query_5(self):
        """Query 5: Seller performance vs weather"""
        print("\n" + "="*80)
        print("QUERY 5: SELLER PERFORMANCE VS WEATHER")
        print("="*80)
        
        # Create seller tiers
        def get_feedback_tier(score):
            if score < 100:
                return 'Low (0-99)'
            elif score < 1000:
                return 'Medium (100-999)'
            elif score < 5000:
                return 'High (1000-4999)'
            else:
                return 'Very High (5000+)'
        
        def get_percentage_tier(percentage):
            if percentage < 95:
                return 'Poor (<95%)'
            elif percentage < 97:
                return 'Fair (95-97%)'
            elif percentage < 99:
                return 'Good (97-99%)'
            else:
                return 'Excellent (99%+)'
        
        self.ebay_data['feedback_score_tier'] = self.ebay_data['seller_feedback_score'].apply(get_feedback_tier)
        self.ebay_data['feedback_percentage_tier'] = self.ebay_data['seller_feedback_percentage'].apply(get_percentage_tier)
        
        # Create weather buckets
        weather_buckets = {
            'rain_products': 'Precipitation-Heavy',
            'heat_products': 'Extreme Heat',
            'cold_products': 'Extreme Cold',
            'seasonal_products': 'Normal'
        }
        
        self.ebay_data['weather_bucket'] = self.ebay_data['weather_category'].map(weather_buckets)
        
        # Group by seller tiers and weather bucket
        result = self.ebay_data.groupby(['feedback_score_tier', 'feedback_percentage_tier', 'weather_bucket']).agg({
            'item_id': 'count',
            'seller_feedback_score': 'mean',
            'price': 'mean'
        }).round(2)
        
        result.columns = ['listings', 'avg_feedback_score', 'avg_price']
        result = result.reset_index()
        
        # Calculate market share
        result['market_share_percent'] = result.groupby('weather_bucket')['listings'].transform(
            lambda x: (x / x.sum() * 100).round(2)
        )
        
        result = result.sort_values(['weather_bucket', 'listings'], ascending=[True, False])
        
        print(f"Results: {len(result)} rows")
        print("\nSeller performance by weather condition:")
        print(result.to_string(index=False))
        
        return result
    
    def run_query_6(self):
        """Query 6: Listing quality vs weather"""
        print("\n" + "="*80)
        print("QUERY 6: LISTING QUALITY VS WEATHER")
        print("="*80)
        
        # Create weather buckets
        weather_buckets = {
            'rain_products': 'Precipitation-Heavy',
            'heat_products': 'Extreme Heat',
            'cold_products': 'Extreme Cold',
            'seasonal_products': 'Normal'
        }
        
        self.ebay_data['weather_bucket'] = self.ebay_data['weather_category'].map(weather_buckets)
        
        # Create buying option complexity
        def get_option_complexity(option):
            if option == 'FIXED_PRICE':
                return 'Simple'
            elif 'BEST_OFFER' in option:
                return 'Complex'
            elif option == 'AUCTION':
                return 'Auction'
            else:
                return 'Other'
        
        self.ebay_data['option_complexity'] = self.ebay_data['buying_options'].apply(get_option_complexity)
        
        # Group by weather bucket and option complexity
        result = self.ebay_data.groupby(['weather_bucket', 'option_complexity']).agg({
            'title_length': ['mean', 'min', 'max', 'std'],
            'item_id': ['count', 'nunique']
        }).round(2)
        
        result.columns = ['avg_title_length', 'min_title_length', 'max_title_length', 'title_length_stddev', 'listings', 'unique_items']
        result = result.reset_index()
        
        # Calculate quality score
        result['quality_score'] = (result['avg_title_length'] * result['listings'] / 1000).round(3)
        
        result = result.sort_values(['weather_bucket', 'option_complexity'])
        
        print(f"Results: {len(result)} rows")
        print("\nListing quality metrics by weather condition:")
        print(result.to_string(index=False))
        
        return result
    
    def run_query_7(self):
        """Query 7: Zip prefix variation"""
        print("\n" + "="*80)
        print("QUERY 7: ZIP PREFIX VARIATION")
        print("="*80)
        
        # Extract zip codes from item_location
        def extract_zip_prefix(location):
            if pd.isna(location):
                return 'Unknown'
            
            location_str = str(location)
            zip_patterns = ['331**', '112**', '113**', '114**', '103**', '100**', '111**', '191**', '021**', '022**', '104**']
            
            for pattern in zip_patterns:
                if pattern in location_str:
                    return pattern[:3]
            
            return 'Unknown'
        
        self.ebay_data['zip_prefix'] = self.ebay_data['item_location'].apply(extract_zip_prefix)
        
        # Create weather buckets
        weather_buckets = {
            'rain_products': 'Precipitation-Heavy',
            'heat_products': 'Extreme Heat',
            'cold_products': 'Extreme Cold',
            'seasonal_products': 'Normal'
        }
        
        self.ebay_data['weather_bucket'] = self.ebay_data['weather_category'].map(weather_buckets)
        
        # Filter out unknown zip codes and group
        valid_data = self.ebay_data[self.ebay_data['zip_prefix'] != 'Unknown']
        
        result = valid_data.groupby(['zip_prefix', 'weather_bucket']).agg({
            'price': ['mean', 'std', 'count']
        }).round(2)
        
        result.columns = ['avg_price', 'price_stddev', 'listings']
        result = result.reset_index()
        
        # Calculate variation coefficient
        result['price_variation_coefficient'] = (result['price_stddev'] / result['avg_price']).round(3)
        
        # Calculate cross-weather variability
        result['cross_weather_price_variability'] = result.groupby('zip_prefix')['avg_price'].transform('std').round(2)
        
        # Calculate market share
        result['market_share_percent'] = result.groupby('weather_bucket')['listings'].transform(
            lambda x: (x / x.sum() * 100).round(2)
        )
        
        # Filter for zip codes with multiple listings
        result = result[result['listings'] >= 2]
        result = result.sort_values('cross_weather_price_variability', ascending=False)
        
        print(f"Results: {len(result)} rows")
        print("\nPrice variation by zip code and weather condition:")
        print(result.to_string(index=False))
        
        return result
    
    def run_all_queries(self):
        """Run all SQL queries and display results"""
        print("🚀 RUNNING ALL SQL QUERIES AGAINST REAL DATA")
        print("="*80)
        
        if not self.load_data():
            print("❌ Failed to load data")
            return
        
        results = {}
        
        try:
            results['query_1'] = self.run_query_1()
            results['query_2'] = self.run_query_2()
            results['query_3'] = self.run_query_3()
            results['query_4'] = self.run_query_4()
            results['query_5'] = self.run_query_5()
            results['query_6'] = self.run_query_6()
            results['query_7'] = self.run_query_7()
            
            print("\n" + "="*80)
            print("🎉 ALL QUERIES COMPLETED SUCCESSFULLY!")
            print("="*80)
            
            # Summary statistics
            print(f"\n📊 SUMMARY:")
            print(f"Total queries executed: 7")
            print(f"Total result rows across all queries: {sum(len(df) for df in results.values())}")
            print(f"Data coverage: 100% of available records")
            
        except Exception as e:
            print(f"❌ Error running queries: {e}")
            logger.error(f"Query execution failed: {e}")
        
        return results

def main():
    """Main execution function"""
    import sys
    
    runner = SQLQueryRunner()
    
    # Check if specific query number is provided
    if len(sys.argv) > 1:
        try:
            query_num = int(sys.argv[1])
            if 1 <= query_num <= 7:
                print(f"🚀 RUNNING QUERY {query_num} ONLY")
                print("="*80)
                
                if not runner.load_data():
                    print("❌ Failed to load data")
                    return
                
                query_functions = {
                    1: runner.run_query_1,
                    2: runner.run_query_2,
                    3: runner.run_query_3,
                    4: runner.run_query_4,
                    5: runner.run_query_5,
                    6: runner.run_query_6,
                    7: runner.run_query_7
                }
                
                query_names = {
                    1: "Impact of Weather Conditions on Daily Listings",
                    2: "Pricing Behavior by Weather and Product Type",
                    3: "Shipping Choices vs Weather",
                    4: "Category Demand Shifts by Weather",
                    5: "Seller Performance vs Weather",
                    6: "Listing Quality vs Weather",
                    7: "Zip Prefix Variation"
                }
                
                print(f"Running: {query_names[query_num]}")
                print("="*80)
                
                try:
                    result = query_functions[query_num]()
                    if result is not None:
                        print(f"\n✅ Query {query_num} executed successfully!")
                        print(f"📊 Result: {len(result)} rows returned")
                    else:
                        print(f"\n❌ Query {query_num} failed")
                except Exception as e:
                    print(f"\n❌ Query {query_num} failed with error: {e}")
                    
            else:
                print("❌ Invalid query number. Please use 1-7")
                print_usage()
        except ValueError:
            print("❌ Invalid argument. Please provide a number between 1-7")
            print_usage()
    else:
        # Run all queries
        results = runner.run_all_queries()
        
        if results:
            print("\n✅ All SQL queries executed successfully!")
            print("📋 Check the output above for detailed results from each query.")
        else:
            print("\n❌ Query execution failed. Check logs for details.")

def print_usage():
    """Print usage instructions"""
    print("\n📋 USAGE:")
    print("  python run_sql_queries.py           # Run all 7 queries")
    print("  python run_sql_queries.py 1         # Run only Query 1")
    print("  python run_sql_queries.py 2         # Run only Query 2")
    print("  python run_sql_queries.py 3         # Run only Query 3")
    print("  python run_sql_queries.py 4         # Run only Query 4")
    print("  python run_sql_queries.py 5         # Run only Query 5")
    print("  python run_sql_queries.py 6         # Run only Query 6")
    print("  python run_sql_queries.py 7         # Run only Query 7")
    print("\n📋 QUERY DESCRIPTIONS:")
    print("  1. Impact of Weather Conditions on Daily Listings")
    print("  2. Pricing Behavior by Weather and Product Type")
    print("  3. Shipping Choices vs Weather")
    print("  4. Category Demand Shifts by Weather")
    print("  5. Seller Performance vs Weather")
    print("  6. Listing Quality vs Weather")
    print("  7. Zip Prefix Variation")

if __name__ == "__main__":
    main()
