#!/usr/bin/env python3
"""
Database Setup Script
Creates the star schema tables and loads sample data for testing
"""

import os
import sys
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseSetup:
    def __init__(self, db_config):
        """Initialize with database configuration"""
        self.db_config = db_config
        self.engine = None
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        try:
            # Create SQLAlchemy engine
            connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Database connection established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_schema(self):
        """Create the star schema tables"""
        logger.info("Creating star schema tables...")
        
        schema_sql = """
        -- Drop existing tables if they exist (in reverse dependency order)
        DROP TABLE IF EXISTS fact_listings CASCADE;
        DROP TABLE IF EXISTS dim_weather CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        DROP TABLE IF EXISTS dim_location CASCADE;
        DROP TABLE IF EXISTS dim_product CASCADE;
        DROP TABLE IF EXISTS dim_seller CASCADE;
        DROP TABLE IF EXISTS dim_marketplace CASCADE;
        DROP TABLE IF EXISTS dim_currency CASCADE;
        DROP TABLE IF EXISTS dim_condition CASCADE;
        DROP TABLE IF EXISTS dim_buying_option CASCADE;
        
        -- Create dimensions
        CREATE TABLE dim_date (
          date_key        DATE PRIMARY KEY,
          year            INT NOT NULL,
          quarter         INT NOT NULL,
          month           INT NOT NULL,
          day             INT NOT NULL,
          day_of_week     INT NOT NULL,
          is_weekend      BOOLEAN NOT NULL
        );
        
        CREATE TABLE dim_product (
          product_key     SERIAL PRIMARY KEY,
          product_type    TEXT NOT NULL UNIQUE
        );
        
        CREATE TABLE dim_location (
          location_key    SERIAL PRIMARY KEY,
          location_name   TEXT NOT NULL,
          state_code      TEXT,
          country_code    TEXT,
          zip_prefix      TEXT,
          UNIQUE (location_name, COALESCE(state_code,''), COALESCE(country_code,''), COALESCE(zip_prefix,''))
        );
        
        CREATE TABLE dim_seller (
          seller_key                  SERIAL PRIMARY KEY,
          feedback_score_bin          TEXT,
          feedback_percentage_bin     TEXT
        );
        
        CREATE TABLE dim_marketplace (
          marketplace_key SERIAL PRIMARY KEY,
          marketplace_id  TEXT NOT NULL UNIQUE
        );
        
        CREATE TABLE dim_currency (
          currency_key    SERIAL PRIMARY KEY,
          currency_code   TEXT NOT NULL UNIQUE
        );
        
        CREATE TABLE dim_condition (
          condition_key   SERIAL PRIMARY KEY,
          condition_name  TEXT NOT NULL UNIQUE
        );
        
        CREATE TABLE dim_buying_option (
          buying_option_key SERIAL PRIMARY KEY,
          buying_option     TEXT NOT NULL UNIQUE
        );
        
        -- Create weather table
        CREATE TABLE dim_weather (
          weather_key         SERIAL PRIMARY KEY,
          date                DATE NOT NULL,
          location_key        INT NOT NULL,
          weather_code        INT NOT NULL,
          weather_category    TEXT,
          temperature_2m      NUMERIC,
          relative_humidity_2m INT,
          cloudcover          NUMERIC,
          rain                NUMERIC,
          sunshine_duration   INT NOT NULL,
          wind_speed_10m      NUMERIC,
          UNIQUE (date, location_key)
        );
        
        -- Create fact table
        CREATE TABLE fact_listings (
          listing_key         BIGSERIAL PRIMARY KEY,
          item_id             TEXT NOT NULL,
          collection_ts       TIMESTAMP WITH TIME ZONE NOT NULL,
          date_key            DATE NOT NULL REFERENCES dim_date(date_key),
          product_key         INT NOT NULL REFERENCES dim_product(product_key),
          weather_key         INT REFERENCES dim_weather(weather_key),
          location_key        INT REFERENCES dim_location(location_key),
          seller_key          INT REFERENCES dim_seller(seller_key),
          marketplace_key     INT NOT NULL REFERENCES dim_marketplace(marketplace_key),
          currency_key        INT NOT NULL REFERENCES dim_currency(currency_key),
          condition_key       INT NOT NULL REFERENCES dim_condition(condition_key),
          buying_option_key   INT NOT NULL REFERENCES dim_buying_option(buying_option_key),
          price               NUMERIC NOT NULL,
          shipping_cost       NUMERIC,
          free_shipping       BOOLEAN NOT NULL,
          title_length        INT,
          CONSTRAINT uq_listing_event UNIQUE (item_id, collection_ts)
        );
        
        -- Create indexes for performance
        CREATE INDEX idx_fact_listings_date_location ON fact_listings(date_key, location_key);
        CREATE INDEX idx_weather_date_location ON dim_weather(date, location_key);
        CREATE INDEX idx_location_state_country ON dim_location(state_code, country_code);
        CREATE INDEX idx_fact_listings_collection_ts ON fact_listings(collection_ts);
        CREATE INDEX idx_weather_temperature ON dim_weather(temperature_2m);
        CREATE INDEX idx_weather_rain ON dim_weather(rain);
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(schema_sql))
                conn.commit()
            
            logger.info("Star schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            return False
    
    def load_dimension_data(self):
        """Load dimension tables with sample data"""
        logger.info("Loading dimension data...")
        
        try:
            with self.engine.connect() as conn:
                # Load dim_date (last 365 days)
                date_sql = """
                INSERT INTO dim_date (date_key, year, quarter, month, day, day_of_week, is_weekend)
                SELECT 
                    date_key,
                    EXTRACT(year FROM date_key) as year,
                    EXTRACT(quarter FROM date_key) as quarter,
                    EXTRACT(month FROM date_key) as month,
                    EXTRACT(day FROM date_key) as day,
                    EXTRACT(dow FROM date_key) as day_of_week,
                    EXTRACT(dow FROM date_key) IN (0, 6) as is_weekend
                FROM generate_series(
                    CURRENT_DATE - INTERVAL '365 days',
                    CURRENT_DATE,
                    INTERVAL '1 day'
                ) as date_key
                ON CONFLICT (date_key) DO NOTHING;
                """
                conn.execute(text(date_sql))
                
                # Load dim_product
                products = ['Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors', 'Books', 'Toys', 'Automotive', 'Health & Beauty']
                for product in products:
                    conn.execute(text("INSERT INTO dim_product (product_type) VALUES (:product) ON CONFLICT (product_type) DO NOTHING"), 
                               {"product": product})
                
                # Load dim_location (East Coast cities)
                locations = [
                    ('New York', 'NY', 'US', '100'),
                    ('Boston', 'MA', 'US', '021'),
                    ('Philadelphia', 'PA', 'US', '191'),
                    ('Washington', 'DC', 'US', '200'),
                    ('Atlanta', 'GA', 'US', '303'),
                    ('Miami', 'FL', 'US', '331'),
                    ('Charlotte', 'NC', 'US', '282'),
                    ('Richmond', 'VA', 'US', '232')
                ]
                
                for location, state, country, zip_prefix in locations:
                    conn.execute(text("""
                        INSERT INTO dim_location (location_name, state_code, country_code, zip_prefix)
                        VALUES (:location, :state, :country, :zip_prefix)
                        ON CONFLICT (location_name, state_code, country_code, zip_prefix) DO NOTHING
                    """), {"location": location, "state": state, "country": country, "zip_prefix": zip_prefix})
                
                # Load dim_seller (feedback bins)
                seller_bins = [
                    ('0-99', '<95'),
                    ('100-999', '95-97'),
                    ('1000-4999', '97-99'),
                    ('5000+', '99-100')
                ]
                
                for score_bin, percentage_bin in seller_bins:
                    conn.execute(text("""
                        INSERT INTO dim_seller (feedback_score_bin, feedback_percentage_bin)
                        VALUES (:score_bin, :percentage_bin)
                    """), {"score_bin": score_bin, "percentage_bin": percentage_bin})
                
                # Load other dimensions
                conn.execute(text("INSERT INTO dim_marketplace (marketplace_id) VALUES ('ebay_us') ON CONFLICT (marketplace_id) DO NOTHING"))
                conn.execute(text("INSERT INTO dim_currency (currency_code) VALUES ('USD') ON CONFLICT (currency_code) DO NOTHING"))
                
                conditions = ['New', 'Used', 'Refurbished']
                for condition in conditions:
                    conn.execute(text("INSERT INTO dim_condition (condition_name) VALUES (:condition) ON CONFLICT (condition_name) DO NOTHING"), 
                               {"condition": condition})
                
                buying_options = ['Buy It Now', 'Auction', 'Best Offer']
                for option in buying_options:
                    conn.execute(text("INSERT INTO dim_buying_option (buying_option) VALUES (:option) ON CONFLICT (buying_option) DO NOTHING"), 
                               {"option": option})
                
                conn.commit()
            
            logger.info("Dimension data loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load dimension data: {e}")
            return False
    
    def load_weather_data(self):
        """Load weather data from CSV files"""
        logger.info("Loading weather data...")
        
        try:
            weather_files = list(Path("weather_data").glob("*.csv"))
            
            if not weather_files:
                logger.warning("No weather CSV files found in weather_data/ directory")
                return False
            
            for file_path in weather_files:
                logger.info(f"Loading weather data from {file_path}")
                
                df = pd.read_csv(file_path)
                
                # Standardize column names
                df.columns = df.columns.str.lower().str.replace(' ', '_')
                
                # Ensure required columns exist
                required_cols = ['date', 'location', 'temperature_2m', 'rain', 'sunshine_duration']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    logger.warning(f"Missing columns in {file_path}: {missing_cols}")
                    continue
                
                # Clean and prepare data
                df['date'] = pd.to_datetime(df['date']).dt.date
                df = df.dropna(subset=['date', 'location'])
                
                # Map location to location_key
                location_map = {}
                with self.engine.connect() as conn:
                    result = conn.execute(text("SELECT location_key, location_name FROM dim_location"))
                    for row in result:
                        location_map[row[1]] = row[0]
                
                # Load weather data
                for _, row in df.iterrows():
                    location_key = location_map.get(row['location'])
                    if location_key:
                        conn.execute(text("""
                            INSERT INTO dim_weather (date, location_key, weather_code, weather_category, 
                                                   temperature_2m, relative_humidity_2m, cloudcover, rain, 
                                                   sunshine_duration, wind_speed_10m)
                            VALUES (:date, :location_key, :weather_code, :weather_category,
                                   :temperature_2m, :relative_humidity_2m, :cloudcover, :rain,
                                   :sunshine_duration, :wind_speed_10m)
                            ON CONFLICT (date, location_key) DO UPDATE SET
                                temperature_2m = EXCLUDED.temperature_2m,
                                rain = EXCLUDED.rain,
                                sunshine_duration = EXCLUDED.sunshine_duration
                        """), {
                            "date": row['date'],
                            "location_key": location_key,
                            "weather_code": row.get('weather_code', 0),
                            "weather_category": row.get('weather_category'),
                            "temperature_2m": row.get('temperature_2m'),
                            "relative_humidity_2m": row.get('relative_humidity_2m'),
                            "cloudcover": row.get('cloudcover'),
                            "rain": row.get('rain', 0),
                            "sunshine_duration": row.get('sunshine_duration', 0),
                            "wind_speed_10m": row.get('wind_speed_10m')
                        })
                
                conn.commit()
            
            logger.info("Weather data loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load weather data: {e}")
            return False
    
    def load_ebay_data(self):
        """Load eBay listing data from CSV files"""
        logger.info("Loading eBay listing data...")
        
        try:
            ebay_files = list(Path("ebay_data").glob("*.csv"))
            
            if not ebay_files:
                logger.warning("No eBay CSV files found in ebay_data/ directory")
                return False
            
            for file_path in ebay_files:
                logger.info(f"Loading eBay data from {file_path}")
                
                df = pd.read_csv(file_path)
                
                # Standardize column names
                df.columns = df.columns.str.lower().str.replace(' ', '_')
                
                # Ensure required columns exist
                required_cols = ['collection_timestamp', 'product_type', 'price', 'item_id']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    logger.warning(f"Missing columns in {file_path}: {missing_cols}")
                    continue
                
                # Clean and prepare data
                df['collection_timestamp'] = pd.to_datetime(df['collection_timestamp'])
                df['date_key'] = df['collection_timestamp'].dt.date
                df = df.dropna(subset=['collection_timestamp', 'product_type', 'price', 'item_id'])
                
                # Get dimension keys
                with self.engine.connect() as conn:
                    # Get product keys
                    product_map = {}
                    result = conn.execute(text("SELECT product_key, product_type FROM dim_product"))
                    for row in result:
                        product_map[row[1]] = row[0]
                    
                    # Get location keys
                    location_map = {}
                    result = conn.execute(text("SELECT location_key, location_name FROM dim_location"))
                    for row in result:
                        location_map[row[1]] = row[0]
                    
                    # Get other dimension keys
                    marketplace_key = conn.execute(text("SELECT marketplace_key FROM dim_marketplace WHERE marketplace_id = 'ebay_us'")).fetchone()[0]
                    currency_key = conn.execute(text("SELECT currency_key FROM dim_currency WHERE currency_code = 'USD'")).fetchone()[0]
                    
                    condition_map = {}
                    result = conn.execute(text("SELECT condition_key, condition_name FROM dim_condition"))
                    for row in result:
                        condition_map[row[1]] = row[0]
                    
                    buying_option_map = {}
                    result = conn.execute(text("SELECT buying_option_key, buying_option FROM dim_buying_option"))
                    for row in result:
                        buying_option_map[row[1]] = row[0]
                    
                    seller_key = conn.execute(text("SELECT seller_key FROM dim_seller LIMIT 1")).fetchone()[0]
                
                # Load eBay data
                for _, row in df.iterrows():
                    product_key = product_map.get(row['product_type'])
                    location_key = location_map.get(row.get('item_location', 'New York'))  # Default to New York
                    condition_key = condition_map.get(row.get('condition', 'New'))
                    buying_option_key = buying_option_map.get(row.get('buying_options', 'Buy It Now'))
                    
                    if product_key and location_key and condition_key and buying_option_key:
                        conn.execute(text("""
                            INSERT INTO fact_listings (item_id, collection_ts, date_key, product_key, location_key,
                                                     seller_key, marketplace_key, currency_key, condition_key,
                                                     buying_option_key, price, shipping_cost, free_shipping, title_length)
                            VALUES (:item_id, :collection_ts, :date_key, :product_key, :location_key,
                                   :seller_key, :marketplace_key, :currency_key, :condition_key,
                                   :buying_option_key, :price, :shipping_cost, :free_shipping, :title_length)
                            ON CONFLICT (item_id, collection_ts) DO NOTHING
                        """), {
                            "item_id": row['item_id'],
                            "collection_ts": row['collection_timestamp'],
                            "date_key": row['date_key'],
                            "product_key": product_key,
                            "location_key": location_key,
                            "seller_key": seller_key,
                            "marketplace_key": marketplace_key,
                            "currency_key": currency_key,
                            "condition_key": condition_key,
                            "buying_option_key": buying_option_key,
                            "price": row['price'],
                            "shipping_cost": row.get('shipping_cost'),
                            "free_shipping": row.get('free_shipping', False),
                            "title_length": row.get('title_length')
                        })
                
                conn.commit()
            
            logger.info("eBay data loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load eBay data: {e}")
            return False
    
    def setup_complete(self):
        """Complete database setup"""
        logger.info("Starting complete database setup...")
        
        if not self.connect():
            return False
        
        try:
            # Create schema
            if not self.create_schema():
                return False
            
            # Load dimension data
            if not self.load_dimension_data():
                return False
            
            # Load weather data
            if not self.load_weather_data():
                return False
            
            # Load eBay data
            if not self.load_ebay_data():
                return False
            
            logger.info("🎉 Database setup completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False

def main():
    """Main execution function"""
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'weather_ebay_analytics'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'password'),
        'port': os.getenv('DB_PORT', '5432')
    }
    
    logger.info("Starting Database Setup")
    logger.info(f"Database: {db_config['database']}@{db_config['host']}:{db_config['port']}")
    
    # Create setup instance
    setup = DatabaseSetup(db_config)
    
    # Run setup
    success = setup.setup_complete()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
