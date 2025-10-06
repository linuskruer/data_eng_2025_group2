import os
import requests
import time
import json
import base64
import csv
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote
import re

class CombinedEbayWeatherAnalyzer:
    def __init__(self, base_directory=None):
        # Initialize paths
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(self.current_dir, "config.json")
        self.token_file = os.path.join(self.current_dir, "ebay_token.json")
        
        # Set up data directory - saves to "ebay_data" folder in project root
        if base_directory is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            base_directory = os.path.join(project_root, "ebay_data")
        
        os.makedirs(base_directory, exist_ok=True)
        self.base_directory = base_directory
        self.data_file = os.path.join(base_directory, "east_coast_weather_data.csv")
        
        # Load configuration
        self.config = self._load_config()
        self.access_token = None
        self.token_expiry = None
        self.refresh_token = None
        
        # ONLY THESE 5 CITIES - Updated ZIP prefixes
        self.city_zip_prefixes = {
            "new_york": ["100", "101", "102", "103", "104", "111", "112", "113", "114", "116"],
            "boston": ["021", "022", "023"],
            "washington_dc": ["200", "202", "203", "204"],
            "miami": ["331", "332", "333"],
            "jacksonville": ["320", "322", "322"]  # Jacksonville, FL ZIP prefixes
        }
        
        self._initialize_token()
    
    def _load_config(self):
        """Load configuration from config.json"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ config.json not found at: {self.config_path}")
            raise
    
    def _load_token_data(self):
        """Load token data from file"""
        try:
            if not os.path.exists(self.token_file):
                return None
                
            with open(self.token_file, 'r') as f:
                token_data = json.load(f)
            
            if 'expiry_time' in token_data:
                expiry_time = datetime.fromisoformat(token_data['expiry_time'])
                if datetime.now() < expiry_time:
                    return token_data
                else:
                    print("🔄 Token expired, refreshing...")
                    return None
            return token_data
                
        except Exception as e:
            print(f"❌ Error loading token: {e}")
            return None
    
    def _save_token_data(self, token_response):
        """Save token data to file"""
        expiry_time = datetime.now() + timedelta(seconds=token_response['expires_in'] - 300)
        
        token_data = {
            'access_token': token_response['access_token'],
            'refresh_token': token_response['refresh_token'],
            'expiry_time': expiry_time.isoformat(),
            'last_updated': datetime.now().isoformat()
        }
        
        with open(self.token_file, 'w') as f:
            json.dump(token_data, f, indent=2)
        
        print("✅ Token data saved successfully")
    
    def _refresh_access_token(self):
        """Automatic access token refresh"""
        try:
            token_data = self._load_token_data()
            if not token_data or 'refresh_token' not in token_data:
                print("❌ No refresh token available - need manual setup")
                return None
            
            refresh_token = token_data['refresh_token']
            print(f"🔄 Refreshing token with refresh_token: {refresh_token[:10]}...")
            
            token_url = "https://api.ebay.com/identity/v1/oauth2/token"
            
            credentials = f"{self.config['client_id']}:{self.config['client_secret']}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {encoded_credentials}"
            }
            
            data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "scope": "https://api.ebay.com/oauth/api_scope"
            }
            
            print("🔄 Sending refresh request...")
            response = requests.post(token_url, headers=headers, data=data)
            
            print(f"📡 Refresh response: {response.status_code}")
            
            if response.status_code == 200:
                token_response = response.json()
                print("✅ Refresh successful!")
                self._save_token_data(token_response)
                
                new_token_data = self._load_token_data()
                if new_token_data and 'access_token' in new_token_data:
                    print("✅ New access token loaded successfully")
                    return new_token_data['access_token']
                else:
                    print("❌ Failed to load new access token after refresh")
                    return None
            else:
                print(f"❌ Token refresh failed: {response.status_code}")
                print("Response:", response.text)
                return None
                
        except Exception as e:
            print(f"❌ Error refreshing token: {e}")
            return None
    
    def _get_new_token_via_auth_code(self, auth_code):
        """Get new token with Auth Code"""
        decoded_code = unquote(auth_code)
        print(f"Using auth code: {decoded_code}")
        
        token_url = "https://api.ebay.com/identity/v1/oauth2/token"
        
        credentials = f"{self.config['client_id']}:{self.config['client_secret']}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {encoded_credentials}"
        }
        
        data = {
            "grant_type": "authorization_code",
            "code": decoded_code,
            "redirect_uri": self.config['runame']
        }
        
        print("🔄 Exchanging auth code for token...")
        response = requests.post(token_url, headers=headers, data=data)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            token_response = response.json()
            self._save_token_data(token_response)
            return token_response['access_token']
        else:
            print(f"❌ Token exchange failed: {response.status_code}")
            print("Response:", response.json())
            return None
    
    def _initialize_token(self):
        """Initialize or load token"""
        token_data = self._load_token_data()
        
        if token_data:
            self.access_token = token_data['access_token']
            self.refresh_token = token_data['refresh_token']
            if 'expiry_time' in token_data:
                self.token_expiry = datetime.fromisoformat(token_data['expiry_time'])
            print("✅ Token loaded from file")
        else:
            print("❌ No valid token available.")
    
    def get_auth_url(self):
        """Return Auth URL"""
        auth_url = (f"https://auth.ebay.com/oauth2/authorize?"
                   f"client_id={self.config['client_id']}&"
                   f"redirect_uri={self.config['runame']}&"
                   f"response_type=code&"
                   f"scope=https://api.ebay.com/oauth/api_scope")
        return auth_url
    
    def setup_initial_token(self, auth_code):
        """Manual initial token setup"""
        token = self._get_new_token_via_auth_code(auth_code)
        if token:
            self.access_token = token
            print("✅ Initial token setup successful!")
            return True
        return False
    
    def _ensure_valid_token(self):
        """Ensure token is valid"""
        if not self.access_token:
            token_data = self._load_token_data()
            if token_data:
                self.access_token = token_data['access_token']
                if 'expiry_time' in token_data:
                    self.token_expiry = datetime.fromisoformat(token_data['expiry_time'])
            else:
                raise Exception("No valid token available. Please run setup first.")
        
        if self.token_expiry and datetime.now() > self.token_expiry - timedelta(minutes=10):
            print("🔄 Token expiring soon, refreshing...")
            new_token = self._refresh_access_token()
            if new_token:
                self.access_token = new_token
    
    def search_products(self, keyword, category_id=None, limit=50, location_filter=None, offset=0):
        """Search for products with automatic token management"""
        self._ensure_valid_token()
        
        if not self.access_token:
            print("❌ No access token available")
            return None
            
        url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }
        
        filters = ['buyingOptions:{FIXED_PRICE}']
        
        if location_filter:
            filters.append(f'itemLocation:{location_filter}')
        
        params = {
            'q': keyword,
            'limit': min(limit, 200),
            'offset': offset,
            'filter': ','.join(filters),
        }
        
        if category_id:
            params['category_ids'] = category_id
            
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print("❌ Token invalid, attempting refresh...")
                new_token = self._refresh_access_token()
                if new_token:
                    self.access_token = new_token
                    headers['Authorization'] = f'Bearer {self.access_token}'
                    response = requests.get(url, headers=headers, params=params)
                    if response.status_code == 200:
                        return response.json()
                return None
            else:
                print(f"❌ Search error for '{keyword}': {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Request failed: {e}")
            return None
        
    def test_connection(self):
        """Test if the API connection works"""
        try:
            self._ensure_valid_token()
            if not self.access_token:
                return False
            result = self.search_products("umbrella", limit=1)
            return result is not None
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False

    def _get_east_coast_time(self):
        """Get current East Coast USA time (EDT/EST)."""
        utc_now = datetime.now(timezone.utc)
        east_coast_tz = timezone(timedelta(hours=-4))
        return utc_now.astimezone(east_coast_tz)

    def get_east_coast_weather_products(self):
        """East Coast specific products for weather correlation"""
        return {
            # 'rain_products': ['umbrella', 'rain jacket'],
            # 'heat_products': ['air conditioner', 'sunscreen'],
            # 'cold_products': ['winter coat', 'thermal gloves'],
            # 'seasonal_products': ['beach towel', 'snow shovel', 'outdoor furniture']
            'rain_products': ['umbrella', 'rain jacket', 'rain boots', 'waterproof pants', 
                              'rain cover for backpack', 'waterproof phone case', 'quick-dry towel', 
                              'moisture-wicking socks', 'anti-fog spray for glasses', 'trench coat', 'waterproof hat',
                                'shoe waterproofing spray', 'drying rack for wet clothes', 'portable rain poncho', 'gaiters'],
            'heat_products': ['air conditioner', 'sunscreen', 'portable fan', 'cooling towel', 'sun hat', 
                              'sunglasses', 'UV-protective clothing', 'insulated water bottle', 
                              'misting spray bottle', 'lightweight linen clothing', 'cooling neck gaiter', 
                              'beach umbrella', 'solar charger', 'aloe vera gel', 'heat-reflective window film'],
            'cold_products': ['winter coat', 'thermal gloves', 'wool beanie', 'scarf', 'thermal underwear', 
                              'insulated boots', 'hand warmers', 'fleece jacket', 'thermal socks', 'ear muffs', 
                              'balaclava', 'windproof jacket', 'heated vest', 'ski pants', 'lip balm with SPF'],
            'seasonal_products': ['beach towel', 'snow shovel', 'outdoor furniture', 'gardening tools', 
                                  'swimming pool', 'christmas decorations', 'halloween costumes', 'spring flower seeds', 
                                  'bird feeder', 'picnic basket', 'camping gear', 'holiday lights', 'lawn mower', 'fall leaf rake', 
                                  'beach chairs']
        }

    def _search_with_pagination(self, query, max_items=1000, location_filter=None):
        all_items = []
        offset = 0
        limit = 200  # Maximum das eBay erlaubt

        while len(all_items) < max_items:
            result = self.search_products(
                query,
                limit=limit,  # 200 verwenden
                offset=offset,
                location_filter=location_filter
            )

            if not result or "itemSummaries" not in result:
                break

            items = result["itemSummaries"]
            all_items.extend(items)
            
            print(f"📄 Page {offset//200 + 1}: {len(items)} items (Total: {len(all_items)})")

            # Stoppe wenn keine weiteren Items oder Limit erreicht
            if len(items) < limit or len(all_items) >= max_items:
                break

            offset += limit
            time.sleep(1.5)  # Rate Limit einhalten

        return all_items

    def _is_allowed_city_zip(self, postal_code):
        """Check if postal code belongs to allowed cities"""
        if not postal_code:
            return False
        prefix = str(postal_code)[:3]
        for city_prefixes in self.city_zip_prefixes.values():
            if prefix in city_prefixes:
                return True
        return False

    def _extract_complete_item_data(self, item, weather_category, product_type):
        """Extract ONLY the specified columns from eBay API response"""
        price_value = item.get('price', {}).get('value')
        if not price_value:
            return None

        try:
            price = float(price_value)
        except (TypeError, ValueError):
            return None

        title = item.get("title", "").lower()
        if product_type.lower() not in title:
            return None

        east_coast_time = self._get_east_coast_time()

        # Extract location data for filtering
        item_location = item.get('itemLocation', 'Unknown')
        seller_info = item.get('seller', {})
        seller_location = seller_info.get('location', 'Unknown')
        zip_prefix = None

        # Extract ZIP prefix for filtering
        if item_location and isinstance(item_location, dict):
            postal_code = item_location.get('postalCode', '')
            zip_prefix = postal_code[:3] if postal_code else None
        elif item_location and isinstance(item_location, str):
            # Try to extract ZIP from string representation
            zip_match = re.search(r"'postalCode':\s*'([0-9]{3})", item_location)
            if zip_match:
                zip_prefix = zip_match.group(1)

        # Skip if not allowed city
        if not self._is_allowed_city_zip(zip_prefix):
            return None

        # Shipping info
        shipping_options = item.get('shippingOptions', [])
        shipping_cost = 0.0
        free_shipping = False
        if shipping_options:
            shipping_cost_value = shipping_options[0].get('shippingCost', {}).get('value', 0)
            try:
                shipping_cost = float(shipping_cost_value)
                free_shipping = shipping_cost == 0
            except (TypeError, ValueError):
                shipping_cost = 0.0
                free_shipping = True

        # Build data record with ONLY the specified columns
        data_record = {
            # Collection info
            'collection_timestamp': east_coast_time.isoformat(),
            'timezone': 'EDT',
            
            # Weather/product category
            'weather_category': weather_category,
            'product_type': product_type,
            
            # Price info
            'price': price,
            'currency': item.get('price', {}).get('currency', 'USD'),
            
            # Seller info
            'seller_feedback_percentage': float(seller_info.get('feedbackPercentage', 0)) if seller_info.get('feedbackPercentage') else 0,
            'seller_feedback_score': int(seller_info.get('feedbackScore', 0)) if seller_info.get('feedbackScore') else 0,
            
            # Location info - KEEP RAW FORMAT
            'item_location': str(item_location),  # Convert dict to string for CSV
            'seller_location': seller_location,
            
            # Shipping info
            'shipping_cost': shipping_cost,
            'free_shipping': free_shipping,
            
            # Product details
            'condition': item.get('condition', ''),
            'buying_options': ','.join(item.get('buyingOptions', [])),
            
            # Title info
            'title_length': len(item.get('title', '')),
            
            # IDs
            'item_id': item.get('itemId'),
            'marketplace_id': 'EBAY_US'
        }

        return data_record

    def collect_east_coast_data(self, items_per_product=1000):
        """Collect data for SPECIFIC 5 East Coast cities only"""
        if not self.access_token:
            return []

        weather_products = self.get_east_coast_weather_products()
        all_data = []

        print(f"🎯 Targeting ONLY 5 cities: {', '.join(self.city_zip_prefixes.keys())}")

        for weather_category, products in weather_products.items():
            print(f"🌤️ Collecting {weather_category} data from 5 target cities...")

            for product in products:
                for city_name in self.city_zip_prefixes.keys():
                    print(f"   🔍 Searching '{product}' in {city_name}...")
                    items = self._search_with_pagination(
                        product, 
                        max_items=items_per_product, 
                        location_filter=city_name
                    )

                    items_collected = 0
                    for item in items:
                        item_data = self._extract_complete_item_data(item, weather_category, product)
                        if item_data:
                            all_data.append(item_data)
                            items_collected += 1

                    print(f"   ✅ {product} ({city_name}): {items_collected} valid items")
                    time.sleep(1)  # Brief pause between city searches

        return all_data

    def save_data(self, new_data, append=True):
        """Save data to CSV file with proper error handling"""
        # Check if csv module is available
        try:
            import csv
        except ImportError:
            print("❌ CSV module not available")
            return None
            
        if not new_data:
            print("📭 No data to save")
            return None

        existing_ids = self._get_existing_ids()
        filtered_data = []
        for row in new_data:
            item_id = row.get("item_id")
            if not item_id:
                continue
            if item_id not in existing_ids:
                filtered_data.append(row)
                existing_ids.add(item_id)

        if not filtered_data:
            print("📭 No new unique records to save")
            return None

        file_mode = 'a' if append and os.path.exists(self.data_file) else 'w'
        write_header = file_mode == 'w' or not os.path.exists(self.data_file)

        try:
            with open(self.data_file, file_mode, newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=filtered_data[0].keys())
                if write_header:
                    writer.writeheader()
                writer.writerows(filtered_data)

            action = "appended to" if file_mode == 'a' else "saved to"
            print(f"💾 {len(filtered_data)} new unique records {action} {self.data_file}")
            
            return self.data_file
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return None
        
    def _get_existing_ids(self):
        """Load existing IDs from CSV"""
        # Check if csv module is available
        try:
            import csv
        except ImportError:
            print("❌ CSV module not available")
            return set()
            
        existing_ids = set()
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        item_id = row.get("item_id")
                        if item_id and item_id.strip():
                            existing_ids.add(item_id.strip())
                print(f"🔍 Loaded {len(existing_ids)} existing IDs from CSV")
            except Exception as e:
                print(f"⚠️ Could not read existing file for duplicates: {e}")
        return existing_ids

    def get_data_stats(self):
        """Return statistics about collected data"""
        if not os.path.exists(self.data_file):
            return {"total_records": 0, "file_exists": False, "data_file_path": self.data_file}
        
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                records = list(reader)
            
            category_counts = {}
            city_counts = {}
            for record in records:
                category = record.get('weather_category', 'UNKNOWN')
                category_counts[category] = category_counts.get(category, 0) + 1
                
                city = record.get('item_location', 'Unknown')
                if city:
                    city_counts[city] = city_counts.get(city, 0) + 1
            
            return {
                "total_records": len(records),
                "file_exists": True,
                "category_distribution": category_counts,
                "city_distribution": city_counts,
                "data_file_path": self.data_file  # This line ensures the key always exists
            }
            
        except Exception as e:
            print(f"❌ Error reading statistics: {e}")
            return {"total_records": 0, "file_exists": False, "data_file_path": self.data_file}

    def simple_auto_collector(self):
        """Automatic collector for 5 specific East Coast cities"""
        collection_count = 0

        print("🚀 Starting automatic data collection...")
        print("🎯 TARGET CITIES: New York, Boston, Washington DC, Miami, Jacksonville")
        print("💾 SAVE LOCATION:", self.data_file)
        print("📅 Collection runs every ten minutes")
        print("⏹️  Press Ctrl+C to stop")

        try:
            while True:
                collection_count += 1
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                print(f"\n{'='*60}")
                print(f"🕐 COLLECTION #{collection_count} - {current_time}")
                print(f"{'='*60}")

                if not self.test_connection():
                    print("❌ API connection failed. Waiting 5 minutes...")
                    time.sleep(300)
                    continue

                print("🌊 Collecting data from 5 target cities...")
                new_data = self.collect_east_coast_data(items_per_product=200)

                if new_data:
                    saved_file = self.save_data(new_data, append=True)
                    stats = self.get_data_stats()
                    print(f"✅ {len(new_data)} new records collected")
                    print(f"📊 Total in database: {stats['total_records']} records")
                    
                    # Safe way to print file path
                    file_path = stats.get('data_file_path', saved_file or self.data_file)
                    print(f"📁 Saved to: {file_path}")
                    
                    if 'city_distribution' in stats:
                        print("🏙️ City distribution:")
                        for city, count in stats['city_distribution'].items():
                            print(f"   {city}: {count} records")
                else:
                    print("❌ No new data collected.")

                # Wait 10 minutes
                print(f"\n⏰ Waiting 10 minutes until next collection...")
                for i in range(600, 0, -60):
                    minutes = i // 60
                    print(f"   {minutes} minutes remaining...", end='\r')
                    time.sleep(60)

        except KeyboardInterrupt:
            stats = self.get_data_stats()
            print(f"\n\n🛑 Collection stopped after {collection_count} runs")
            print(f"📈 Final statistics: {stats['total_records']} records")
            file_path = stats.get('data_file_path', self.data_file)
            print(f"💾 Data saved to: {file_path}")


def main():
    """Main function to run the combined analyzer"""
    analyzer = CombinedEbayWeatherAnalyzer()
    
    # Show save location
    print(f"💾 Data will be saved to: {analyzer.data_file}")
    
    if not analyzer.test_connection():
        print("❌ API connection failed. Please check your token setup.")
        return
    
    # Start automatic collection
    analyzer.simple_auto_collector()


if __name__ == "__main__":
    main()