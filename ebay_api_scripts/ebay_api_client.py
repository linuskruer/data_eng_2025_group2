import os
import requests
import time
import json
import base64
from datetime import datetime, timedelta
from urllib.parse import unquote

class EbayAPIClient:
    def __init__(self):
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(self.current_dir, "config.json")
        self.token_file = os.path.join(self.current_dir, "ebay_token.json")
        
        # Lade Config
        self.config = self._load_config()
        self.access_token = None
        self.token_expiry = None
        self.refresh_token = None
        
        self._initialize_token()
    
    def _load_config(self):
        """Lade Konfiguration aus config.json"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ config.json not found at: {self.config_path}")
            raise
    
    def _load_token_data(self):
        """Lade Token-Daten aus Datei"""
        try:
            if not os.path.exists(self.token_file):
                return None
                
            with open(self.token_file, 'r') as f:
                token_data = json.load(f)
            
            # Prüfe ob Token noch gültig
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
        """Speichere Token-Daten in Datei"""
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
        """Automatisches Refresh des Access Tokens"""
        try:
            token_data = self._load_token_data()
            if not token_data or 'refresh_token' not in token_data:
                print("❌ No refresh token available")
                return None
            
            refresh_token = token_data['refresh_token']
            
            # Token refresh request
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
            
            print("🔄 Refreshing access token...")
            response = requests.post(token_url, headers=headers, data=data)
            
            if response.status_code == 200:
                token_response = response.json()
                self._save_token_data(token_response)
                return token_response['access_token']
            else:
                print(f"❌ Token refresh failed: {response.status_code}")
                print(response.json())
                return None
                
        except Exception as e:
            print(f"❌ Error refreshing token: {e}")
            return None
    
    def _get_new_token_via_auth_code(self, auth_code):
        """Hole neuen Token mit Auth Code"""
        # URL-decode den Auth Code zuerst
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
        """Initialisiere oder lade Token"""
        token_data = self._load_token_data()
        
        if token_data:
            # Verwende vorhandenen Token
            self.access_token = token_data['access_token']
            self.refresh_token = token_data['refresh_token']
            if 'expiry_time' in token_data:
                self.token_expiry = datetime.fromisoformat(token_data['expiry_time'])
            print("✅ Token loaded from file")
            
        else:
            print("❌ No valid token available.")
    
    def get_auth_url(self):
        """Gib die Auth URL zurück"""
        auth_url = (f"https://auth.ebay.com/oauth2/authorize?"
                   f"client_id={self.config['client_id']}&"
                   f"redirect_uri={self.config['runame']}&"
                   f"response_type=code&"
                   f"scope=https://api.ebay.com/oauth/api_scope")
        return auth_url
    
    def setup_initial_token(self, auth_code):
        """Manuelle Einrichtung des initialen Tokens"""
        token = self._get_new_token_via_auth_code(auth_code)
        if token:
            self.access_token = token
            print("✅ Initial token setup successful!")
            return True
        return False
    
    def _ensure_valid_token(self):
        """Stelle sicher, dass Token gültig ist"""
        if not self.access_token:
            token_data = self._load_token_data()
            if token_data:
                self.access_token = token_data['access_token']
                if 'expiry_time' in token_data:
                    self.token_expiry = datetime.fromisoformat(token_data['expiry_time'])
            else:
                raise Exception("No valid token available. Please run setup first.")
        
        # Refresh wenn Token bald abläuft
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
                    # Wiederhole den Request mit neuem Token
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

    # Deine anderen Methoden hier einfügen...
    def search_east_coast_only(self, keyword, limit=200):
        """Search ONLY East Coast items using ZIP code prefixes"""
        east_coast_zip_prefixes = [
            # New York
            '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', 
            '110', '111', '112', '113', '114', '115', '116', '117', '118', '119',
            # Florida
            '331', '330', '333', '334', '335', '336', '337', '338', '339', '320', '321', '322', '323', '324',
            # Massachusetts  
            '021', '022', '023', '024', '025', '026', '027',
            # Georgia
            '303', '300', '301', '302', '304', '305', '306', '307', '308', '309', '310', '311', '312',
            # North Carolina
            '282', '283', '284', '285', '286', '287', '288', '289', '270', '271', '272', '273', '274', '275', '276', '277', '278', '279',
            # New Jersey
            '070', '071', '072', '073', '074', '075', '076', '077', '078', '079', '080', '081', '082', '083', '084', '085', '086', '087', '088', '089',
            # Pennsylvania
            '191', '190', '192', '193', '194', '195', '196', '197', '198', '199', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159',
            # Virginia
            '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236', '237', '238', '239', '240', '241', '242', '243',
            # Maryland
            '212', '214', '207', '208', '209', '210', '211', '215', '216', '217', '218', '219',
            # Connecticut
            '068', '069', '060', '061', '062', '063', '064', '065', '066', '067',
            # Rhode Island
            '029', '028',
            # Delaware
            '199', '198', '197',
            # South Carolina
            '292', '293', '294', '295', '296', '297', '298', '299', '290', '291'
        ]
        
        all_results = []
        
        for zip_prefix in east_coast_zip_prefixes[:15]:  # Limit to 15 ZIP prefixes
            if len(all_results) >= limit:
                break
                
            print(f"   📮 Searching ZIP area {zip_prefix}...")
            result = self.search_products(keyword, limit=15, location_filter=zip_prefix)
            if result and 'itemSummaries' in result:
                all_results.extend(result['itemSummaries'])
                print(f"   ✅ {zip_prefix}: {len(result['itemSummaries'])} items")
            time.sleep(1)
        
        # Remove duplicates
        unique_items = {}
        for item in all_results:
            item_id = item.get('itemId')
            if item_id and item_id not in unique_items:
                unique_items[item_id] = item
        
        unique_results = list(unique_items.values())[:limit]
        print(f"   📊 East Coast only: {len(unique_results)} items")
        
        return {'itemSummaries': unique_results} if unique_results else None

    def search_east_coast_cities(self, keyword, limit=200):
        """Search in major East Coast cities"""
        east_coast_cities = [
            # New York
            'New York, NY', 'Brooklyn, NY', 'Queens, NY', 'Bronx, NY', 'Manhattan, NY',
            'Staten Island, NY', 'Long Island, NY', 'Buffalo, NY', 'Rochester, NY',
            # New Jersey
            'Newark, NJ', 'Jersey City, NJ', 'Paterson, NJ', 'Elizabeth, NJ', 'Edison, NJ',
            # Florida
            'Miami, FL', 'Jacksonville, FL', 'Tampa, FL', 'Orlando, FL', 'St. Petersburg, FL',
            # Georgia
            'Atlanta, GA', 'Augusta, GA', 'Columbus, GA', 'Savannah, GA', 'Athens, GA',
            # Massachusetts
            'Boston, MA', 'Worcester, MA', 'Springfield, MA', 'Cambridge, MA', 'Lowell, MA',
            # Pennsylvania
            'Philadelphia, PA', 'Pittsburgh, PA', 'Allentown, PA', 'Erie, PA', 'Reading, PA',
            # Virginia
            'Virginia Beach, VA', 'Norfolk, VA', 'Chesapeake, VA', 'Richmond, VA', 'Newport News, VA',
            # North Carolina
            'Charlotte, NC', 'Raleigh, NC', 'Greensboro, NC', 'Durham, NC', 'Winston-Salem, NC',
            # Maryland
            'Baltimore, MD', 'Frederick, MD', 'Rockville, MD', 'Gaithersburg, MD', 'Bowie, MD',
            # South Carolina
            'Charleston, SC', 'Columbia, SC', 'North Charleston, SC', 'Mount Pleasant, SC', 'Rock Hill, SC',
            # Connecticut
            'Bridgeport, CT', 'New Haven, CT', 'Stamford, CT', 'Hartford, CT', 'Waterbury, CT'
        ]
        
        all_results = []
        
        for city in east_coast_cities[:12]:  # Limit to 12 cities
            if len(all_results) >= limit:
                break
                
            print(f"   🏙️  Searching {city}...")
            
            result = self.search_products(keyword, limit=20, location_filter=city)
            
            if result and 'itemSummaries' in result:
                all_results.extend(result['itemSummaries'])
                print(f"   ✅ {city}: {len(result['itemSummaries'])} items")
            else:
                print(f"   ❌ No results in {city}")
            
            time.sleep(1)
        
        # Remove duplicates
        unique_items = {}
        for item in all_results:
            item_id = item.get('itemId')
            if item_id and item_id not in unique_items:
                unique_items[item_id] = item
        
        unique_results = list(unique_items.values())
        print(f"   📊 East Coast cities search: {len(unique_results)} items")
        
        return {'itemSummaries': unique_results} if unique_results else None

    def search_east_coast_variations(self, base_keyword, limit=200):
        """Search variations of the same product WITH East Coast filtering"""
        all_results = []
        
        variations = {
            'umbrella': [
                'umbrella', 'rain umbrella', 'windproof umbrella', 
                'compact umbrella', 'golf umbrella', 'travel umbrella',
                'automatic umbrella', 'folding umbrella'
            ],
            'rain jacket': [
                'rain jacket', 'waterproof jacket', 'raincoat', 
                'windbreaker', 'waterproof shell', 'rain parka',
                'waterproof windbreaker'
            ],
            'air conditioner': [
                'air conditioner', 'portable ac', 'window ac', 
                'ac unit', 'air conditioning', 'portable air conditioner',
                'window air conditioner'
            ],
            'sunscreen': [
                'sunscreen', 'sunblock', 'spf 50', 
                'sun cream', 'sunscreen lotion', 'spf 30',
                'mineral sunscreen'
            ],
            'winter coat': [
                'winter coat', 'winter jacket', 'puffer coat', 
                'parka', 'insulated jacket', 'down coat',
                'ski jacket'
            ],
            'thermal gloves': [
                'thermal gloves', 'winter gloves', 'insulated gloves', 
                'ski gloves', 'cold weather gloves', 'waterproof gloves',
                'touchscreen gloves'
            ],
            'beach towel': [
                'beach towel', 'sand free towel', 'quick dry towel', 
                'oversized towel', 'beach blanket', 'microfiber towel',
                'turkey beach towel'
            ],
            'snow shovel': [
                'snow shovel', 'snow pusher', 'snow scoop', 
                'roof rake', 'ice scraper', 'snow removal',
                'ergonomic snow shovel'
            ],
            'outdoor furniture': [
                'outdoor furniture', 'patio furniture', 'garden furniture', 
                'lawn chairs', 'deck furniture', 'porch furniture',
                'outdoor dining set'
            ]
        }
        
        variations_list = variations.get(base_keyword, [base_keyword])
        
        for variation in variations_list[:4]:  # Erste 4 Variationen
            if len(all_results) >= limit:
                break
                
            print(f"   🔄 East Coast search for variation: '{variation}'...")
            
            # Use East Coast only search for each variation
            result = self.search_east_coast_only(variation, limit=min(50, limit - len(all_results)))
            
            if result and 'itemSummaries' in result:
                all_results.extend(result['itemSummaries'])
                print(f"   ✅ '{variation}': {len(result['itemSummaries'])} East Coast items")
            else:
                print(f"   ❌ No East Coast results for '{variation}'")
            
            time.sleep(1)
        
        # Remove duplicates
        unique_items = {}
        for item in all_results:
            item_id = item.get('itemId')
            if item_id and item_id not in unique_items:
                unique_items[item_id] = item
        
        unique_results = list(unique_items.values())
        print(f"   📊 Variations total: {len(unique_results)} unique East Coast items")
        
        return {'itemSummaries': unique_results} if unique_results else None
        
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