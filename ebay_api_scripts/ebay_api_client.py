import os
import requests
import time
from datetime import datetime

class EbayAPIClient:
    def __init__(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        token_file = os.path.join(current_dir, "ebay_token.txt")
        self.access_token = self._load_token_from_file(token_file)
        
    def _load_token_from_file(self, token_file):
        """Load OAuth token from text file"""
        try:
            with open(token_file, 'r') as f:
                token = f.read().strip()
            print("✅ Token loaded from file")
            return token
        except FileNotFoundError:
            print(f"❌ Token file not found at: {token_file}")
            return None
        except Exception as e:
            print(f"❌ Error reading token file: {e}")
            return None
    
    def search_products(self, keyword, category_id=None, limit=50, location_filter=None, offset=0):
        """Search for products with optional location filter and pagination"""
        if not self.access_token:
            print("No access token available")
            return None
            
        url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
        }
        
        # Build filters
        filters = ['buyingOptions:{FIXED_PRICE}']
        
        # Add location filter if provided
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
                print("❌ Error: OAuth token is invalid or expired")
                return None
            else:
                print(f"❌ Search error for '{keyword}': {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Request failed: {e}")
            return None
    
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
        if not self.access_token:
            return False
        result = self.search_products("umbrella", limit=1)
        return result is not None