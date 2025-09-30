from datetime import datetime, timezone, timedelta
import time
import re

class WeatherCorrelationAnalyzer:
    def __init__(self, api_client):
        self.api_client = api_client
        # Allowed city postal code prefixes
        self.city_zip_prefixes = {
            "newyork": ["100", "101", "102", "103", "104", "111", "112", "113", "114", "116"],
            "miami": ["331", "332"],
            "boston": ["021", "022"],
            "washington_dc": ["200", "202"],
            "philadelphia": ["191", "192"]
        }

    def _get_east_coast_time(self):
        """Get current East Coast USA time (EDT/EST)."""
        utc_now = datetime.now(timezone.utc)
        east_coast_tz = timezone(timedelta(hours=-4))  # EDT fixed offset
        return utc_now.astimezone(east_coast_tz)

    def get_east_coast_weather_products(self):
        """East Coast specific products for weather correlation"""
        return {
            'rain_products': ['umbrella', 'rain jacket'],
            'heat_products': ['air conditioner', 'sunscreen'],
            'cold_products': ['winter coat', 'thermal gloves'],
            'seasonal_products': ['beach towel', 'snow shovel', 'outdoor furniture']
        }

    def _search_with_pagination(self, query, max_items=2000, location_filter=None):
        """Fetch multiple pages of results for a query, optionally with a location filter."""
        all_items = []
        offset = 0
        limit = 200  # eBay max per request

        while len(all_items) < max_items:
            result = self.api_client.search_products(
                query,
                limit=limit,
                offset=offset,
                location_filter=location_filter
            )

            if not result or "itemSummaries" not in result:
                break

            items = result["itemSummaries"]
            all_items.extend(items)

            if len(items) < limit:
                break  # last page

            offset += limit
            time.sleep(1.5)  # avoid rate limits

        return all_items

    def collect_east_coast_data(self, items_per_product=2000):
        """Collect data for restricted East Coast cities with full pagination"""
        if not self.api_client.access_token:
            return []

        weather_products = self.get_east_coast_weather_products()
        all_data = []

        for weather_category, products in weather_products.items():
            print(f"🌤️ Collecting {weather_category} data (city-specific, paginated)...")

            for product in products:
                for city, prefixes in self.city_zip_prefixes.items():
                    print(f"   🔍 Searching '{product}' in {city}...")
                    items = self._search_with_pagination(product, max_items=items_per_product, location_filter=city)

                    items_collected = 0
                    for item in items:
                        item_data = self._extract_correlation_data(item, weather_category, product)
                        if item_data:
                            all_data.append(item_data)
                            items_collected += 1

                    print(f"   ✅ {product} ({city}): {items_collected} valid items")

        return all_data

    def _is_allowed_city_zip(self, postal_code):
        """Check if postal code belongs to allowed cities."""
        if not postal_code:
            return False
        prefix = str(postal_code)[:3]
        for city_prefixes in self.city_zip_prefixes.values():
            if prefix in city_prefixes:
                return True
        return False

    def _extract_correlation_data(self, item, weather_category, product_type):
        """Extract raw attributes, filter by allowed city ZIPs."""
        price_value = item.get('price', {}).get('value')
        if not price_value:
            return None

        try:
            price = float(price_value)
        except (TypeError, ValueError):
            return None

        # Ensure product word appears in title
        title = item.get("title", "").lower()
        if product_type.lower() not in title:
            return None

        # Get East Coast timestamp
        east_coast_time = self._get_east_coast_time()

        # Extract postal code
        zip_prefix = None
        if 'itemLocation' in item:
            if isinstance(item['itemLocation'], dict):
                zip_prefix = item['itemLocation'].get('postalCode', '')[:3]
            elif isinstance(item['itemLocation'], str):
                zip_match = re.search(r"'postalCode':\s*'([0-9]{3})", item['itemLocation'])
                if zip_match:
                    zip_prefix = zip_match.group(1)

        # Skip if not allowed city
        if not self._is_allowed_city_zip(zip_prefix):
            return None

        # Seller info
        seller_info = item.get('seller', {})
        try:
            seller_feedback_percentage = float(seller_info.get('feedbackPercentage', 0))
        except (TypeError, ValueError):
            seller_feedback_percentage = 0

        try:
            seller_feedback_score = int(seller_info.get('feedbackScore', 0))
        except (TypeError, ValueError):
            seller_feedback_score = 0

        # Shipping info
        shipping_cost = 0.0
        free_shipping = False
        shipping_options = item.get('shippingOptions', [])
        if shipping_options:
            shipping_cost_value = shipping_options[0].get('shippingCost', {}).get('value', 0)
            try:
                shipping_cost = float(shipping_cost_value)
                free_shipping = shipping_cost == 0
            except (TypeError, ValueError):
                shipping_cost = 0.0
                free_shipping = True

        return {
            # Time (API collection time)
            'collection_timestamp': east_coast_time.isoformat(),
            #'collection_hour': east_coast_time.hour,
            #'collection_day': east_coast_time.weekday(),
            #'collection_date': east_coast_time.strftime('%Y-%m-%d'),
            'timezone': 'EDT',

            # Weather/product category
            'weather_category': weather_category,
            'product_type': product_type,

            # Price
            'price': price,
            'currency': item.get('price', {}).get('currency', 'USD'),

            # Seller info
            'seller_feedback_percentage': seller_feedback_percentage,
            'seller_feedback_score': seller_feedback_score,

            # Location info
            'item_location': str(item.get('itemLocation', 'Unknown')),
            'seller_location': seller_info.get('location', 'Unknown'),
            #'zip_prefix': zip_prefix,

            # Shipping
            'shipping_cost': shipping_cost,
            'free_shipping': free_shipping,

            # Product details
            'condition': item.get('condition', 'UNKNOWN'),

            # Buying options
            'buying_options': ','.join(item.get('buyingOptions', [])),

            # Engagement proxies
            'title_length': len(item.get('title', '')),

            # Identification
            'item_id': item.get('itemId'),
            'marketplace_id': 'EBAY_US'
        }

    def get_data_summary(self, data):
        """Generate summary statistics for the collected data"""
        if not data:
            return "No data available"

        total_records = len(data)
        prices = [item['price'] for item in data if item.get('price')]
        avg_price = sum(prices) / len(prices) if prices else 0

        # Weather category distribution
        category_dist = {}
        for item in data:
            category = item.get('weather_category', 'UNKNOWN')
            category_dist[category] = category_dist.get(category, 0) + 1

        # ZIP prefix distribution
        zip_dist = {}
        for item in data:
            zp = item.get('zip_prefix', 'UNKNOWN')
            zip_dist[zp] = zip_dist.get(zp, 0) + 1

        return {
            'total_records': total_records,
            'average_price': round(avg_price, 2),
            'min_price': min(prices) if prices else 0,
            'max_price': max(prices) if prices else 0,
            'category_distribution': category_dist,
            'zip_distribution': zip_dist
        }