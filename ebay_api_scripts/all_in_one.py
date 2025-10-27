import os
import requests
import time
import csv
import re
from datetime import datetime, timedelta, timezone

from ebay_auth import EbayAuth


class CombinedEbayWeatherAnalyzer:
    def __init__(self, base_directory=None):
        # —— Paths & output file ——
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

        # Set up data directory – saves to "ebay_data" folder in project root by default
        if base_directory is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            base_directory = os.path.join(project_root, "ebay_data")
        os.makedirs(base_directory, exist_ok=True)

        self.base_directory = base_directory
        self.data_file = os.path.join(base_directory, "east_coast_weather_data.csv")

        # —— Credentials from ENV (A2) ——
        client_id = os.getenv("EBAY_CLIENT_ID")
        client_secret = os.getenv("EBAY_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError(
                "Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET environment variables."
            )

        # —— Auth helper (uses Application Access Token flow) ——
        self.ebay_auth = EbayAuth(client_id=client_id, client_secret=client_secret)

        # —— City ZIP prefixes (East Coast target subset) ——
        self.city_zip_prefixes = {
            "new_york": ["100", "101", "102", "103", "104", "111", "112", "113", "114", "116"],
            "boston": ["021", "022", "023"],
            "washington_dc": ["200", "202", "203", "204"],
            "miami": ["331", "332", "333"],
            "jacksonville": ["320", "322", "322"]
        }

    # ----------------------------
    # eBay Search
    # ----------------------------
    def search_products(self, keyword, category_id=None, limit=50, location_filter=None, offset=0):
        """Search for products using eBay Browse API with automatic token management."""
        token = self.ebay_auth.get_access_token()

        url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_US"
        }

        filters = ['buyingOptions:{FIXED_PRICE}']
        if location_filter:
            # Note: itemLocation filter accepts strings like city or region; we pass city name
            filters.append(f'itemLocation:{location_filter}')

        params = {
            "q": keyword,
            "limit": min(limit, 200),
            "offset": offset,
            "filter": ",".join(filters),
        }
        if category_id:
            params["category_ids"] = category_id

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Search error for '{keyword}' [{response.status_code}]: {response.text}")
                return None
        except Exception as e:
            print(f"❌ Request failed: {e}")
            return None

    def test_connection(self):
        """Test if the API connection works."""
        try:
            token = self.ebay_auth.get_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "X-EBAY-C-MARKETPLACE-ID": "EBAY_US"
            }
            resp = requests.get(
                "https://api.ebay.com/buy/browse/v1/item_summary/search?q=umbrella&limit=1",
                headers=headers,
                timeout=30
            )
            if resp.status_code == 200:
                print("✅ eBay API connection successful!")
                return True
            print(f"❌ eBay API error: {resp.status_code} - {resp.text}")
            return False
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False

    # ----------------------------
    # Product sets & pagination
    # ----------------------------
    def get_east_coast_weather_products(self):
        """Products grouped for weather correlation."""
        return {
            "rain_products": [
                "umbrella", "rain jacket", "rain boots", "waterproof pants",
                "rain cover for backpack", "waterproof phone case", "quick-dry towel",
                "moisture-wicking socks", "anti-fog spray for glasses", "trench coat", "waterproof hat",
                "shoe waterproofing spray", "drying rack for wet clothes", "portable rain poncho", "gaiters"
            ],
            "heat_products": [
                "air conditioner", "sunscreen", "portable fan", "cooling towel", "sun hat",
                "sunglasses", "UV-protective clothing", "insulated water bottle",
                "misting spray bottle", "lightweight linen clothing", "cooling neck gaiter",
                "beach umbrella", "solar charger", "aloe vera gel", "heat-reflective window film"
            ],
            "cold_products": [
                "winter coat", "thermal gloves", "wool beanie", "scarf", "thermal underwear",
                "insulated boots", "hand warmers", "fleece jacket", "thermal socks", "ear muffs",
                "balaclava", "windproof jacket", "heated vest", "ski pants", "lip balm with SPF"
            ],
            "seasonal_products": [
                "beach towel", "snow shovel", "outdoor furniture", "gardening tools",
                "swimming pool", "christmas decorations", "halloween costumes", "spring flower seeds",
                "bird feeder", "picnic basket", "camping gear", "holiday lights", "lawn mower",
                "fall leaf rake", "beach chairs"
            ]
        }

    def _search_with_pagination(self, query, max_items=1000, location_filter=None):
        """Fetch up to max_items by paging through the Browse API."""
        all_items = []
        offset = 0
        limit = 200  # eBay max

        while len(all_items) < max_items:
            result = self.search_products(
                query,
                limit=limit,
                offset=offset,
                location_filter=location_filter
            )
            if not result or "itemSummaries" not in result:
                break

            items = result["itemSummaries"]
            all_items.extend(items)
            print(f"📄 Page {offset // limit + 1}: {len(items)} items (Total: {len(all_items)})")

            if len(items) < limit or len(all_items) >= max_items:
                break

            offset += limit
            time.sleep(1.5)  # Gentle rate limiting

        return all_items

    # ----------------------------
    # Filters & extraction
    # ----------------------------
    def _is_allowed_city_zip(self, postal_code):
        """Check if postal code belongs to allowed cities (based on 3-digit prefix)."""
        if not postal_code:
            return False
        prefix = str(postal_code)[:3]
        for city_prefixes in self.city_zip_prefixes.values():
            if prefix in city_prefixes:
                return True
        return False

    def _get_east_coast_time(self):
        """Get current East Coast USA time (EDT/EST approximation)."""
        utc_now = datetime.now(timezone.utc)
        east_coast_tz = timezone(timedelta(hours=-4))  # Simple offset; OK for correlation
        return utc_now.astimezone(east_coast_tz)

    def _extract_complete_item_data(self, item, weather_category, product_type):
        """Extract ONLY the specified columns from eBay item."""
        price_value = item.get("price", {}).get("value")
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

        # Location fields
        item_location = item.get("itemLocation", "Unknown")
        seller_info = item.get("seller", {})
        seller_location = seller_info.get("location", "Unknown")
        zip_prefix = None

        if item_location and isinstance(item_location, dict):
            postal_code = item_location.get("postalCode", "")
            zip_prefix = postal_code[:3] if postal_code else None
        elif item_location and isinstance(item_location, str):
            # Try to extract ZIP from string representation
            zip_match = re.search(r"'postalCode':\s*'([0-9]{3})", item_location)
            if zip_match:
                zip_prefix = zip_match.group(1)

        # Skip if not an allowed city
        if not self._is_allowed_city_zip(zip_prefix):
            return None

        # Shipping info
        shipping_options = item.get("shippingOptions", [])
        shipping_cost = 0.0
        free_shipping = False
        if shipping_options:
            shipping_cost_value = shipping_options[0].get("shippingCost", {}).get("value", 0)
            try:
                shipping_cost = float(shipping_cost_value)
                free_shipping = (shipping_cost == 0)
            except (TypeError, ValueError):
                shipping_cost = 0.0
                free_shipping = True

        data_record = {
            # Collection info
            "collection_timestamp": east_coast_time.isoformat(),
            "timezone": "EDT",

            # Weather/product category
            "weather_category": weather_category,
            "product_type": product_type,

            # Price info
            "price": price,
            "currency": item.get("price", {}).get("currency", "USD"),

            # Seller info
            "seller_feedback_percentage": float(seller_info.get("feedbackPercentage", 0)) if seller_info.get("feedbackPercentage") else 0,
            "seller_feedback_score": int(seller_info.get("feedbackScore", 0)) if seller_info.get("feedbackScore") else 0,

            # Location info - keep raw format for CSV
            "item_location": str(item_location),
            "seller_location": seller_location,

            # Shipping info
            "shipping_cost": shipping_cost,
            "free_shipping": free_shipping,

            # Product details
            "condition": item.get("condition", ""),
            "buying_options": ",".join(item.get("buyingOptions", [])),

            # Misc
            "title_length": len(item.get("title", "")),

            # IDs
            "item_id": item.get("itemId"),
            "marketplace_id": "EBAY_US"
        }
        return data_record

    # ----------------------------
    # Collection & storage
    # ----------------------------
    def collect_east_coast_data(self, items_per_product=1000):
        """Collect data for SPECIFIC 5 East Coast cities only."""
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

    def _get_existing_ids(self):
        """Load existing item_ids from CSV (to avoid duplicates)."""
        existing_ids = set()
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        item_id = row.get("item_id")
                        if item_id and item_id.strip():
                            existing_ids.add(item_id.strip())
                print(f"🔍 Loaded {len(existing_ids)} existing IDs from CSV")
            except Exception as e:
                print(f"⚠️ Could not read existing file for duplicates: {e}")
        return existing_ids

    def save_data(self, new_data, append=True):
        """Save data to CSV file with deduplication (B1 – CSV only)."""
        if not new_data:
            print("📭 No data to save")
            return None

        existing_ids = self._get_existing_ids()
        filtered_data = []
        for row in new_data:
            item_id = row.get("item_id")
            if item_id and item_id not in existing_ids:
                filtered_data.append(row)
                existing_ids.add(item_id)

        if not filtered_data:
            print("📭 No new unique records to save")
            return None

        file_mode = "a" if append and os.path.exists(self.data_file) else "w"
        write_header = (file_mode == "w") or (not os.path.exists(self.data_file))

        try:
            with open(self.data_file, file_mode, newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=filtered_data[0].keys())
                if write_header:
                    writer.writeheader()
                writer.writerows(filtered_data)

            action = "appended to" if file_mode == "a" else "saved to"
            print(f"💾 {len(filtered_data)} new unique records {action} {self.data_file}")
            return self.data_file
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return None

    def get_data_stats(self):
        """Return simple statistics over the CSV."""
        if not os.path.exists(self.data_file):
            return {"total_records": 0, "file_exists": False, "data_file_path": self.data_file}

        try:
            with open(self.data_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                records = list(reader)

            category_counts = {}
            city_counts = {}
            for record in records:
                category = record.get("weather_category", "UNKNOWN")
                category_counts[category] = category_counts.get(category, 0) + 1

                city = record.get("item_location", "Unknown")
                if city:
                    city_counts[city] = city_counts.get(city, 0) + 1

            return {
                "total_records": len(records),
                "file_exists": True,
                "category_distribution": category_counts,
                "city_distribution": city_counts,
                "data_file_path": self.data_file
            }
        except Exception as e:
            print(f"❌ Error reading statistics: {e}")
            return {"total_records": 0, "file_exists": False, "data_file_path": self.data_file}

    # ----------------------------
    # Simple loop runner (for local testing)
    # ----------------------------
    def simple_auto_collector(self):
        """Automatic collector for 5 specific East Coast cities."""
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

                    file_path = stats.get("data_file_path", saved_file or self.data_file)
                    print(f"📁 Saved to: {file_path}")

                    if "city_distribution" in stats:
                        print("🏙️ City distribution:")
                        for city, count in stats["city_distribution"].items():
                            print(f"   {city}: {count} records")
                else:
                    print("❌ No new data collected.")

                # Wait 10 minutes
                print("\n⏰ Waiting 10 minutes until next collection...")
                for i in range(600, 0, -60):
                    minutes = i // 60
                    print(f"   {minutes} minutes remaining...", end="\r")
                    time.sleep(60)

        except KeyboardInterrupt:
            stats = self.get_data_stats()
            print(f"\n\n🛑 Collection stopped after {collection_count} runs")
            print(f"📈 Final statistics: {stats['total_records']} records")
            file_path = stats.get("data_file_path", self.data_file)
            print(f"💾 Data saved to: {file_path}")


def main():
    analyzer = CombinedEbayWeatherAnalyzer()
    print(f"💾 Data will be saved to: {analyzer.data_file}")

    if not analyzer.test_connection():
        print("❌ API connection failed. Check EBAY_CLIENT_ID / EBAY_CLIENT_SECRET.")
        return

    # Start automatic collection loop (local testing)
    analyzer.simple_auto_collector()


if __name__ == "__main__":
    main()
