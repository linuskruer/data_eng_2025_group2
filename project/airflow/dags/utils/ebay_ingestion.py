"""
eBay data ingestion into ClickHouse bronze layer.
Adapted from Kubernetes_ebay/all_in_one.py for Docker environment.
Uses clickhouse_connect instead of clickhouse_driver.
"""
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests
import clickhouse_connect
from utils.ebay_auth import EbayAuth


BRONZE_TABLE = os.getenv("CH_BRONZE_TABLE", "bronze.ebay_raw_data")


class EbayIngestor:
    def __init__(self):
        # eBay creds (env only)
        client_id = os.getenv("EBAY_CLIENT_ID")
        client_secret = os.getenv("EBAY_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET.")

        self.ebay_auth = EbayAuth(client_id=client_id, client_secret=client_secret)

        # ClickHouse connection (using clickhouse_connect for Docker)
        self.ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
        self.ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.ch_user = os.getenv("CLICKHOUSE_USER", "default")
        self.ch_password = os.getenv("CLICKHOUSE_PASSWORD", "mypassword")
        self.client = clickhouse_connect.get_client(
            host=self.ch_host,
            port=self.ch_port,
            username=self.ch_user,
            password=self.ch_password
        )

        # East Coast city filters (3-digit ZIP prefixes)
        self.city_zip_prefixes = {
            "new_york": ["100", "101", "102", "103", "104", "111", "112", "113", "114", "116"],
            "boston": ["021", "022", "023"],
            "washington_dc": ["200", "202", "203", "204"],
            "miami": ["331", "332", "333"],
            "jacksonville": ["320", "322", "322"],
        }

    # ----------------------------
    # eBay search helpers
    # ----------------------------
    def search_products(self, keyword, category_id=None, limit=50, location_filter=None, offset=0):
        """Call eBay Browse API with an app access token."""
        token = self.ebay_auth.get_access_token()
        url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
        }
        filters = ['buyingOptions:{FIXED_PRICE}']
        if location_filter:
            filters.append(f'itemLocation:{location_filter}')
        params = {
            "q": keyword,
            "limit": min(limit, 200),
            "offset": offset,
            "filter": ",".join(filters),
        }
        if category_id:
            params["category_ids"] = category_id

        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"❌ eBay API {resp.status_code}: {resp.text[:250]}")
            return None

    def _search_with_pagination(self, query, max_items=1000, location_filter=None):
        all_items, offset, limit = [], 0, 200
        while len(all_items) < max_items:
            result = self.search_products(query, limit=limit, offset=offset, location_filter=location_filter)
            if not result or "itemSummaries" not in result:
                break
            items = result["itemSummaries"]
            all_items.extend(items)
            print(f"📄 '{query}' {location_filter or ''}: page {offset // limit + 1} → {len(items)} items")
            if len(items) < limit or len(all_items) >= max_items:
                break
            offset += limit
            time.sleep(1.5)
        return all_items

    # ----------------------------
    # Transformation
    # ----------------------------
    def _get_east_coast_time(self):
        utc_now = datetime.now(timezone.utc)
        east_coast_tz = timezone(timedelta(hours=-4))  # simple offset, OK for correlation
        return utc_now.astimezone(east_coast_tz)

    def _is_allowed_city_zip(self, postal_code):
        if not postal_code:
            return False
        prefix = str(postal_code)[:3]
        for city_prefixes in self.city_zip_prefixes.values():
            if prefix in city_prefixes:
                return True
        return False

    def _extract_record(self, item, weather_category, product_type):
        # minimal guards
        price_value = item.get("price", {}).get("value")
        if not price_value:
            return None
        try:
            price = float(price_value)
        except (TypeError, ValueError):
            return None
        title = (item.get("title") or "").lower()
        if product_type.lower() not in title:
            return None

        item_location = item.get("itemLocation", "Unknown")
        seller_info = item.get("seller", {}) or {}
        seller_location = seller_info.get("location", "Unknown")

        # ZIP prefix
        zip_prefix = None
        if isinstance(item_location, dict):
            postal_code = item_location.get("postalCode", "")
            zip_prefix = postal_code[:3] if postal_code else None
        elif isinstance(item_location, str):
            m = re.search(r"'postalCode':\s*'([0-9]{3})", item_location)
            if m:
                zip_prefix = m.group(1)
        if not self._is_allowed_city_zip(zip_prefix):
            return None

        # shipping
        shipping_options = item.get("shippingOptions", []) or []
        shipping_cost, free_shipping = 0.0, False
        if shipping_options:
            val = shipping_options[0].get("shippingCost", {}).get("value", 0)
            try:
                shipping_cost = float(val)
                free_shipping = (shipping_cost == 0.0)
            except (TypeError, ValueError):
                shipping_cost, free_shipping = 0.0, True

        east_ts = self._get_east_coast_time()

        # Return as dict for clickhouse_connect insert
        return {
            "collection_timestamp": east_ts,
            "timezone": "EDT",
            "weather_category": weather_category,
            "product_type": product_type,
            "price": price,
            "currency": item.get("price", {}).get("currency", "USD"),
            "seller_feedback_percentage": float(seller_info.get("feedbackPercentage", 0) or 0),
            "seller_feedback_score": int(seller_info.get("feedbackScore", 0) or 0),
            "item_location": str(item_location),
            "seller_location": seller_location or "Unknown",
            "shipping_cost": shipping_cost,
            "free_shipping": 1 if free_shipping else 0,
            "condition": item.get("condition", "") or "",
            "buying_options": ",".join(item.get("buyingOptions", []) or []),
            "title_length": len(item.get("title", "") or ""),
            "item_id": item.get("itemId"),
            "marketplace_id": "EBAY_US",
        }

    def products_by_weather(self):
        return {
            "rain_products": [
                "umbrella","rain jacket","rain boots","waterproof pants",
                "rain cover for backpack","waterproof phone case","quick-dry towel",
                "moisture-wicking socks","anti-fog spray for glasses","trench coat","waterproof hat",
                "shoe waterproofing spray","drying rack for wet clothes","portable rain poncho","gaiters"
            ],
            "heat_products": [
                "air conditioner","sunscreen","portable fan","cooling towel","sun hat",
                "sunglasses","UV-protective clothing","insulated water bottle",
                "misting spray bottle","lightweight linen clothing","cooling neck gaiter",
                "beach umbrella","solar charger","aloe vera gel","heat-reflective window film"
            ],
            "cold_products": [
                "winter coat","thermal gloves","wool beanie","scarf","thermal underwear",
                "insulated boots","hand warmers","fleece jacket","thermal socks","ear muffs",
                "balaclava","windproof jacket","heated vest","ski pants","lip balm with SPF"
            ],
            "seasonal_products": [
                "beach towel","snow shovel","outdoor furniture","gardening tools",
                "swimming pool","christmas decorations","halloween costumes","spring flower seeds",
                "bird feeder","picnic basket","camping gear","holiday lights","lawn mower","fall leaf rake","beach chairs"
            ],
        }

    # ----------------------------
    # ClickHouse I/O
    # ----------------------------
    def ensure_table(self):
        # Create minimal compatible table if not exists (flat columns)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS bronze.ebay_raw_data
        (
            collection_timestamp   DateTime,
            timezone               String,
            weather_category       String,
            product_type           String,
            price                  Float64,
            currency               String,
            seller_feedback_percentage Float64,
            seller_feedback_score  Int32,
            item_location          String,
            seller_location        String,
            shipping_cost          Float64,
            free_shipping          UInt8,
            condition              String,
            buying_options         String,
            title_length           Int32,
            item_id                String,
            marketplace_id         String
        )
        ENGINE = MergeTree()
        ORDER BY item_id;
        """
        self.client.command(create_sql)
        print(f"✅ Ensured table {BRONZE_TABLE} exists")

    def insert_rows(self, rows):
        if not rows:
            print("📭 No rows to insert into ClickHouse.")
            return 0
        
        # Convert list of dicts to DataFrame for insert
        import pandas as pd
        df = pd.DataFrame(rows)
        
        # Insert using clickhouse_connect
        self.client.insert_df(BRONZE_TABLE, df)
        print(f"✅ Inserted {len(rows)} rows into {BRONZE_TABLE}")
        return len(rows)

    # ----------------------------
    # One-shot run for Airflow
    # ----------------------------
    def run_once(self, items_per_product=200):
        # sanity: can we get a token?
        _ = self.ebay_auth.get_access_token()

        self.ensure_table()

        rows = []
        products = self.products_by_weather()
        print("🚀 Collecting eBay items for 5 East Coast cities → Bronze ClickHouse")

        for weather_category, prod_list in products.items():
            for product in prod_list:
                for city_name in self.city_zip_prefixes.keys():
                    items = self._search_with_pagination(
                        query=product, max_items=items_per_product, location_filter=city_name
                    )
                    for it in items:
                        rec = self._extract_record(it, weather_category, product)
                        if rec:
                            rows.append(rec)
                    print(f"   ✅ {product:20s} @ {city_name:14s} → {len(items)} raw, {len(rows)} total so far")
                    time.sleep(1)

        # Idempotency guard: prevent duplicate item_ids for this run
        # (simple dedupe in memory by (collection_timestamp,item_id))
        seen = set()
        dedup = []
        for r in rows:
            key = (r["collection_timestamp"], r["item_id"])
            if key not in seen:
                seen.add(key)
                dedup.append(r)
        print(f"🔁 Deduplicated: {len(rows)} → {len(dedup)}")

        return self.insert_rows(dedup)


def run_ebay_ingestion(**kwargs):
    """
    Airflow callable to run eBay ingestion.
    Gets credentials from Airflow Variables.
    """
    import os
    from airflow.models import Variable
    
    # Get credentials from Airflow Variables
    client_id = Variable.get("EBAY_CLIENT_ID", default_var="")
    client_secret = Variable.get("EBAY_CLIENT_SECRET", default_var="")
    items_per_product = int(Variable.get("EBAY_ITEMS_PER_PRODUCT", default_var="200"))
    
    # Set environment variables for EbayIngestor
    os.environ["EBAY_CLIENT_ID"] = client_id
    os.environ["EBAY_CLIENT_SECRET"] = client_secret
    os.environ["EBAY_ITEMS_PER_PRODUCT"] = str(items_per_product)
    os.environ["CLICKHOUSE_HOST"] = "clickhouse-server"
    os.environ["CLICKHOUSE_PORT"] = "8123"
    os.environ["CLICKHOUSE_USER"] = "default"
    os.environ["CLICKHOUSE_PASSWORD"] = "mypassword"
    os.environ["CH_BRONZE_TABLE"] = "bronze.ebay_raw_data"
    
    ing = EbayIngestor()
    count = ing.run_once(items_per_product=items_per_product)
    print(f"🎉 Done. Inserted rows: {count}")
    return count

