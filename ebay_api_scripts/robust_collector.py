import time
from datetime import datetime
from ebay_api_client import EbayAPIClient
from weather_correlation_analyzer import WeatherCorrelationAnalyzer
from data_manager import DataManager

def robust_auto_collector():
    """Robust collector with automatic token recovery"""
    collection_count = 0

    print("🚀 Starting ROBUST automatic data collection...")
    print("🔄 Automatic token recovery enabled")

    try:
        while True:
            collection_count += 1
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            print(f"\n{'='*60}")
            print(f"🕐 COLLECTION #{collection_count} - {current_time}")
            print(f"{'='*60}")

            try:
                # API Client mit automatischem Token Management
                api_client = EbayAPIClient()
                analyzer = WeatherCorrelationAnalyzer(api_client)
                data_manager = DataManager()

                # Test connection with auto-retry
                if not api_client.test_connection():
                    print("❌ API connection failed, attempting token refresh...")
                    if api_client.force_token_refresh():
                        print("✅ Token refreshed, retrying connection...")
                        api_client = EbayAPIClient()  # Neue Instanz
                    
                    if not api_client.test_connection():
                        print("❌ Still no connection, waiting 10 minutes...")
                        time.sleep(600)
                        continue

                # Collect data
                print("🌊 Collecting East Coast weather data...")
                new_data = analyzer.collect_east_coast_data(items_per_product=200)

                if new_data:
                    data_manager.save_data(new_data, append=True)
                    stats = data_manager.get_data_stats()
                    print(f"✅ {len(new_data)} new records collected")
                    print(f"📊 Total: {stats['total_records']} records")
                else:
                    print("❌ No new data collected.")

            except Exception as e:
                print(f"❌ Collection error: {e}")
                print("🔄 Retrying in 10 minutes...")
                time.sleep(600)
                continue

            # Wait 1 hour
            print(f"\n⏰ Waiting 1 hour until next collection...")
            for i in range(3600, 0, -60):
                minutes = i // 60
                print(f"   {minutes} minutes remaining...", end='\r')
                time.sleep(60)

    except KeyboardInterrupt:
        print(f"\n\n🛑 Collection stopped after {collection_count} runs")

if __name__ == "__main__":
    robust_auto_collector()