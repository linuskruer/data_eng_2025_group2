import time
from datetime import datetime
from ebay_api_client import EbayAPIClient
from weather_correlation_analyzer import WeatherCorrelationAnalyzer
from data_manager import DataManager

def simple_auto_collector():
    """Simple automatic collector restricted to specific East Coast cities"""
    api_client = EbayAPIClient()
    analyzer = WeatherCorrelationAnalyzer(api_client)
    data_manager = DataManager()

    collection_count = 0

    print("🚀 Starting automatic data collection...")
    print("🎯 STRATEGY: Restricted Cities Only (NY, Miami, Boston, DC, Philly)")
    print("📊 Expected: Fewer Products × More Items per Product")
    print("📅 Collection runs every ten minutes")
    print("⏹️  Press Ctrl+C to stop")

    try:
        while True:
            collection_count += 1
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            print(f"\n{'='*60}")
            print(f"🕐 COLLECTION #{collection_count} - {current_time}")
            print(f"{'='*60}")

            # Test API connection
            if not api_client.test_connection():
                print("❌ API connection failed. Waiting 5 minutes...")
                time.sleep(300)
                continue

            # Collect restricted city data
            print("🌊 Collecting restricted East Coast city weather data...")
            new_data = analyzer.collect_east_coast_data(items_per_product=200)

            if new_data:
                data_manager.save_data(new_data, append=True)
                stats = data_manager.get_data_stats()
                print(f"✅ {len(new_data)} new records collected")
                print(f"📊 Total: {stats['total_records']} records")

                # Backup every 12 hours
                if collection_count % 12 == 0:
                    data_manager.create_backup()
            else:
                print("❌ No new data collected.")

            # Wait 1 hour (3600 seconds)
            # print(f"\n⏰ Waiting 1 hour until next collection...")
            # for i in range(3600, 0, -60):
            #     minutes = i // 60
            #     print(f"   {minutes} minutes remaining...", end='\r')
            #     time.sleep(60)
            # Wartezeit auf 10 Minuten setzen

            # Wait 10 minutes
            for i in range(600, 0, -60):
                minutes = i // 60
                print(f"   {minutes} minutes remaining...", end='\r')
                time.sleep(60)

    except KeyboardInterrupt:
        stats = data_manager.get_data_stats()
        print(f"\n\n🛑 Collection stopped after {collection_count} runs")
        print(f"📈 Final statistics: {stats['total_records']} records")

if __name__ == "__main__":
    simple_auto_collector()
