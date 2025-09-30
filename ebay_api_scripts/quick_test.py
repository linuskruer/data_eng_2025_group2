from ebay_api_client import EbayAPIClient
from weather_correlation_analyzer import WeatherCorrelationAnalyzer
from data_manager import DataManager

def quick_test():
    """Schneller Test der Funktionalität"""
    print("🔍 Schnelltest der East Coast Datensammlung...")
    
    api_client = EbayAPIClient()
    analyzer = WeatherCorrelationAnalyzer(api_client)
    data_manager = DataManager()
    
    if api_client.test_connection():
        print("✅ API-Verbindung erfolgreich")
        
        # Test-Sammlung mit weniger Daten
        test_data = analyzer.collect_east_coast_data(items_per_product=5)
        
        if test_data:
            data_manager.save_data(test_data)
            stats = data_manager.get_data_stats()
            print(f"✅ Test erfolgreich! {stats['total_records']} Test-Datensätze gesammelt.")
            print(f"📊 Enthaltene Spalten: {list(test_data[0].keys())}")
        else:
            print("❌ Test fehlgeschlagen - keine Daten erhalten")
    else:
        print("❌ API-Verbindung fehlgeschlagen")

if __name__ == "__main__":
    quick_test()