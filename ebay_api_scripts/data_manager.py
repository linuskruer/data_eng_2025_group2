import csv
import json
import os
from datetime import datetime

class DataManager:
    def __init__(self, base_directory=None):
        if base_directory is None:
            base_directory = os.path.dirname(os.path.abspath(__file__))
        self.base_directory = base_directory
        self.data_file = os.path.join(base_directory, "east_coast_weather_data.csv")
    
    def save_data(self, new_data, append=True):
        """Save only new data to CSV, avoid duplicates by item_id"""
        if not new_data:
            print("📭 No data to save")
            return None

        existing_ids = set()
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        existing_ids.add(row.get("item_id"))
            except Exception as e:
                print(f"⚠️ Could not read existing file for duplicates: {e}")

        # Filter nur neue Artikel
        filtered_data = [row for row in new_data if row.get("item_id") not in existing_ids]

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
    
    def get_data_stats(self):
        """Return statistics about collected data"""
        if not os.path.exists(self.data_file):
            return {"total_records": 0, "file_exists": False}
        
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                records = list(reader)
            
            # Group by weather category
            category_counts = {}
            for record in records:
                category = record.get('weather_category', 'UNKNOWN')
                category_counts[category] = category_counts.get(category, 0) + 1
            
            return {
                "total_records": len(records),
                "file_exists": True,
                "category_distribution": category_counts,
                "first_record": records[0] if records else None,
                "last_record": records[-1] if records else None
            }
            
        except Exception as e:
            print(f"❌ Error reading statistics: {e}")
            return {"total_records": 0, "file_exists": False}
    
    def create_backup(self):
        """Create a backup of the data"""
        if os.path.exists(self.data_file):
            backup_file = f"backup_east_coast_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            backup_path = os.path.join(self.base_directory, backup_file)
            
            import shutil
            shutil.copy2(self.data_file, backup_path)
            print(f"📦 Backup created: {backup_file}")
            return backup_path
        return None
    