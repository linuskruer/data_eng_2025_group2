import csv
import json
import os
from datetime import datetime

class DataManager:
    def __init__(self, base_directory=None):
        if base_directory is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            base_directory = os.path.join(project_root, "ebay_data")
        
        os.makedirs(base_directory, exist_ok=True)
        self.base_directory = base_directory
        self.data_file = os.path.join(base_directory, "east_coast_weather_data.csv")
    
    def save_data(self, new_data, append=True):
        """Save only new data to CSV, avoid duplicates by item_id"""
        if not new_data:
            print("📭 No data to save")
            return None

        # Verbesserte Duplikatprüfung
        existing_ids = self._get_existing_ids()
        
        # Filter nur wirklich neue Artikel
        filtered_data = []
        for row in new_data:
            item_id = row.get("item_id")
            if not item_id:
                continue  # Überspringe Einträge ohne item_id
            if item_id not in existing_ids:
                filtered_data.append(row)
                existing_ids.add(item_id)  # Füge zur Set hinzu für weitere Prüfungen in diesem Durchlauf

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
            print(f"💾 {len(filtered_data)} new unique records {action} {os.path.basename(self.data_file)}")
            
            # Debug-Info
            print(f"🔍 Debug: Total records after save: {self._get_total_records()}")
            
            return self.data_file
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return None
    
    def _get_existing_ids(self):
        """Lade existierende IDs mit verbesserter Fehlerbehandlung"""
        existing_ids = set()
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        item_id = row.get("item_id")
                        if item_id and item_id.strip():  # Prüfe auf leere IDs
                            existing_ids.add(item_id.strip())
                print(f"🔍 Loaded {len(existing_ids)} existing IDs from CSV")
            except Exception as e:
                print(f"⚠️ Could not read existing file for duplicates: {e}")
        return existing_ids
    
    def _get_total_records(self):
        """Zähle die tatsächlichen Records in der Datei"""
        if not os.path.exists(self.data_file):
            return 0
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                return sum(1 for _ in reader)
        except:
            return 0
    
    def get_data_stats(self):
        """Return statistics about collected data"""
        total_records = self._get_total_records()
        
        if total_records == 0:
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