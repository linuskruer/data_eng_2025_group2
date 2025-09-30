import csv
import json
import os
from datetime import datetime

class DataManager:
    def __init__(self, base_directory=None):
        # Set the target directory for data storage
        if base_directory is None:
            # Go up one level from scripts directory to project root, then into ebay_data
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)  # Go up to project root
            base_directory = os.path.join(project_root, "ebay_data")
        
        # Create directory if it doesn't exist
        os.makedirs(base_directory, exist_ok=True)
        self.base_directory = base_directory
        
        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.data_file = os.path.join(base_directory, f"east_coast_weather_data_{timestamp}.csv")
    
    def save_data(self, new_data, append=True):
        """Save only new data to CSV, avoid duplicates by item_id"""
        if not new_data:
            print("📭 No data to save")
            return None

        # For timestamped files, we always create new files, so no need to check duplicates
        # across different runs. But we can still check within the current dataset.
        existing_ids = set()
        
        # If we're appending to an existing file in this run, check for duplicates
        if append and os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        existing_ids.add(row.get("item_id"))
            except Exception as e:
                print(f"⚠️ Could not read existing file for duplicates: {e}")

        # Filter only new articles
        filtered_data = [row for row in new_data if row.get("item_id") not in existing_ids]

        if not filtered_data:
            print("📭 No new unique records to save")
            return None

        # For timestamped files, we only append if the file was created in this session
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
            return self.data_file
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return None
    
    def get_data_stats(self):
        """Return statistics about collected data - now checks all CSV files in directory"""
        csv_files = [f for f in os.listdir(self.base_directory) if f.endswith('.csv') and f.startswith('east_coast_weather_data_')]
        
        if not csv_files:
            return {"total_records": 0, "file_exists": False}
        
        all_records = []
        latest_file = None
        latest_time = None
        
        for csv_file in csv_files:
            file_path = os.path.join(self.base_directory, csv_file)
            try:
                # Extract timestamp from filename for sorting
                file_timestamp = csv_file.replace('east_coast_weather_data_', '').replace('.csv', '')
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    file_records = list(reader)
                    all_records.extend(file_records)
                    
                    # Track latest file
                    if latest_time is None or file_timestamp > latest_time:
                        latest_time = file_timestamp
                        latest_file = csv_file
                        
            except Exception as e:
                print(f"⚠️ Could not read file {csv_file}: {e}")
        
        if not all_records:
            return {"total_records": 0, "file_exists": True, "files_found": len(csv_files)}

        # Group by weather category
        category_counts = {}
        for record in all_records:
            category = record.get('weather_category', 'UNKNOWN')
            category_counts[category] = category_counts.get(category, 0) + 1
        
        return {
            "total_records": len(all_records),
            "file_exists": True,
            "files_found": len(csv_files),
            "latest_file": latest_file,
            "category_distribution": category_counts,
            "first_record": all_records[0] if all_records else None,
            "last_record": all_records[-1] if all_records else None
        }
    
    def create_backup(self):
        """Create a backup of the latest data file"""
        csv_files = [f for f in os.listdir(self.base_directory) if f.endswith('.csv') and f.startswith('east_coast_weather_data_')]
        
        if not csv_files:
            print("📭 No data files found to backup")
            return None
            
        # Find the latest file
        latest_file = None
        latest_time = None
        for csv_file in csv_files:
            file_timestamp = csv_file.replace('east_coast_weather_data_', '').replace('.csv', '')
            if latest_time is None or file_timestamp > latest_time:
                latest_time = file_timestamp
                latest_file = csv_file
        
        if latest_file:
            source_path = os.path.join(self.base_directory, latest_file)
            backup_file = f"backup_{latest_file}"
            backup_path = os.path.join(self.base_directory, backup_file)
            
            import shutil
            shutil.copy2(source_path, backup_path)
            print(f"📦 Backup created: {backup_file}")
            return backup_path
        
        return None
    
    def get_latest_data_file(self):
        """Get the path to the most recent data file"""
        return self.data_file