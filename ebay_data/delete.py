import csv
import os
from datetime import datetime

def delete_rows_by_date_robust(csv_file_path, target_date):
    """
    Robust solution for malformed CSV files
    """
    
    if not os.path.exists(csv_file_path):
        print(f"❌ File not found: {csv_file_path}")
        return
    
    print(f"📖 Reading CSV file: {csv_file_path}")
    
    rows_to_keep = []
    rows_to_delete = 0
    deleted_examples = []
    fieldnames = None
    malformed_rows = 0
    
    # First pass: read and filter data
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        
        try:
            # Read header
            header = next(reader)
            fieldnames = header
            print(f"📋 Detected {len(header)} columns in header")
            
            for line_num, row in enumerate(reader, start=2):  # start=2 because of header
                try:
                    # Convert row to dictionary
                    row_dict = {}
                    for i, value in enumerate(row):
                        if i < len(fieldnames):
                            row_dict[fieldnames[i]] = value
                        else:
                            # Extra fields get numbered column names
                            row_dict[f'extra_field_{i}'] = value
                    
                    # Check if we have enough fields for collection_timestamp
                    if 'collection_timestamp' in row_dict:
                        timestamp = row_dict['collection_timestamp']
                        date_part = timestamp[:10] if timestamp and len(timestamp) >= 10 else ''
                        
                        if date_part == target_date:
                            rows_to_delete += 1
                            if len(deleted_examples) < 3:
                                deleted_examples.append(timestamp)
                        else:
                            rows_to_keep.append(row_dict)
                    else:
                        # If no timestamp field, keep the row
                        rows_to_keep.append(row_dict)
                        
                except Exception as e:
                    malformed_rows += 1
                    print(f"⚠️  Skipping malformed row {line_num}: {e}")
                    continue
                    
        except Exception as e:
            print(f"❌ Error reading CSV: {e}")
            return
    
    total_processed = rows_to_delete + len(rows_to_keep) + malformed_rows
    print(f"📊 Processing complete:")
    print(f"   - Total rows processed: {total_processed}")
    print(f"   - Rows to delete: {rows_to_delete}")
    print(f"   - Rows to keep: {len(rows_to_keep)}")
    print(f"   - Malformed rows skipped: {malformed_rows}")
    
    if rows_to_delete == 0:
        print(f"✅ No rows found with collection_timestamp starting with {target_date}")
        return
    
    # Show examples
    print(f"📝 Example timestamps to be deleted:")
    for example in deleted_examples:
        print(f"   - {example}")
    
    # Create backup
    backup_path = csv_file_path.replace('.csv', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    import shutil
    shutil.copy2(csv_file_path, backup_path)
    print(f"💾 Backup created: {backup_path}")
    
    # Write filtered data
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
        if fieldnames:
            # Use original fieldnames
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows_to_keep)
        else:
            # Fallback: infer fieldnames from data
            if rows_to_keep:
                all_keys = set()
                for row in rows_to_keep:
                    all_keys.update(row.keys())
                writer = csv.DictWriter(f, fieldnames=sorted(all_keys), extrasaction='ignore')
                writer.writeheader()
                writer.writerows(rows_to_keep)
    
    print(f"💾 Updated file saved: {csv_file_path}")
    print(f"📊 Final count: {len(rows_to_keep)} rows remaining")

def main():
    csv_file = "ebay_data/east_coast_weather_data.csv"
    
    # If file not found, try alternative paths
    if not os.path.exists(csv_file):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Try script directory
        csv_file = os.path.join(script_dir, "ebay_data", "east_coast_weather_data.csv")
        
        if not os.path.exists(csv_file):
            # Try project root
            project_root = os.path.dirname(script_dir)
            csv_file = os.path.join(project_root, "ebay_data", "east_coast_weather_data.csv")
    
    target_date = "2025-10-05"
    
    print(f"🔍 Looking for file: {csv_file}")
    print(f"🎯 Target date to delete: {target_date}")
    print("=" * 50)
    
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found at: {csv_file}")
        return
    
    # First, let's analyze the file
    print("🔍 Analyzing CSV file structure...")
    with open(csv_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i < 5:  # Show first 5 lines
                fields = line.strip().split(',')
                print(f"Line {i+1}: {len(fields)} fields")
                if i == 0:
                    print(f"Header: {fields}")
            else:
                break
    
    response = input("\n❓ Continue with deletion? (y/n): ")
    if response.lower() != 'y':
        print("❌ Operation cancelled")
        return
    
    delete_rows_by_date_robust(csv_file, target_date)

if __name__ == "__main__":
    main()