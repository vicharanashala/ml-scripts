import csv
from datetime import datetime

# List of input files
input_files = [
    'Data/PB JAN .csv',
    'Data/PB FEB.csv',
    'Data/PB MAR.csv',
    'Data/PB APR.csv',
    'Data/PB MAY.csv',
    'Data/PB JUL.csv'
]

# Required columns in the output
output_columns = [
    'BlockName', 'Category', 'Year', 'Month', 'Day', 
    'Crop', 'DistrictName', 'QueryType', 'Season', 
    'Sector', 'StateName', 'QueryText', 'KccAns', 'req_id'
]

# Counter for req_id
req_id_counter = 1

# List to store all rows
all_rows = []

# Process each file
for file_path in input_files:
    print(f"Processing {file_path}...")
    
    row_count = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Skip rows with empty QueryText
            query_text = row.get('QueryText', '').strip()
            if not query_text:
                continue
            
            # Extract Day from CreatedOn column
            created_on = row.get('CreatedOn', '')
            try:
                # Parse the datetime string (format: 2025-04-02T10:20:17.017)
                dt = datetime.fromisoformat(created_on.replace('Z', '+00:00'))
                day = dt.day
            except:
                # If parsing fails, try to extract day manually
                try:
                    day = int(created_on.split('T')[0].split('-')[2])
                except:
                    day = ''
            
            # Create output row
            output_row = {
                'BlockName': row.get('BlockName', ''),
                'Category': row.get('Category', ''),
                'Year': row.get('year', ''),
                'Month': row.get('month', ''),
                'Day': day,
                'Crop': row.get('Crop', ''),
                'DistrictName': row.get('DistrictName', ''),
                'QueryType': row.get('QueryType', ''),
                'Season': row.get('Season', ''),
                'Sector': row.get('Sector', ''),
                'StateName': row.get('StateName', ''),
                'QueryText': query_text,
                'KccAns': row.get('KccAns', ''),
                'req_id': req_id_counter
            }
            
            all_rows.append(output_row)
            req_id_counter += 1
            row_count += 1
    
    print(f"  - Processed {row_count} rows")

# Write to output file
output_file = 'Data/PB_Combined_Cleaned.csv'
print(f"\nWriting to {output_file}...")

with open(output_file, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=output_columns)
    writer.writeheader()
    writer.writerows(all_rows)

print(f"\n✓ Successfully created {output_file}")
print(f"  - Total rows: {len(all_rows)}")
print(f"  - Columns: {', '.join(output_columns)}")

# Show first few rows
print(f"\nFirst 3 rows:")
for i, row in enumerate(all_rows[:3], 1):
    print(f"\nRow {i}:")
    for key, value in row.items():
        # Truncate long values for display
        display_value = str(value)[:100] + '...' if len(str(value)) > 100 else value
        print(f"  {key}: {display_value}")
