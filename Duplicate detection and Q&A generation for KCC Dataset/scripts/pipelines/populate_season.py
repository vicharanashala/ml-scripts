import csv

def determine_season(month, day):
    """
    Determine the season based on month and day.
    
    Rabi season: Mid November to mid April/May (Nov 16 - May 15)
    Kharif season: June to October (Jun 1 - Oct 31)
    
    Args:
        month: Month number (1-12) as string
        day: Day of month as string
    
    Returns:
        Season name: "Rabi" or "Kharif" or empty string if outside both seasons
    """
    # Handle missing values
    if not month or not day or month == '' or day == '':
        return ""
    
    try:
        # Convert to float first (to handle decimal values), then to int
        month = int(float(month))
        day = int(float(day))
    except (ValueError, TypeError):
        return ""
    
    # Kharif season: June to October (months 6-10)
    if 6 <= month <= 10:
        return "Kharif"
    
    # Rabi season: Mid November to mid May
    # November 16 onwards
    if month == 11 and day >= 16:
        return "Rabi"
    
    # December to April (full months)
    if 12 <= month <= 4:
        return "Rabi"
    
    # May 1 to May 15
    if month == 5 and day <= 15:
        return "Rabi"
    
    # Outside both seasons (Nov 1-15, May 16-31)
    return ""

def main():
    # File paths
    input_file = '/home/ubuntu/Kshitij/unique-qs/outputs/deduplicated/PB_Paddy_Final_Unique.csv'
    output_file = input_file  # Overwrite the original file
    
    print(f"Reading file: {input_file}")
    
    # Read the CSV file
    rows = []
    season_column_index = None
    month_column_index = None
    day_column_index = None
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        # Find column indices
        try:
            season_column_index = header.index('Season')
            month_column_index = header.index('Month')
            day_column_index = header.index('Day')
        except ValueError as e:
            print(f"ERROR: Required column not found: {e}")
            return
        
        rows.append(header)
        
        # Process each row
        row_count = 0
        for row in reader:
            row_count += 1
            if len(row) > max(season_column_index, month_column_index, day_column_index):
                month = row[month_column_index]
                day = row[day_column_index]
                season = determine_season(month, day)
                row[season_column_index] = season
            rows.append(row)
    
    print(f"Total rows processed: {row_count}")
    
    # Count seasons
    rabi_count = sum(1 for row in rows[1:] if len(row) > season_column_index and row[season_column_index] == 'Rabi')
    kharif_count = sum(1 for row in rows[1:] if len(row) > season_column_index and row[season_column_index] == 'Kharif')
    empty_count = sum(1 for row in rows[1:] if len(row) > season_column_index and row[season_column_index] == '')
    
    print("\nSeason distribution:")
    print(f"  Rabi: {rabi_count}")
    print(f"  Kharif: {kharif_count}")
    print(f"  Empty: {empty_count}")
    
    # Write the updated CSV
    print(f"\nSaving updated file to: {output_file}")
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    
    print("Done! Season column has been populated successfully.")

if __name__ == "__main__":
    main()
