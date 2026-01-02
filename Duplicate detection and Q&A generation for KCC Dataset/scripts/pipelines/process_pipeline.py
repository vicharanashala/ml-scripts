#!/usr/bin/env python3
"""
Complete processing pipeline for Punjab Paddy data
"""
import csv
import os
import sys

# Anthropic API Key
API_KEY = "YOUR_ANTHROPIC_API_KEY_HERE"

def determine_season(month, day):
    """
    Determine the season based on month and day.
    
    Rabi season: Mid November to mid April/May (Nov 16 - May 15)
    Kharif season: June to October (Jun 1 - Oct 31)
    """
    if not month or not day or month == '' or day == '':
        return ""
    
    try:
        month = int(float(month))
        day = int(float(day))
    except (ValueError, TypeError):
        return ""
    
    # Kharif season: June to October (months 6-10)
    if 6 <= month <= 10:
        return "Kharif"
    
    # Rabi season: Mid November to mid May
    if month == 11 and day >= 16:
        return "Rabi"
    if 12 <= month <= 4:
        return "Rabi"
    if month == 5 and day <= 15:
        return "Rabi"
    
    return ""

def step1_filter_punjab_paddy():
    """
    Step 1: Filter rows with StateName=PUNJAB and Crop=Paddy Dhan
    """
    print("\n" + "="*60)
    print("STEP 1: Filtering Punjab Paddy data")
    print("="*60)
    
    input_file = 'Data/PB_Combined_Cleaned.csv'
    output_file = 'outputs/step1_filtered_punjab_paddy.csv'
    
    # Create output directory
    os.makedirs('outputs', exist_ok=True)
    
    print(f"Reading: {input_file}")
    
    filtered_rows = []
    total_rows = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        for row in reader:
            total_rows += 1
            # Filter for Punjab and Paddy Dhan
            if (row.get('StateName', '').strip().upper() == 'PUNJAB' and 
                'PADDY' in row.get('Crop', '').strip().upper() and 
                'DHAN' in row.get('Crop', '').strip().upper()):
                filtered_rows.append(row)
    
    print(f"Total rows read: {total_rows}")
    print(f"Filtered rows (Punjab + Paddy Dhan): {len(filtered_rows)}")
    
    # Write filtered data
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(filtered_rows)
    
    print(f"Saved to: {output_file}")
    return output_file

def step2_deduplicate(input_file, output_file):
    """
    Step 2: Run deduplication pipeline
    """
    print("\n" + "="*60)
    print("STEP 2: Running deduplication pipeline")
    print("="*60)
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    
    # Run the deduplication script
    cmd = f"cd /home/ubuntu/Kshitij/unique-qs && PYTHONPATH=/home/ubuntu/Kshitij/unique-qs venv/bin/python scripts/data_processing/deduplicate_questions.py --input {input_file} --output {output_file}"
    print(f"Running: {cmd}")
    
    result = os.system(cmd)
    if result != 0:
        print(f"ERROR: Deduplication failed with exit code {result}")
        sys.exit(1)
    
    print(f"Deduplication completed successfully")
    return output_file

def step3_generate_qa(input_file, output_file):
    """
    Step 3: Generate Questions and Answers for each row
    """
    print("\n" + "="*60)
    print("STEP 3: Generating Questions and Answers")
    print("="*60)
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    
    # Run the Q&A generation script with API key
    cmd = f"cd /home/ubuntu/Kshitij/unique-qs && PYTHONPATH=/home/ubuntu/Kshitij/unique-qs venv/bin/python scripts/qa_generation/generate_punjab_qa.py --input {input_file} --output {output_file} --api-key {API_KEY}"
    print(f"Running Q&A generation...")
    
    result = os.system(cmd)
    if result != 0:
        print(f"ERROR: Q&A generation failed with exit code {result}")
        sys.exit(1)
    
    print(f"Q&A generation completed successfully")
    return output_file

def step4_merge_files(file1, file2, output_file):
    """
    Step 4: Merge the generated Q&A file with PUNJAB_Paddy_Dhan.csv
    """
    print("\n" + "="*60)
    print("STEP 4: Merging files")
    print("="*60)
    
    print(f"File 1: {file1}")
    print(f"File 2: {file2}")
    print(f"Output: {output_file}")
    
    # Read both files
    rows = []
    header = None
    
    for input_file in [file1, file2]:
        print(f"Reading: {input_file}")
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if header is None:
                header = reader.fieldnames
            
            for row in reader:
                rows.append(row)
    
    print(f"Total rows after merge: {len(rows)}")
    
    # Write merged data
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Saved to: {output_file}")
    return output_file

def populate_season_column(file_path):
    """
    Populate the Season column based on Month and Day
    """
    print(f"\nPopulating Season column in: {file_path}")
    
    rows = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        for row in reader:
            month = row.get('Month', '')
            day = row.get('Day', '')
            row['Season'] = determine_season(month, day)
            rows.append(row)
    
    # Write back
    with open(file_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    
    # Count seasons
    rabi_count = sum(1 for row in rows if row.get('Season') == 'Rabi')
    kharif_count = sum(1 for row in rows if row.get('Season') == 'Kharif')
    empty_count = sum(1 for row in rows if row.get('Season') == '')
    
    print(f"Season distribution:")
    print(f"  Rabi: {rabi_count}")
    print(f"  Kharif: {kharif_count}")
    print(f"  Empty: {empty_count}")

def main():
    print("\n" + "="*60)
    print("PUNJAB PADDY DATA PROCESSING PIPELINE")
    print("="*60)
    
    # Step 1: Filter Punjab Paddy data
    step1_output = step1_filter_punjab_paddy()
    
    # Populate season for step 1 output
    populate_season_column(step1_output)
    
    # Step 2: Deduplicate filtered data
    step2_output = 'outputs/step2_deduplicated.csv'
    step2_deduplicate(step1_output, step2_output)
    
    # Populate season for step 2 output
    populate_season_column(step2_output)
    
    # Step 3: Generate Q&A
    step3_output = 'outputs/step3_with_qa.csv'
    step3_generate_qa(step2_output, step3_output)
    
    # Populate season for step 3 output
    populate_season_column(step3_output)
    
    # Step 4: Merge with existing PUNJAB_Paddy_Dhan.csv
    step4_output = 'outputs/step4_merged.csv'
    step4_merge_files(step3_output, 'Data/PUNJAB_Paddy_Dhan.csv', step4_output)
    
    # Populate season for step 4 output
    populate_season_column(step4_output)
    
    # Step 5: Final deduplication
    final_output = 'outputs/final_deduplicated_punjab_paddy.csv'
    step2_deduplicate(step4_output, final_output)
    
    # Populate season for final output
    populate_season_column(final_output)
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*60)
    print(f"\nFinal output: {final_output}")
    
    # Show final statistics
    with open(final_output, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        final_count = sum(1 for _ in reader)
    
    print(f"Final row count: {final_count}")

if __name__ == "__main__":
    main()
