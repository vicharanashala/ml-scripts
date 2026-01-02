#!/usr/bin/env python3
"""
Filter Paddy-Relevant Questions using Claude Batch API

This script uses Claude's Batch API to classify whether questions are truly
related to Paddy (Dhan/Rice) cultivation, even if the Crop column says "Paddy Dhan".
"""

import csv
import json
import time
from pathlib import Path
from anthropic import Anthropic

# API Configuration
API_KEY = "YOUR_ANTHROPIC_API_KEY_HERE"

def create_classification_prompt(question):
    """Create a prompt to classify if a question is about Paddy/Rice cultivation"""
    return f"""You are an agricultural expert specializing in crop classification. 

Analyze the following question and determine if it is specifically related to Paddy (also known as Dhan or Rice) cultivation.

Question: "{question}"

A question is considered PADDY-RELATED if it discusses:
- Rice/Paddy/Dhan cultivation, planting, or harvesting
- Rice nursery management
- Paddy field preparation, transplanting, or water management
- Pests, diseases, or weeds specific to rice/paddy crops
- Fertilizers, nutrients, or soil management for rice/paddy
- Rice varieties or seed treatment
- Any aspect of rice/paddy farming

A question is considered NOT PADDY-RELATED if it discusses:
- Other crops (wheat, maize, cotton, vegetables, etc.) without mentioning paddy
- General weather information without crop-specific context
- Government schemes or general farming advice not specific to paddy
- Livestock, poultry, or non-crop agriculture
- Other topics unrelated to paddy cultivation

Respond with ONLY ONE WORD:
- "YES" if the question is about Paddy/Rice cultivation
- "NO" if the question is NOT about Paddy/Rice cultivation

Your response:"""

def create_batch_requests(input_file):
    """Create batch API requests for all questions"""
    print(f"\n📖 Reading input file: {input_file}")
    
    requests = []
    rows = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            rows.append(row)
            question = row.get('Question', '') or row.get('QueryText', '')
            
            if question:
                request = {
                    "custom_id": f"classify_{idx}",
                    "params": {
                        "model": "claude-sonnet-4-5-20250929",
                        "max_tokens": 10,
                        "temperature": 0,
                        "messages": [
                            {
                                "role": "user",
                                "content": create_classification_prompt(question)
                            }
                        ]
                    }
                }
                requests.append(request)
    
    print(f"✅ Created {len(requests)} classification requests")
    return requests, rows

def submit_batch(client, requests):
    """Submit batch requests to Claude API"""
    print(f"\n📤 Submitting batch to Claude API...")
    
    # Create batch file
    batch_file = "outputs/classification_batch.jsonl"
    with open(batch_file, 'w') as f:
        for request in requests:
            f.write(json.dumps(request) + '\n')
    
    print(f"💾 Saved batch requests to: {batch_file}")
    
    # Submit batch
    message_batch = client.messages.batches.create(
        requests=requests
    )
    
    print(f"\n✅ Batch submitted successfully!")
    print(f"📋 Batch ID: {message_batch.id}")
    print(f"📊 Status: {message_batch.processing_status}")
    print(f"🔢 Request count: {message_batch.request_counts}")
    
    return message_batch

def wait_for_batch(client, batch_id):
    """Wait for batch processing to complete"""
    print(f"\n⏳ Waiting for batch processing to complete...")
    
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        status = batch.processing_status
        counts = batch.request_counts
        
        print(f"\r📊 Status: {status} | "
              f"✅ {counts.succeeded}/{counts.processing + counts.succeeded} | "
              f"⏳ Processing: {counts.processing} | "
              f"⚠️  Errors: {counts.errored}", end='', flush=True)
        
        if status == "ended":
            print(f"\n\n✅ Batch processing completed!")
            print(f"   Succeeded: {counts.succeeded}")
            print(f"   Errored: {counts.errored}")
            print(f"   Canceled: {counts.canceled}")
            return batch
        
        time.sleep(5)

def retrieve_results(client, batch_id):
    """Retrieve and parse batch results"""
    print(f"\n📥 Retrieving batch results...")
    
    results = {}
    for result in client.messages.batches.results(batch_id):
        custom_id = result.custom_id
        
        if result.result.type == "succeeded":
            response_text = result.result.message.content[0].text.strip().upper()
            is_paddy_related = response_text == "YES"
            results[custom_id] = is_paddy_related
        else:
            # If classification failed, assume it's paddy-related (conservative approach)
            results[custom_id] = True
    
    print(f"✅ Retrieved {len(results)} results")
    return results

def filter_and_save(rows, results, output_file):
    """Filter rows based on classification results and save"""
    print(f"\n🔍 Filtering rows based on classification...")
    
    filtered_rows = []
    non_paddy_rows = []
    
    for idx, row in enumerate(rows):
        custom_id = f"classify_{idx}"
        is_paddy_related = results.get(custom_id, True)  # Default to True if not found
        
        if is_paddy_related:
            filtered_rows.append(row)
        else:
            non_paddy_rows.append(row)
    
    # Save filtered (paddy-related) rows
    if filtered_rows:
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(filtered_rows)
        print(f"✅ Saved {len(filtered_rows)} paddy-related rows to: {output_file}")
    
    # Save non-paddy rows for reference
    non_paddy_file = output_file.replace('.csv', '_non_paddy_removed.csv')
    if non_paddy_rows:
        with open(non_paddy_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(non_paddy_rows)
        print(f"📋 Saved {len(non_paddy_rows)} non-paddy rows to: {non_paddy_file}")
    
    return len(filtered_rows), len(non_paddy_rows)

def main():
    print("="*70)
    print("PADDY QUESTION RELEVANCE FILTER - Using Claude Batch API")
    print("="*70)
    
    # Configuration
    input_file = "outputs/final_deduplicated_punjab_paddy.csv"
    output_file = "outputs/final_paddy_relevant_only.csv"
    
    # Initialize Anthropic client
    client = Anthropic(api_key=API_KEY)
    
    # Step 1: Create batch requests
    requests, rows = create_batch_requests(input_file)
    
    # Step 2: Submit batch
    batch = submit_batch(client, requests)
    
    # Step 3: Wait for completion
    completed_batch = wait_for_batch(client, batch.id)
    
    # Step 4: Retrieve results
    results = retrieve_results(client, batch.id)
    
    # Step 5: Filter and save
    paddy_count, non_paddy_count = filter_and_save(rows, results, output_file)
    
    # Summary
    print("\n" + "="*70)
    print("FILTERING COMPLETE!")
    print("="*70)
    print(f"📊 Original rows:        {len(rows)}")
    print(f"✅ Paddy-related:        {paddy_count} ({paddy_count/len(rows)*100:.1f}%)")
    print(f"❌ Non-paddy (removed):  {non_paddy_count} ({non_paddy_count/len(rows)*100:.1f}%)")
    print(f"\n📁 Output file: {output_file}")
    print("="*70)

if __name__ == "__main__":
    main()
