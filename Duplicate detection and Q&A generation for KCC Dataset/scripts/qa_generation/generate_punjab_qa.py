#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Punjab Question-Answer Generation Script

Generates English Question and Answer pairs from Punjabi QueryText and KccAns
using Anthropic's Claude AI Batch API for cost-effective processing.

Usage:
    python generate_punjab_qa.py [OPTIONS]
    
Options:
    --input, -i PATH        Path to input CSV file (default: Data/PB_Combined_Cleaned.csv)
    --output, -o PATH       Path to output CSV file (default: Data/PB_Combined_QA.csv)
    --rows, -r NUMBER       Number of rows to process (default: all)
    --api-key, -k KEY       Anthropic API key (required)
    --help, -h             Show this help message
    
Examples:
    # Process first 100 rows (for testing)
    python generate_punjab_qa.py --api-key YOUR_KEY --rows 100
    
    # Process all rows
    python generate_punjab_qa.py --api-key YOUR_KEY
    
    # Custom input/output
    python generate_punjab_qa.py -i Data/test.csv -o Data/test_qa.csv -k YOUR_KEY -r 500
"""

import nest_asyncio
nest_asyncio.apply()

import pandas as pd
import asyncio
import re
import json
import pytz
import secrets
import argparse
import sys
import os
from pathlib import Path
from anthropic import AsyncAnthropic
from datetime import timezone

def extract_json_from_text(text: str):
    """
    Cleans a text block that may be wrapped in Markdown code fences
    and tries to parse it as JSON.

    Returns:
        tuple (data, error)
        - data: parsed JSON (dict/list) if successful, else None
        - error: error message if parsing failed, else None
    """
    if not text or not isinstance(text, str):
        return None, "Invalid input: text must be a non-empty string"

    # Clean code fences like ```json ... ```
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.DOTALL).strip()

    # Attempt to parse as JSON
    try:
        data = json.loads(cleaned)
        return data, None
    except json.JSONDecodeError as e:
        return None, f"JSON parsing failed: {e}"

def print_batch_status(batch):
    """
    Nicely formats and prints an Anthropic MessageBatch object
    in IST with total time taken.
    """
    ist = pytz.timezone("Asia/Kolkata")

    def to_ist(dt):
        return dt.astimezone(ist).strftime("%d %b %Y, %I:%M:%S %p IST") if dt else "—"

    created = batch.created_at
    ended = batch.ended_at
    expires = batch.expires_at

    # Compute duration
    duration = (ended - created).total_seconds() if ended and created else None
    duration_str = f"{duration:.1f} seconds" if duration else "—"

    print("\n=== 📨 Message Batch Summary ===")
    print(f"Batch ID:          {batch.id}")
    print(f"Status:            {batch.processing_status.upper()}")
    print(f"Created At:        {to_ist(created)}")
    print(f"Ended At:          {to_ist(ended)}")
    print(f"Expires At:        {to_ist(expires)}")
    print(f"⏱️  Time Taken:     {duration_str}")
    print(f"Results URL:       {batch.results_url}")

    print("\n--- Request Counts ---")
    counts = batch.request_counts
    print(f"  ✅ Succeeded: {counts.succeeded}")
    print(f"  ⚠️  Errored:   {counts.errored}")
    print(f"  🚫 Canceled:  {counts.canceled}")
    print(f"  ⏳ Processing: {counts.processing}")
    print(f"  ⌛ Expired:    {counts.expired}")

    if batch.processing_status == "ended":
        print("\n✅ Batch processing has completed.")
    elif batch.processing_status == "in_progress":
        print("\n⏳ Batch is still processing.")
    else:
        print(f"\nℹ️ Current batch state: {batch.processing_status}")

    print("\n==============================\n")

    return batch.processing_status.upper() == "ENDED"

async def process_results(batch_id, original_df, output_filename):
    """Process batch results and save to CSV"""
    print(f"\n📥 Retrieving batch results...")
    result_stream = await client.messages.batches.results(batch_id)

    # Results come back in the same order as requests were sent
    results_list = []
    processed_count = 0

    async for entry in result_stream:
        processed_count += 1
        
        if entry.result.type != "succeeded":
            results_list.append({
                "Question": "",
                "primary_category": "",
                "Answer": ""
            })
            print(f"   ⚠️  Row {processed_count} failed")
            continue

        blocks = entry.result.message.content

        question = ""
        category = ""
        answer = ""

        for block in blocks:
            text = getattr(block, "text", None)
            if not text:
                continue

            m = re.search(r'\s*```json\s*({.*})\s*```', text, re.DOTALL)
            if not m:
                # Try parsing without code fences
                try:
                    parsed = json.loads(text.strip())
                    question = parsed.get("question", "").strip()
                    category = parsed.get("category", "").strip()
                    answer = parsed.get("answer", "").strip()
                    break
                except:
                    continue
            else:
                json_str = m.group(1).replace('{{', '{').replace('}}', '}')
                try:
                    parsed = json.loads(json_str)
                    question = parsed.get("question", "").strip()
                    category = parsed.get("category", "").strip()
                    answer = parsed.get("answer", "").strip()
                    break
                except json.JSONDecodeError:
                    continue

        results_list.append({
            "Question": question,
            "primary_category": category,
            "Answer": answer
        })
        
        if processed_count % 100 == 0:
            print(f"   Processed {processed_count} results...")

    print(f"✅ Retrieved {len(results_list)} results")

    # Add results to original dataframe (in order)
    original_df["Question"] = [r["Question"] for r in results_list]
    original_df["primary_category"] = [r["primary_category"] for r in results_list]
    original_df["Answer"] = [r["Answer"] for r in results_list]

    # Calculate success rate
    successful = sum(1 for r in results_list if r["Question"] and r["Answer"])
    success_rate = (successful / len(results_list)) * 100 if results_list else 0
    
    # Category distribution
    categories = {}
    for r in results_list:
        cat = r.get("primary_category", "Unknown")
        if cat:
            categories[cat] = categories.get(cat, 0) + 1
    
    print(f"\n📈 Processing Statistics:")
    print(f"   Total rows: {len(results_list)}")
    print(f"   Successful: {successful} ({success_rate:.1f}%)")
    print(f"   Failed: {len(results_list) - successful}")
    
    if categories:
        print(f"\n📊 Category Distribution:")
        for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            print(f"   {cat}: {count} ({count/len(results_list)*100:.1f}%)")

    print(f"\n💾 Saving results to {output_filename}...")
    original_df.to_csv(output_filename, index=False, encoding='utf-8')
    print(f"✅ Results saved successfully!")
    
    print(f"\n📁 Output file: {output_filename}")
    print(f"📊 Total rows: {len(original_df)}")
    print(f"📋 Columns: {', '.join(original_df.columns)}")

    return original_df

async def main(requests, df, output_filename, interval=10):
    """Main function to run batch processing"""
    # 1️⃣ Create the batch
    batch = await client.messages.batches.create(
        requests=requests
    )
    print("✅ Batch created:", batch.id)
    print(f"📝 Save this Batch ID in case you need to retrieve results later!")
    
    # 2️⃣ Poll for completion
    while True:
        batch = await client.messages.batches.retrieve(batch.id)
        completed = print_batch_status(batch)
        if completed:
            break
        await asyncio.sleep(interval)
    
    # 3️⃣ Process and save results
    await process_results(batch.id, df, output_filename)

# ============================================================================
# MAIN SCRIPT EXECUTION
# ============================================================================

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate English Q&A pairs from Punjabi agricultural data using Claude AI Batch API.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --api-key YOUR_KEY --rows 100
  %(prog)s -k YOUR_KEY
  %(prog)s -i Data/test.csv -o Data/test_qa.csv -k YOUR_KEY -r 500
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        type=str,
        default='../../Data/PB_Combined_Cleaned.csv',
        help='Path to input CSV file (default: ../../Data/PB_Combined_Cleaned.csv)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='../../outputs/qa_results/PB_Combined_QA.csv',
        help='Path to output CSV file (default: ../../outputs/qa_results/PB_Combined_QA.csv)'
    )
    
    parser.add_argument(
        '--rows', '-r',
        type=str,
        default='all',
        help='Number of rows to process (default: all). Use "all" to process entire dataset or a number like 100, 500, etc.'
    )
    
    parser.add_argument(
        '--api-key', '-k',
        type=str,
        required=True,
        help='Anthropic API key (required)'
    )

    parser.add_argument(
        '--start-index',
        type=int,
        default=0,
        help='Start index for processing (0-based, inclusive, default: 0)'
    )
    
    parser.add_argument(
        '--end-index',
        type=int,
        default=None,
        help='End index for processing (0-based, exclusive, default: None)'
    )
    
    args = parser.parse_args()
    
    # Parse rows argument
    if args.rows.lower() == 'all':
        num_rows = None
    else:
        try:
            num_rows = int(args.rows)
            if num_rows <= 0:
                parser.error("--rows must be a positive number or 'all'")
        except ValueError:
            parser.error(f"--rows must be a number or 'all', got: {args.rows}")
    
    return args.input, args.output, num_rows, args.api_key, args.start_index, args.end_index

if __name__ == "__main__":
    # Parse command-line arguments
    INPUT_FILE, OUTPUT_FILE, NUM_QUESTIONS, API_KEY, START_INDEX, END_INDEX = parse_arguments()
    
    # Display configuration
    print("\n" + "="*70)
    print("PUNJAB Q&A GENERATION - CONFIGURATION")
    print("="*70)
    print(f"📂 Input File:  {INPUT_FILE}")
    print(f"💾 Output File: {OUTPUT_FILE}")
    print(f"📊 Rows:        {'ALL' if NUM_QUESTIONS is None else NUM_QUESTIONS}")
    print(f"🔑 API Key:     {'*' * 20}{API_KEY[-8:] if len(API_KEY) > 8 else '***'}")
    print("="*70 + "\n")
    
    # Validate input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: Input file not found: {INPUT_FILE}")
        print(f"   Please check the path and try again.")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(OUTPUT_FILE)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"📁 Created output directory: {output_dir}\n")
    
    # Initialize client
    client = AsyncAnthropic(api_key=API_KEY)
    
    # Load CSV file
    print(f"📂 Loading data from {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    # Standardize column names if needed
    df.rename(columns={'year': 'Year', 'month': 'Month'}, inplace=True)
    
    print(f"✅ Total rows in dataset: {len(df)}")
    print("\nAvailable columns:")
    for col in df.columns:
        print(f"  - {col}")
    
    # System prompt for translation, classification, and Q&A generation
    system_prompt = '''
You are an AI expert helping Indian farmers by translating, classifying, and generating structured agricultural question-answer pairs.

Your task is to:
1. TRANSLATION/REFINEMENT (CRITICAL):
   - The input question may be in Punjabi, Hindi, or English.
   - If in Punjabi or Hindi: Translate it into clear, professional English.
   - If in English: Refine the grammar and clarity if needed.
2. Classify the question into ONE category: Disease, Pest, Fertilizer and Nutrient, or Variety
3. Generate a comprehensive English answer following the template structure for that category

CLASSIFICATION CATEGORIES:
- Disease: Plant diseases, fungal/bacterial/viral infections
- Pest: Insects, pests, pest damage, pest control
- Fertilizer and Nutrient: Fertilizers, nutrients, soil health, deficiencies
- Variety: Crop varieties, seed selection, variety characteristics

ENHANCED ANSWER STRUCTURE BY CATEGORY:

Disease: 
1. Description 
2. Symptoms 
3. Severity 
4. Control (Severe: chemicals + PHI, Initial: biological + cultural practices)
5. PROTECTIVE GEAR (if chemicals mentioned): gloves, mask, protective clothing, eye protection, safe handling
6. SAFETY DISCLAIMER (if chemicals mentioned): follow doses, observe PHI, safe application
8. REFERRAL: "For further guidance or clarification, farmers are advised to contact the nearest Krishi Vigyan Kendra (KVK) or local agricultural extension officer."

Pest: 
1. Description 
2. Damage symptoms 
3. Severity 
4. Control (Severe: insecticides + PHI, Initial: biological + cultural practices)
5. PROTECTIVE GEAR (if insecticides mentioned): gloves, mask, protective clothing, eye protection, safe handling
6. SAFETY DISCLAIMER (if insecticides mentioned): follow doses, observe PHI, safe application
8. REFERRAL: "For further guidance or clarification, farmers are advised to contact the nearest Krishi Vigyan Kendra (KVK) or local agricultural extension officer."

Fertilizer and Nutrient: 
1. Intro 
2. Soil test 
3. Organic (FYM/vermicompost/green manure) 
4. NPK (doses, splits, sources) 
5. Micronutrients (Zn, B, Fe) 
6. PROTECTIVE GEAR (for chemical fertilizers): gloves, mask, protective clothing, safe handling and storage
7. SAFETY GUIDELINES: proper doses, application timing, environmental safety
9. REFERRAL: "For further guidance or clarification, farmers are advised to contact the nearest Krishi Vigyan Kendra (KVK) or local agricultural extension officer."

Variety: 
1. Region 
2. Season 
3. Water requirements 
4. Varieties (2-3 recommended) 
5. Duration 
6. Seed rate 
7. Spacing 
8. Yield 
9. Traits 
10. Disease/Pest resistance 
11. Seed source 
13. REFERRAL: "For further guidance or clarification, farmers are advised to contact the nearest Krishi Vigyan Kendra (KVK) or local agricultural extension officer."

MANDATORY SAFETY REQUIREMENTS:


2. PROTECTIVE GEAR (MANDATORY when mentioning chemicals - pesticides, fungicides, insecticides, chemical fertilizers):
   - Chemical-resistant gloves
   - Face mask or respirator
   - Protective clothing (long sleeves, full pants)
   - Eye protection (goggles or face shield)
   - Safe handling: wash hands after use, avoid eating/drinking during application
   - Safe storage: keep away from children, animals, and food items

3. SAFETY DISCLAIMER (MANDATORY for chemical applications):
   - Follow recommended doses strictly - do not exceed
   - Observe Pre-Harvest Interval (PHI) - mention specific days
   - Avoid spraying during windy conditions or rain
   - Do not mix different chemicals unless specified
   - Proper disposal of empty containers
   - Keep emergency contact numbers handy

4. REFERRAL FOOTER (MANDATORY FOR ALL ANSWERS):
   - Every answer MUST end with: "For further guidance or clarification, farmers are advised to contact the nearest Krishi Vigyan Kendra (KVK) or local agricultural extension officer."

IMPORTANT GUIDELINES:
- Translate accurately from Punjabi to English
- Classify into exactly ONE category
- Follow the ENHANCED answer structure for that category
- Make the answer comprehensive (400-600 words to accommodate safety sections)
- Include practical details: timing, quantities, methods, precautions
- Use specific product names, varieties, and practices for Punjab
- Keep regional context (Punjab, India) in mind
- Use simple, farmer-friendly language
- Structure the answer in clear paragraphs
- NEVER skip protective gear section when chemicals are mentioned
- ALWAYS end with referral footer

CHEMICAL NAMING REQUIREMENTS (CRITICAL):
- ALWAYS use chemical compound names (generic/active ingredient names)
- NEVER use brand names or trade names for chemicals
- Examples:
  * Use "Chlorpyrifos 20% EC" NOT "Dursban" or "Coroban"
  * Use "Mancozeb 75% WP" NOT "Dithane M-45" or "Indofil"
  * Use "Imidacloprid 17.8% SL" NOT "Confidor" or "Admire"
  * Use "Carbendazim 50% WP" NOT "Bavistin" or "Derosal"
  * Use "Glyphosate 41% SL" NOT "Roundup" or "Glycel"
- Include concentration and formulation type (EC, WP, SL, etc.)
- This applies to all pesticides, fungicides, insecticides, herbicides, and chemical fertilizers

Topics: crops, soil, pests, fertilizers, irrigation, climate, farm machinery, weather, and government schemes.

FORMAT:
- Output ONLY valid JSON with keys: question, category, answer
- question: Clear English question
- category: ONE of (Disease, Pest, Fertilizer and Nutrient, Variety)
- answer: Comprehensive answer (400-600 words) including:
  * Category-specific template structure
  * Protective gear section (if chemicals mentioned)
  * Safety disclaimer (if chemicals mentioned)
  * Referral footer (mandatory)
- Do not include any explanatory text outside the JSON
'''
    
    # Prepare batch requests
    # Determine how many rows to process
    if END_INDEX is not None:
        print(f"\n🔧 Preparing range: {START_INDEX} to {END_INDEX} (exclusive)")
        rows_to_process = df.iloc[START_INDEX:END_INDEX]
    elif NUM_QUESTIONS:
        print(f"\n🔧 Preparing first {NUM_QUESTIONS} rows starting from {START_INDEX}")
        rows_to_process = df.iloc[START_INDEX:START_INDEX+NUM_QUESTIONS]
    else:
        print(f"\n🔧 Preparing all rows starting from {START_INDEX}")
        rows_to_process = df.iloc[START_INDEX:]
    
    requests = []
    
    for idx, row in rows_to_process.iterrows():
        user_message = f"""
Translate, classify, and generate an English question-answer pair from the following agricultural data.
The input may be in Punjabi, Hindi, or English.

Original Query Text: {row['QueryText']}
Original Answer (KccAns): {row['KccAns']}

Context Information:
- State: {row['StateName']}
- District: {row['DistrictName']}
- Block: {row['BlockName']}
- Crop: {row['Crop']}
- Category: {row['Category']}
- Query Type: {row['QueryType']}
- Season: {row['Season']}
- Sector: {row['Sector']}
- Year: {row['Year']}
- Month: {row['Month']}

Instructions:
1. Translate/Refine the QueryText into a clear, well-formed English question
2. Classify the question into ONE category (Disease, Pest, Fertilizer and Nutrient, or Variety)
3. Generate a comprehensive answer (300-500 words) following the template structure for that category
4. Make the answer educational and practical for farmers
5. Include specific details about methods, timing, quantities, and best practices
6. Keep the regional context (e.g. State provided) in mind

Output format:
```json
{{
  "question": "Your translated and refined English question here",
  "category": "Disease|Pest|Fertilizer and Nutrient|Variety",
  "answer": "Your comprehensive answer following the template structure for the category (300-500 words)"
}}
```

Generate the JSON response now.
"""
        
        unique_id = secrets.token_hex(5)
        req_id = f"pb_{unique_id}"
        
        requests.append({
            "custom_id": req_id,
            "params": {
                "model": "claude-sonnet-4-5-20250929",
                "max_tokens": 4000,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_message}],
            },
        })
    
    print(f"✅ Prepared {len(requests)} requests")
    
    # Run the batch processing
    print(f"\n🚀 Starting batch processing...")
    print(f"📊 Output will be saved to: {OUTPUT_FILE}")
    print(f"⚠️  Note: Processing {len(requests)} questions may take considerable time.")
    print(f"⚠️  Estimated time: {len(requests) * 1.5 / 60:.1f} minutes (approximately)")
    print(f"\n💡 Tip: The batch will continue processing even if you close this script.")
    print(f"    You can retrieve results later using the Batch ID shown above.\n")
    
    asyncio.run(main(requests, rows_to_process.copy(), OUTPUT_FILE))
    
    print("\n" + "="*70)
    print("✅ PROCESSING COMPLETE!")
    print("="*70)
    print(f"📁 Output saved to: {OUTPUT_FILE}")
    print("="*70)
