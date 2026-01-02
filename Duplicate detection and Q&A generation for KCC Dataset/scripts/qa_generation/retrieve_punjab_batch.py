#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retrieve Punjab Q&A Batch Results

Use this script to retrieve results from a previously created batch
if the main script was interrupted or you want to check on a batch later.

Usage:
    python retrieve_punjab_batch.py --batch-id YOUR_BATCH_ID --api-key YOUR_API_KEY [OPTIONS]
    
Options:
    --batch-id, -b ID       Batch ID to retrieve (required)
    --api-key, -k KEY       Anthropic API key (required)
    --input, -i PATH        Original input CSV file (default: Data/PB_Combined_Cleaned.csv)
    --output, -o PATH       Output CSV file (default: Data/PB_Combined_QA_Retrieved.csv)
    --rows, -r NUMBER       Number of rows that were processed (default: all)
"""

import nest_asyncio
nest_asyncio.apply()

import pandas as pd
import asyncio
import re
import json
import argparse
import sys
import os
from anthropic import AsyncAnthropic

async def process_results(client, batch_id, original_df, output_filename):
    """Process batch results and save to CSV"""
    print(f"\n📥 Retrieving batch results for batch: {batch_id}...")
    
    # First check batch status
    batch = await client.messages.batches.retrieve(batch_id)
    print(f"\n📊 Batch Status: {batch.processing_status.upper()}")
    print(f"   Succeeded: {batch.request_counts.succeeded}")
    print(f"   Errored: {batch.request_counts.errored}")
    print(f"   Processing: {batch.request_counts.processing}")
    
    if batch.processing_status != "ended":
        print(f"\n⚠️  Warning: Batch is not yet complete (status: {batch.processing_status})")
        print(f"   You may want to wait and try again later.")
        response = input("\nDo you want to retrieve partial results anyway? (y/n): ")
        if response.lower() != 'y':
            print("Exiting...")
            sys.exit(0)
    
    result_stream = await client.messages.batches.results(batch_id)

    # Results come back in the same order as requests were sent
    results_list = []
    processed_count = 0

    async for entry in result_stream:
        processed_count += 1
        
        if entry.result.type != "succeeded":
            results_list.append({
                "Question": "",
                "Answer": ""
            })
            print(f"   ⚠️  Row {processed_count} failed")
            continue

        blocks = entry.result.message.content

        question = ""
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
                    answer = parsed.get("answer", "").strip()
                    break
                except:
                    continue
            else:
                json_str = m.group(1).replace('{{', '{').replace('}}', '}')
                try:
                    parsed = json.loads(json_str)
                    question = parsed.get("question", "").strip()
                    answer = parsed.get("answer", "").strip()
                    break
                except json.JSONDecodeError:
                    continue

        results_list.append({
            "Question": question,
            "Answer": answer
        })
        
        if processed_count % 100 == 0:
            print(f"   Processed {processed_count} results...")

    print(f"✅ Retrieved {len(results_list)} results")

    # Add results to original dataframe (in order)
    original_df["Question"] = [r["Question"] for r in results_list]
    original_df["Answer"] = [r["Answer"] for r in results_list]

    # Calculate success rate
    successful = sum(1 for r in results_list if r["Question"] and r["Answer"])
    success_rate = (successful / len(results_list)) * 100 if results_list else 0
    
    print(f"\n📈 Processing Statistics:")
    print(f"   Total rows: {len(results_list)}")
    print(f"   Successful: {successful} ({success_rate:.1f}%)")
    print(f"   Failed: {len(results_list) - successful}")

    print(f"\n💾 Saving results to {output_filename}...")
    original_df.to_csv(output_filename, index=False, encoding='utf-8')
    print(f"✅ Results saved successfully!")
    
    print(f"\n📁 Output file: {output_filename}")
    print(f"📊 Total rows: {len(original_df)}")

    return original_df

async def main(client, batch_id, df, output_filename):
    """Main function to retrieve batch results"""
    await process_results(client, batch_id, df, output_filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Retrieve results from a Punjab Q&A batch processing job.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--batch-id', '-b',
        type=str,
        required=True,
        help='Batch ID to retrieve (required)'
    )
    
    parser.add_argument(
        '--api-key', '-k',
        type=str,
        required=True,
        help='Anthropic API key (required)'
    )
    
    parser.add_argument(
        '--input', '-i',
        type=str,
        default='../../Data/PB_Combined_Cleaned.csv',
        help='Original input CSV file (default: ../../Data/PB_Combined_Cleaned.csv)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='../../outputs/qa_results/PB_Combined_QA_Retrieved.csv',
        help='Output CSV file (default: ../../outputs/qa_results/PB_Combined_QA_Retrieved.csv)'
    )
    
    parser.add_argument(
        '--rows', '-r',
        type=str,
        default='all',
        help='Number of rows that were processed (default: all)'
    )
    
    args = parser.parse_args()
    
    # Parse rows argument
    if args.rows.lower() == 'all':
        num_rows = None
    else:
        try:
            num_rows = int(args.rows)
        except ValueError:
            print(f"❌ Error: --rows must be a number or 'all', got: {args.rows}")
            sys.exit(1)
    
    # Display configuration
    print("\n" + "="*70)
    print("RETRIEVE PUNJAB Q&A BATCH RESULTS")
    print("="*70)
    print(f"🆔 Batch ID:    {args.batch_id}")
    print(f"📂 Input File:  {args.input}")
    print(f"💾 Output File: {args.output}")
    print(f"📊 Rows:        {'ALL' if num_rows is None else num_rows}")
    print("="*70 + "\n")
    
    # Validate input file exists
    if not os.path.exists(args.input):
        print(f"❌ Error: Input file not found: {args.input}")
        sys.exit(1)
    
    # Initialize client
    client = AsyncAnthropic(api_key=args.api_key)
    
    # Load CSV file
    print(f"📂 Loading original data from {args.input}...")
    df = pd.read_csv(args.input)
    
    # Get the subset that was processed
    if num_rows:
        df = df.head(num_rows)
    
    print(f"✅ Loaded {len(df)} rows")
    
    # Run retrieval
    asyncio.run(main(client, args.batch_id, df, args.output))
    
    print("\n" + "="*70)
    print("✅ RETRIEVAL COMPLETE!")
    print("="*70)
    print(f"📁 Output saved to: {args.output}")
    print("="*70)
