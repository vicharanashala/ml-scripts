#!/usr/bin/env python3
"""
Batch processing script to deduplicate all data files.
Processes both CSV and Excel files with the same configuration.
"""

import sys
from pathlib import Path

# Add parent directory to path to import deduplicate_questions
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from deduplicate_questions import QuestionDeduplicator, load_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Process all data files."""
    
    # Load configuration
    config = load_config('config.yaml')
    
    # Files to process
    files_to_process = [
        {
            'input': 'Data/AI_ANS_25K.csv',
            'output': 'Data/filtered/AI_ANS_25K_deduplicated.csv',
            'column': 'QueryText'
        },
        {
            'input': 'Data/UP 2025 Wheat Only Final 67k.xlsx',
            'output': 'Data/filtered/UP_2025_Wheat_67k_deduplicated.xlsx',
            'column': 'QueryText'
        }
    ]
    
    # Process each file
    for i, file_info in enumerate(files_to_process, 1):
        logger.info("="*70)
        logger.info(f"Processing file {i}/{len(files_to_process)}: {file_info['input']}")
        logger.info("="*70)
        
        # Check if file exists
        if not Path(file_info['input']).exists():
            logger.warning(f"File not found: {file_info['input']}, skipping...")
            continue
        
        try:
            # Create deduplicator
            deduplicator = QuestionDeduplicator(config)
            
            # Run deduplication
            df_result = deduplicator.deduplicate(
                input_file=file_info['input'],
                output_file=file_info['output'],
                question_column=file_info['column']
            )
            
            # Print report
            deduplicator.report.print_summary()
            
            # Save report
            report_file = f"{file_info['output']}.report.txt"
            deduplicator.report.save_to_file(report_file)
            
            logger.info(f"✓ Successfully processed {file_info['input']}")
            logger.info("")
            
        except Exception as e:
            logger.error(f"✗ Failed to process {file_info['input']}: {e}", exc_info=True)
            continue
    
    logger.info("="*70)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info("="*70)


if __name__ == "__main__":
    main()
