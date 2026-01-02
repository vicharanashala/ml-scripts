#!/usr/bin/env python3
"""
File Organization Script

Organizes project files into proper folder structure with sub-folders.
"""

import shutil
from pathlib import Path

def create_directory_structure():
    """Create the organized directory structure."""
    directories = [
        # outputs/final subdirectories
        'outputs/final/UP_Data',
        'outputs/final/State_Paddy',
        'outputs/final/Cross_Duplicates',
        'outputs/final/Punjab_Data',
        
        # outputs/qa_results subdirectories
        'outputs/qa_results/UP_Data',
        'outputs/qa_results/State_Paddy',
    ]
    
    for dir_path in directories:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f'✓ Created: {dir_path}')

def organize_files():
    """Move files to their appropriate directories."""
    
    moves = []
    
    # UP Paddy files (outputs/final)
    up_paddy_files = [
        'UP_Unique_Filtered.csv',
        'UP_Unique_Paddy.csv',
    ]
    for f in up_paddy_files:
        src = f'outputs/final/{f}'
        dst = f'outputs/final/UP_Data/{f}'
        if Path(src).exists():
            moves.append((src, dst))
    
    # UP Wheat files (outputs/final)
    up_wheat_files = [
        'UP_Wheat_Unique.csv',
    ]
    for f in up_wheat_files:
        src = f'outputs/final/{f}'
        dst = f'outputs/final/UP_Data/{f}'
        if Path(src).exists():
            moves.append((src, dst))
    
    # State Paddy files (12 states)
    states = [
        'TamilNadu', 'WestBengal', 'Odisha', 'Haryana', 'Bihar',
        'MadhyaPradesh', 'Chhattisgarh', 'AndhraPradesh', 'Telangana',
        'Uttarakhand', 'Karnataka', 'Maharashtra'
    ]
    for state in states:
        src = f'outputs/final/{state}_Paddy_Unique.csv'
        dst = f'outputs/final/State_Paddy/{state}_Paddy_Unique.csv'
        if Path(src).exists():
            moves.append((src, dst))
    
    # Cross-duplicate files
    cross_dup_files = [
        'UP_Paddy_Duplicate_Check.csv',
        'UP_Paddy_Duplicates_Found.csv',
        'UP_Paddy_Unique_ToProcess.csv',
    ]
    for f in cross_dup_files:
        src = f'outputs/final/{f}'
        dst = f'outputs/final/Cross_Duplicates/{f}'
        if Path(src).exists():
            moves.append((src, dst))
    
    # Punjab/Paddy legacy files
    punjab_files = [
        'final_deduplicated_punjab_paddy.csv',
        'final_paddy_relevant_only.csv',
        'final_paddy_relevant_only_non_paddy_removed.csv',
    ]
    for f in punjab_files:
        src = f'outputs/final/{f}'
        dst = f'outputs/final/Punjab_Data/{f}'
        if Path(src).exists():
            moves.append((src, dst))
    
    # Q&A Results - UP Data
    qa_up_files = [
        'UP_Paddy_QA_Full_NoSource.csv',
        'UP_Paddy_QA_Batch_0_100.csv',
        'UP_Wheat_QA_Full.csv',
    ]
    for f in qa_up_files:
        src = f'outputs/qa_results/{f}'
        dst = f'outputs/qa_results/UP_Data/{f}'
        if Path(src).exists():
            moves.append((src, dst))
    
    # Execute moves
    print(f'\n{"="*70}')
    print(f'Moving {len(moves)} files...')
    print(f'{"="*70}\n')
    
    for src, dst in moves:
        try:
            shutil.move(src, dst)
            print(f'✓ {Path(src).name} → {Path(dst).parent.name}/')
        except Exception as e:
            print(f'✗ Failed to move {src}: {e}')
    
    print(f'\n{"="*70}')
    print(f'File organization complete!')
    print(f'{"="*70}')

def print_summary():
    """Print summary of organized structure."""
    print('\n' + '='*70)
    print('ORGANIZED STRUCTURE SUMMARY')
    print('='*70)
    
    structure = {
        'outputs/final/UP_Data': 'UP Paddy and Wheat unique datasets',
        'outputs/final/State_Paddy': '12 state-wise Paddy unique datasets',
        'outputs/final/Cross_Duplicates': 'UP Paddy cross-duplicate analysis',
        'outputs/final/Punjab_Data': 'Legacy Punjab/Paddy datasets',
        'outputs/qa_results/UP_Data': 'UP Paddy and Wheat Q&A results',
    }
    
    for path, desc in structure.items():
        if Path(path).exists():
            file_count = len(list(Path(path).glob('*.csv')))
            print(f'\n{path}/')
            print(f'  {desc}')
            print(f'  Files: {file_count}')

if __name__ == '__main__':
    print('='*70)
    print('PROJECT FILE ORGANIZATION')
    print('='*70)
    print()
    
    # Create directory structure
    print('Creating directory structure...')
    create_directory_structure()
    
    # Organize files
    print('\nOrganizing files...')
    organize_files()
    
    # Print summary
    print_summary()
    
    print('\n✓ Organization complete!')
