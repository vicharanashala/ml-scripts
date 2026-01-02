#!/usr/bin/env python3
"""
GitHub Preparation Cleanup Script

Deletes unnecessary files and prepares project for GitHub push.
"""

import shutil
from pathlib import Path

def cleanup_for_github():
    """Delete unnecessary files and folders."""
    
    items_to_delete = []
    
    # 1. Large raw data folders (8.4GB+)
    large_data_folders = [
        'Data/All Datasets',
        'Data/UP Data', 
        'Data/State_Paddy',
    ]
    
    # 2. Large raw data files
    large_data_files = [
        'Data/UP_Combined.csv',
        'Data/UP_Wheat_Raw.csv',
    ]
    
    # 3. Temporary/partial Q&A files
    temp_qa_files = [
        'outputs/final/State_Paddy/MadhyaPradesh_Paddy_Remaining.csv',
        'outputs/qa_results/State_Paddy/MadhyaPradesh_Paddy_QA.csv',
        'outputs/qa_results/State_Paddy/MadhyaPradesh_Paddy_QA_Remaining.csv',
        'outputs/qa_results/UP_Paddy_QA_Batch_0_100.csv',
    ]
    
    # 4. Archived/test folders
    archived_folders = [
        'outputs/archived',
        'outputs/cross_dataset_analysis',
        'outputs/qa_results/test_archive',
        'outputs/reports',
    ]
    
    # 5. Utility scripts (move to archive, not delete)
    # Keep all scripts for now
    
    # Collect all items
    for folder in large_data_folders + archived_folders:
        if Path(folder).exists():
            items_to_delete.append(('folder', folder))
    
    for file in large_data_files + temp_qa_files:
        if Path(file).exists():
            items_to_delete.append(('file', file))
    
    # Execute deletions
    print('='*70)
    print('GITHUB PREPARATION: CLEANUP')
    print('='*70)
    print()
    
    deleted_count = 0
    freed_space = 0
    
    for item_type, item_path in items_to_delete:
        try:
            path = Path(item_path)
            
            if item_type == 'folder' and path.exists():
                # Calculate size before deletion
                size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
                shutil.rmtree(path)
                freed_space += size
                deleted_count += 1
                print(f'✓ Deleted folder: {item_path} ({size / 1024 / 1024 / 1024:.2f} GB)')
            
            elif item_type == 'file' and path.exists():
                size = path.stat().st_size
                path.unlink()
                freed_space += size
                deleted_count += 1
                print(f'✓ Deleted file: {item_path} ({size / 1024 / 1024:.1f} MB)')
        
        except Exception as e:
            print(f'✗ Failed to delete {item_path}: {e}')
    
    print()
    print('='*70)
    print(f'Cleanup complete!')
    print(f'  Items deleted: {deleted_count}')
    print(f'  Space freed: {freed_space / 1024 / 1024 / 1024:.2f} GB')
    print('='*70)

if __name__ == '__main__':
    cleanup_for_github()
