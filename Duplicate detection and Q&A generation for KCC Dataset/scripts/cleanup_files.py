#!/usr/bin/env python3
"""
Cleanup Script - Delete unnecessary files and folders
"""

import shutil
from pathlib import Path

def delete_unnecessary_files():
    """Delete unnecessary files and folders."""
    
    items_to_delete = []
    
    # 1. Duplicate group folders (large, not needed for final output)
    group_folders = [
        'Data/groups/AndhraPradesh_Paddy_Raw',
        'Data/groups/Bihar_Paddy_Raw',
        'Data/groups/Chhattisgarh_Paddy_Raw',
        'Data/groups/Haryana_Paddy_Raw',
        'Data/groups/Karnataka_Paddy_Raw',
        'Data/groups/MadhyaPradesh_Paddy_Raw',
        'Data/groups/Maharashtra_Paddy_Raw',
        'Data/groups/Odisha_Paddy_Raw',
        'Data/groups/TamilNadu_Paddy_Raw',
        'Data/groups/Telangana_Paddy_Raw',
        'Data/groups/Uttarakhand_Paddy_Raw',
        'Data/groups/WestBengal_Paddy_Raw',
        'Data/groups/UP_Wheat_Raw',
        'Data/groups/UP_filtered',
        'Data/groups/step1_filtered_punjab_paddy',
        'Data/groups/step4_merged',
    ]
    
    # 2. Report files (.report.txt)
    report_files = list(Path('outputs/final').rglob('*.report.txt'))
    
    # 3. Temporary/intermediate data files
    temp_files = [
        'Data/UP_Paddy_Generated.csv',
        'Data/UP_Paddy_GDB_Reference.csv',
    ]
    
    # Collect all items
    for folder in group_folders:
        if Path(folder).exists():
            items_to_delete.append(('folder', folder))
    
    for report in report_files:
        items_to_delete.append(('file', str(report)))
    
    for temp in temp_files:
        if Path(temp).exists():
            items_to_delete.append(('file', temp))
    
    # Execute deletions
    print('='*70)
    print('CLEANUP: DELETING UNNECESSARY FILES AND FOLDERS')
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
                print(f'✓ Deleted folder: {item_path} ({size / 1024 / 1024:.1f} MB)')
            
            elif item_type == 'file' and path.exists():
                size = path.stat().st_size
                path.unlink()
                freed_space += size
                deleted_count += 1
                print(f'✓ Deleted file: {item_path}')
        
        except Exception as e:
            print(f'✗ Failed to delete {item_path}: {e}')
    
    print()
    print('='*70)
    print(f'Cleanup complete!')
    print(f'  Items deleted: {deleted_count}')
    print(f'  Space freed: {freed_space / 1024 / 1024:.1f} MB')
    print('='*70)

if __name__ == '__main__':
    delete_unnecessary_files()
