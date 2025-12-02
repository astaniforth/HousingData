#!/usr/bin/env python3
import json

with open('run_workflow.ipynb', 'r') as f:
    nb = json.load(f)

# Find cells with date extraction logic
keywords = ['earliest', 'dob_bin', 'dob_bbl', '_get_earliest', 'application_date', 'get_application']
found_cells = []

for i, cell in enumerate(nb['cells']):
    if cell.get('cell_type') == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])
        if any(kw in source_text.lower() for kw in keywords):
            found_cells.append((i, cell))

print(f"Found {len(found_cells)} cells with date extraction logic\n")

for i, (cell_idx, cell) in enumerate(found_cells[:5]):
    print(f"=" * 70)
    print(f"CELL {cell_idx}")
    print("=" * 70)
    source_lines = cell['source']
    # Show first 100 lines
    for line in source_lines[:100]:
        print(line, end='')
    if len(source_lines) > 100:
        print(f"\n... ({len(source_lines) - 100} more lines)")
    print("\n")

