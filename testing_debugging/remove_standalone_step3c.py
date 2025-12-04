#!/usr/bin/env python3
"""
Remove the standalone Step 3C cells since address fallback is now integrated into cell 13
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Read notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

print(f"Notebook has {len(notebook['cells'])} cells")

# Find and remove Step 3C cells
cells_to_remove = []
for i, cell in enumerate(notebook['cells']):
    # Check for Step 3C markdown or code cells
    if cell.get('cell_type') == 'markdown':
        source = cell.get('source', [])
        if isinstance(source, list):
            source = ''.join(source)
        if 'Step 3C' in source and 'Address-Based Fallback' in source:
            print(f"Found Step 3C markdown cell at index {i}")
            cells_to_remove.append(i)
    
    elif cell.get('cell_type') == 'code':
        source = cell.get('source', [])
        if isinstance(source, list):
            source = ''.join(source)
        if 'TIER 3: ADDRESS-BASED FALLBACK' in source and 'For buildings without DOB data after BIN and BBL queries' in source:
            print(f"Found Step 3C code cell at index {i}")
            cells_to_remove.append(i)

# Remove cells in reverse order so indices don't shift
for idx in sorted(cells_to_remove, reverse=True):
    print(f"Removing cell at index {idx}")
    del notebook['cells'][idx]

print(f"\nRemoved {len(cells_to_remove)} cells")
print(f"Notebook now has {len(notebook['cells'])} cells")

# Write notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Updated {notebook_path}")


