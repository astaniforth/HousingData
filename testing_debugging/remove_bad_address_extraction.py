#!/usr/bin/env python3
"""
Fix the address date extraction - insert it at the correct position
(after BIN/BBL extraction creates earliest_dob_date column)
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Read notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# First, remove the incorrectly placed address date extraction code
for i, cell in enumerate(notebook['cells']):
    if cell.get('cell_type') == 'code':
        source = cell.get('source', [])
        if isinstance(source, list):
            source_str = ''.join(source)
        else:
            source_str = source
        
        # Check if this cell has the address date extraction code
        if "TIER 3: ADDRESS-BASED DATE EXTRACTION" in source_str:
            print(f"Found cell with address date extraction at index {i}")
            
            # Remove the address date extraction section
            lines = source if isinstance(source, list) else source.split('\n')
            
            # Find start and end of the section to remove
            start_idx = None
            end_idx = None
            for idx, line in enumerate(lines):
                if "# TIER 3: ADDRESS-BASED DATE EXTRACTION" in line:
                    start_idx = idx
                if start_idx is not None and "Final count - Buildings without DOB dates:" in line:
                    end_idx = idx + 1
                    break
            
            if start_idx is not None and end_idx is not None:
                # Remove those lines
                new_lines = lines[:start_idx] + lines[end_idx:]
                cell['source'] = new_lines
                print(f"Removed address date extraction from lines {start_idx} to {end_idx}")
            break

# Write notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Removed incorrectly placed address date extraction")
print("The address date extraction needs to be added to a LATER cell")
print("(after BIN/BBL merge creates the earliest_dob_date column)")


