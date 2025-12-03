#!/usr/bin/env python3
"""
Fix the address fallback cells to use correct variable names
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Read notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

fixed_count = 0

for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        # Get source as string
        source_lines = cell.get('source', [])
        if isinstance(source_lines, list):
            source = ''.join(source_lines)
        else:
            source = source_lines
        
        # Check if this is an address fallback cell
        if 'ADDRESS-BASED FALLBACK' in source and 'projects_with_no_dob' in source:
            print(f"Found address fallback cell to fix")
            
            # Fix variable names
            new_source = source.replace('projects_with_no_dob', 'mfp_projects_without_dob')
            new_source = new_source.replace(
                'hpd_multifamily_finance_new_construction_with_normalized_ids_df',
                'hpd_multifamily_finance_new_construction_for_matching_df'
            )
            
            # Update cell source
            cell['source'] = new_source.split('\n')
            fixed_count += 1

print(f"Fixed {fixed_count} cells")

# Write notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Updated {notebook_path}")

