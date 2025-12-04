#!/usr/bin/env python3
"""
Fix the address fallback cell to use correct column names
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Read notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

fixed_count = 0

for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        source_lines = cell.get('source', [])
        if isinstance(source_lines, list):
            source = ''.join(source_lines)
        else:
            source = source_lines
        
        # Check if this is an address fallback cell with the merge issue
        if 'ADDRESS-BASED FALLBACK' in source and "['Project ID', 'Building ID', 'bin_normalized', 'bbl_normalized']" in source:
            print(f"Found address fallback cell to fix")
            
            # Replace the problematic merge section
            old_code = """    # Extract addresses - need to get from original HPD dataframe
    # The normalized IDs dataframe may not have address columns
    # Merge with original HPD data to get addresses
    hpd_with_addresses = pd.merge(
        buildings_needing_address_fallback_df[['Project ID', 'Building ID', 'bin_normalized', 'bbl_normalized']],
        hpd_multifamily_finance_new_construction_df[['Project ID', 'Building ID', 'Borough', 'Number', 'Street']],
        on=['Project ID', 'Building ID'],
        how='left'
    )"""
            
            new_code = """    # Extract addresses from the buildings dataframe
    # hpd_multifamily_finance_new_construction_for_matching_df already has all columns we need"""
            
            new_source = source.replace(old_code, new_code)
            
            # Also update the loop to use buildings_needing_address_fallback_df directly
            new_source = new_source.replace(
                'for idx, row in hpd_with_addresses.iterrows():',
                'for idx, row in buildings_needing_address_fallback_df.iterrows():'
            )
            
            cell['source'] = new_source.split('\n')
            fixed_count += 1

print(f"Fixed {fixed_count} cells")

# Write notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Updated {notebook_path}")


