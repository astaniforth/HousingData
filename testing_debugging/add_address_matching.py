#!/usr/bin/env python3
"""
Add address-based matching (Tier 3) to the matching cell (cell 13, formerly cell 14)
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Address-based matching code to add after BBL matching
address_matching_code = '''

# TIER 3: ADDRESS MATCHING
# For projects still unmatched after BIN and BBL, try matching by address
print(f'\\nProjects still unmatched after BIN and BBL: {len(mfp_projects_without_dob)}')

if len(mfp_projects_without_dob) > 0 and not combined_dob_with_normalized_bbl_df.empty:
    print("Attempting address-based matching for remaining unmatched projects...")
    
    # Get buildings for unmatched projects
    unmatched_buildings_df = hpd_multifamily_finance_new_construction_for_matching_df[
        hpd_multifamily_finance_new_construction_for_matching_df['Project ID'].isin(mfp_projects_without_dob)
    ].copy()
    
    # Normalize addresses in HPD data
    unmatched_buildings_df['address_key'] = (
        unmatched_buildings_df['Borough'].str.upper().str.strip() + '|' +
        unmatched_buildings_df['Number'].astype(str).str.strip() + '|' +
        unmatched_buildings_df['Street'].str.upper().str.strip()
    )
    
    # Normalize addresses in DOB data
    if 'borough' in combined_dob_with_normalized_bbl_df.columns and 'house__' in combined_dob_with_normalized_bbl_df.columns and 'street_name' in combined_dob_with_normalized_bbl_df.columns:
        combined_dob_with_normalized_bbl_df['address_key'] = (
            combined_dob_with_normalized_bbl_df['borough'].astype(str).str.upper().str.strip() + '|' +
            combined_dob_with_normalized_bbl_df['house__'].astype(str).str.strip() + '|' +
            combined_dob_with_normalized_bbl_df['street_name'].astype(str).str.upper().str.strip()
        )
        
        # Find DOB addresses
        dob_addresses = set(combined_dob_with_normalized_bbl_df['address_key'].dropna().unique())
        
        # Match HPD to DOB by address
        matched_by_address_df = unmatched_buildings_df[
            unmatched_buildings_df['address_key'].isin(dob_addresses)
        ]
        
        if not matched_by_address_df.empty:
            matched_project_ids_address = set(matched_by_address_df['Project ID'].unique())
            print(f'✅ Projects matched via address: {len(matched_project_ids_address)}')
            print(f'   Sample: {list(matched_project_ids_address)[:5]}')
            
            # Update the unmatched set
            mfp_projects_without_dob = mfp_projects_without_dob - matched_project_ids_address
            print(f'   Projects still unmatched: {len(mfp_projects_without_dob)}')
        else:
            print('❌ No additional matches found via address')
    else:
        print('⚠️  DOB data missing required address columns for matching')
'''

# Read notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the cell that has the matching logic (now cell 13, formerly 14)
# Look for the cell that has "mfp_projects_without_dob" calculation
for i, cell in enumerate(notebook['cells']):
    if cell.get('cell_type') == 'code':
        source = cell.get('source', [])
        if isinstance(source, list):
            source = ''.join(source)
        
        # Check if this is the matching cell
        if 'mfp_projects_without_dob = mfp_projects_with_dob ^ all_mfp_projects' in source:
            print(f"Found matching cell at index {i}")
            
            # Add address matching code at the end
            new_source = source + address_matching_code
            cell['source'] = new_source.split('\\n')
            
            print(f"✅ Added address-based matching to cell {i}")
            break

# Write notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Updated {notebook_path}")
print("\\nNext: Re-run cell 13 to apply address-based matching")

