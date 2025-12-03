#!/usr/bin/env python3
"""
Add address-based matching to the matching cell using JSON manipulation
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Address matching code
address_matching_code = """

# TIER 3: ADDRESS MATCHING
# For projects still unmatched after BIN and BBL, try matching by address
print(f'\\nAttempting address-based matching for {len(mfp_projects_without_dob)} remaining unmatched projects...')

if len(mfp_projects_without_dob) > 0 and not combined_dob_with_normalized_bbl_df.empty:
    # Get buildings for unmatched projects
    unmatched_buildings_df = hpd_multifamily_finance_new_construction_for_matching_df[
        hpd_multifamily_finance_new_construction_for_matching_df['Project ID'].isin(mfp_projects_without_dob)
    ].copy()
    
    # Normalize addresses in HPD data
    unmatched_buildings_df['address_key'] = (
        unmatched_buildings_df['Borough'].astype(str).str.upper().str.strip() + '|' +
        unmatched_buildings_df['Number'].astype(str).str.strip() + '|' +
        unmatched_buildings_df['Street'].astype(str).str.upper().str.strip()
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
            print(f'   Projects remaining unmatched: {len(mfp_projects_without_dob)}')
        else:
            print('❌ No additional matches found via address')
    else:
        print('⚠️  DOB data missing required address columns for address matching')
"""

# Read notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find cell with the matching logic
found = False
for i, cell in enumerate(notebook['cells']):
    if cell.get('cell_type') == 'code':
        source = cell.get('source', [])
        if isinstance(source, list):
            source_str = ''.join(source)
        else:
            source_str = source
        
        # Look for the unique text that identifies the matching cell
        if "print(f'Sample unmatched: {list(mfp_projects_without_dob)[:3]}')" in source_str and "# DEBUG: Analyze a sample project" in source_str:
            print(f"Found matching cell at index {i}")
            
            # Find insertion point (before DEBUG section)
            lines = source if isinstance(source, list) else source.split('\n')
            
            # Find the line with "# DEBUG: Analyze a sample project"
            insertion_idx = None
            for idx, line in enumerate(lines):
                if "# DEBUG: Analyze a sample project" in line:
                    insertion_idx = idx
                    break
            
            if insertion_idx is not None:
                # Insert address matching code before DEBUG
                new_lines = lines[:insertion_idx] + address_matching_code.split('\n') + lines[insertion_idx:]
                cell['source'] = new_lines
                found = True
                print(f"✅ Inserted address matching at line {insertion_idx}")
                break

if found:
    # Write notebook
    with open(notebook_path, 'w') as f:
        json.dump(notebook, f, indent=1)
    print(f"✅ Updated {notebook_path}")
else:
    print("❌ Could not find matching cell")

