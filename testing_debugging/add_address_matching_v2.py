#!/usr/bin/env python3
"""
Add address-based matching to cell 13 after BBL matching
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Read notebook
with open(notebook_path, 'r') as f:
    content = f.read()

# The code to insert (will be added before the DEBUG section)
address_matching_code = '''

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

'''

# Find and replace: insert before the DEBUG section
search_string = '''    "    print(f'Sample unmatched: {list(mfp_projects_without_dob)[:3]}')\n",
    "\\n",
    "# DEBUG: Analyze a sample project to understand matching\n",'''

replace_string = '''    "    print(f'Sample unmatched: {list(mfp_projects_without_dob)[:3]}')\n",
    "\\n",''' + address_matching_code.replace('\n', '\\n",\n    "').rstrip('",\n    "') + '''\n",
    "\\n",
    "# DEBUG: Analyze a sample project to understand matching\n",'''

if search_string in content:
    print("Found insertion point")
    new_content = content.replace(search_string, replace_string)
    
    with open(notebook_path, 'w') as f:
        f.write(new_content)
    
    print("✅ Added address-based matching to cell 13")
else:
    print("❌ Could not find insertion point")
    print("Searching for alternative...")

print("Done")


