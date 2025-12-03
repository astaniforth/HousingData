#!/usr/bin/env python3
"""
Rewrite the address fallback cell with correct logic
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Read notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# The correct code for address fallback
new_cell_code = '''# TIER 3: ADDRESS-BASED FALLBACK
# For buildings without DOB data after BIN and BBL queries, try address-based lookup

print("=" * 70)
print("STEP 3C: ADDRESS-BASED FALLBACK (TIER 3)")
print("=" * 70)

# Identify buildings that still don't have DOB matches
if 'mfp_projects_without_dob' in globals() and len(mfp_projects_without_dob) > 0:
    print(f"\\nProjects without DOB data after BIN/BBL queries: {len(mfp_projects_without_dob)}")
    
    # Get the buildings that need address fallback
    buildings_needing_address_fallback_df = hpd_multifamily_finance_new_construction_for_matching_df[
        hpd_multifamily_finance_new_construction_for_matching_df['Project ID'].isin(mfp_projects_without_dob)
    ].copy()
    
    print(f"Total building records needing address fallback: {len(buildings_needing_address_fallback_df)}")
    
    # Extract addresses for query
    addresses = []
    address_to_project_map = {}  # Map (borough, house, street) -> list of project IDs
    
    for idx, row in buildings_needing_address_fallback_df.iterrows():
        borough = str(row.get('Borough', '')).upper().strip()
        house_no = str(row.get('Number', '')).strip()
        street = str(row.get('Street', '')).strip().upper()
        
        # Skip if any required field is missing or invalid
        if (not borough or borough == 'NAN' or 
            not house_no or house_no == 'NAN' or 
            not street or street == 'NAN' or
            'T00:00:00' in house_no):  # Skip date-like house numbers
            continue
        
        address_tuple = (borough, house_no, street)
        
        # Add to unique addresses list
        if address_tuple not in address_to_project_map:
            addresses.append(address_tuple)
            address_to_project_map[address_tuple] = set()
        
        # Track which project IDs correspond to this address
        project_id = row.get('Project ID')
        if pd.notna(project_id):
            address_to_project_map[address_tuple].add(project_id)
    
    print(f"Unique addresses to query: {len(addresses)}")
    
    if len(addresses) > 0:
        # Query DOB by address
        print(f"\\nQuerying DOB APIs by address (Tier 3 fallback)...")
        dob_address_results_df = query_dob_by_address(addresses)
        
        if not dob_address_results_df.empty:
            print(f"✅ Found {len(dob_address_results_df)} DOB records by address")
            
            # Add source tag
            dob_address_results_df['source'] = 'ADDRESS_FALLBACK'
            
            # Normalize BIN column
            if 'bin__' in dob_address_results_df.columns:
                dob_address_results_df['bin_normalized'] = dob_address_results_df['bin__'].astype(str).str.replace('.0', '', regex=False)
            elif 'bin' in dob_address_results_df.columns:
                dob_address_results_df['bin_normalized'] = dob_address_results_df['bin'].astype(str).str.replace('.0', '', regex=False)
            
            # Reconstruct BBL if not present (reuse function from earlier)
            if 'bbl' not in dob_address_results_df.columns:
                dob_address_results_df['bbl_reconstructed'] = dob_address_results_df.apply(reconstruct_bbl, axis=1)
                dob_address_results_df['bbl_normalized'] = dob_address_results_df['bbl_reconstructed']
            else:
                dob_address_results_df['bbl_normalized'] = dob_address_results_df['bbl'].apply(
                    lambda x: str(int(float(x))).zfill(10) if pd.notna(x) else None
                )
            
            # Append to combined DOB data
            all_cols = list(set(list(combined_dob_with_normalized_bbl_df.columns) + list(dob_address_results_df.columns)))
            combined_dob_aligned = combined_dob_with_normalized_bbl_df.reindex(columns=all_cols)
            address_aligned = dob_address_results_df.reindex(columns=all_cols)
            combined_dob_with_normalized_bbl_df = pd.concat([combined_dob_aligned, address_aligned], ignore_index=True)
            
            print(f"✅ Appended address fallback results")
            print(f"   Total DOB records now: {len(combined_dob_with_normalized_bbl_df)}")
            
            # Count how many projects now have matches by checking addresses
            matched_projects = set()
            for idx, dob_row in dob_address_results_df.iterrows():
                dob_borough = str(dob_row.get('borough', '')).upper().strip()
                dob_house = str(dob_row.get('house__', '')).strip()
                dob_street = str(dob_row.get('street_name', '')).strip().upper()
                
                address_key = (dob_borough, dob_house, dob_street)
                if address_key in address_to_project_map:
                    matched_projects.update(address_to_project_map[address_key])
            
            print(f"   Projects matched via address: {len(matched_projects)}")
            if matched_projects:
                print(f"   Sample: {list(matched_projects)[:5]}")
        else:
            print(f"❌ No DOB records found by address")
    else:
        print(f"⚠️  No valid addresses to query")
else:
    print(f"\\n✅ All projects already have DOB data")

print(f"\\n✅ Step 3C complete")'''

fixed_count = 0

for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code':
        source_lines = cell.get('source', [])
        if isinstance(source_lines, list):
            source = ''.join(source_lines)
        else:
            source = source_lines
        
        # Check if this is an address fallback cell
        if 'TIER 3: ADDRESS-BASED FALLBACK' in source or ('ADDRESS-BASED FALLBACK' in source and 'STEP 3C' in source):
            print(f"Found address fallback cell at index {i}")
            cell['source'] = new_cell_code.split('\n')
            fixed_count += 1

print(f"Fixed {fixed_count} cells")

# Write notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Updated {notebook_path}")

