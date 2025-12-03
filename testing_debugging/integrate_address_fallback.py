#!/usr/bin/env python3
"""
Add address fallback to the end of cell 13 (before matching in cell 14)
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Address fallback code to append to cell 13
address_fallback_code = '''

# TIER 3: ADDRESS FALLBACK
# Query by address for projects still unmatched after BIN and BBL queries
print("\\n" + "=" * 70)
print("TIER 3: ADDRESS FALLBACK")
print("=" * 70)

# First, combine all DOB data collected so far to see what we have
all_dob_dfs = []
if not dob_bisweb_bin_df.empty:
    all_dob_dfs.append(dob_bisweb_bin_df)
if not dob_now_bin_df.empty:
    all_dob_dfs.append(dob_now_bin_df)
if not dob_bisweb_bbl_df.empty:
    all_dob_dfs.append(dob_bisweb_bbl_df)
if not dob_now_bbl_df.empty:
    all_dob_dfs.append(dob_now_bbl_df)

if all_dob_dfs:
    temp_combined_dob = pd.concat(all_dob_dfs, ignore_index=True)
    
    # Normalize BINs and BBLs in temp combined data
    if 'bin__' in temp_combined_dob.columns:
        temp_combined_dob['bin_normalized'] = temp_combined_dob['bin__'].astype(str).str.replace('.0', '', regex=False)
    elif 'bin' in temp_combined_dob.columns:
        temp_combined_dob['bin_normalized'] = temp_combined_dob['bin'].astype(str).str.replace('.0', '', regex=False)
    
    # Find which BINs/BBLs we've already matched
    matched_bins = set(temp_combined_dob['bin_normalized'].dropna().unique())
    
    # Find projects that still need address fallback
    # These are projects whose BINs are NOT in matched_bins
    hpd_temp = hpd_multifamily_finance_new_construction_df.copy()
    hpd_temp['BIN_normalized'] = hpd_temp['BIN'].astype(str).str.replace('.0', '', regex=False)
    
    projects_still_unmatched = hpd_temp[~hpd_temp['BIN_normalized'].isin(matched_bins)]['Project ID'].unique()
    
    print(f"Projects still unmatched after BIN/BBL queries: {len(projects_still_unmatched)}")
    
    if len(projects_still_unmatched) > 0:
        # Get buildings for these projects
        buildings_for_address_fallback = hpd_multifamily_finance_new_construction_df[
            hpd_multifamily_finance_new_construction_df['Project ID'].isin(projects_still_unmatched)
        ].copy()
        
        print(f"Building records needing address fallback: {len(buildings_for_address_fallback)}")
        
        # Extract addresses
        addresses = []
        for idx, row in buildings_for_address_fallback.iterrows():
            borough = str(row.get('Borough', '')).upper().strip()
            house_no = str(row.get('Number', '')).strip()
            street = str(row.get('Street', '')).strip().upper()
            
            # Skip invalid addresses
            if (not borough or borough == 'NAN' or 
                not house_no or house_no == 'NAN' or 
                not street or street == 'NAN' or
                'T00:00:00' in house_no):
                continue
            
            address_tuple = (borough, house_no, street)
            if address_tuple not in [a for a in addresses]:
                addresses.append(address_tuple)
        
        print(f"Unique addresses to query: {len(addresses)}")
        
        if len(addresses) > 0:
            # Query DOB by address
            dob_address_df = query_dob_by_address(addresses)
            
            if not dob_address_df.empty:
                print(f"✅ Found {len(dob_address_df)} DOB records by address")
                
                # Add source tag
                dob_address_df['source'] = 'ADDRESS_FALLBACK'
                
                # Normalize BIN/BBL
                if 'bin__' in dob_address_df.columns:
                    dob_address_df['bin_normalized'] = dob_address_df['bin__'].astype(str).str.replace('.0', '', regex=False)
                elif 'bin' in dob_address_df.columns:
                    dob_address_df['bin_normalized'] = dob_address_df['bin'].astype(str).str.replace('.0', '', regex=False)
                
                # Add to our BBL results (will be combined with BIN results in next cell)
                if dob_now_bbl_df.empty:
                    dob_now_bbl_df = dob_address_df.copy()
                else:
                    # Ensure columns match
                    all_cols = list(set(list(dob_now_bbl_df.columns) + list(dob_address_df.columns)))
                    dob_now_bbl_aligned = dob_now_bbl_df.reindex(columns=all_cols)
                    address_aligned = dob_address_df.reindex(columns=all_cols)
                    dob_now_bbl_df = pd.concat([dob_now_bbl_aligned, address_aligned], ignore_index=True)
                
                print(f"✅ Address fallback results will be included in matching")
            else:
                print(f"❌ No DOB records found by address")
        else:
            print(f"⚠️  No valid addresses to query")
    else:
        print("✅ All projects already matched via BIN/BBL")
else:
    print("⚠️  No DOB data collected yet, skipping address fallback")
'''

# Read notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find cell 13 and append address fallback code
for i, cell in enumerate(notebook['cells']):
    if cell.get('cell_type') == 'code' and cell.get('execution_count') == 13:
        print(f"Found cell 13 at index {i}")
        
        # Get current source
        current_source = cell.get('source', [])
        if isinstance(current_source, list):
            current_source = ''.join(current_source)
        
        # Append address fallback code
        new_source = current_source + address_fallback_code
        
        # Update cell
        cell['source'] = new_source.split('\n')
        print(f"✅ Added address fallback to cell 13")
        break

# Write notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Updated {notebook_path}")
print("\nNext steps:")
print("1. Delete the standalone Step 3C cells (cells 15 and 21)")
print("2. Re-run cell 13 onwards to get address fallback before matching")

