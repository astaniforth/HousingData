#!/usr/bin/env python3
"""
Add address-based date extraction at the correct position in the notebook
(after hpd_multifamily_finance_new_construction_with_dob_date_df is created)
"""

import json

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Address-based date extraction code
address_date_extraction = '''
# TIER 3: ADDRESS-BASED DATE EXTRACTION
# For buildings still without DOB dates, try extracting by address
print("\\n" + "=" * 70)
print("TIER 3: ADDRESS-BASED DATE EXTRACTION")
print("=" * 70)

# Find buildings without DOB dates
no_date_mask = hpd_multifamily_finance_new_construction_with_dob_date_df['earliest_dob_date'].isna()
buildings_without_dates = hpd_multifamily_finance_new_construction_with_dob_date_df[no_date_mask].copy()
print(f"Buildings without DOB dates after BIN/BBL extraction: {len(buildings_without_dates)}")

if len(buildings_without_dates) > 0 and 'address_key' in combined_dob_with_normalized_bbl_df.columns:
    # Create address keys in buildings without dates
    buildings_without_dates['address_key'] = (
        buildings_without_dates['Borough'].astype(str).str.upper().str.strip() + '|' +
        buildings_without_dates['Number'].astype(str).str.strip() + '|' +
        buildings_without_dates['Street'].astype(str).str.upper().str.strip()
    )
    
    # Get DOB data with address keys and dates
    dob_with_address = dob_df[dob_df['address_key'].notna()].copy() if 'address_key' in dob_df.columns else pd.DataFrame()
    
    if not dob_with_address.empty and 'earliest_dob_date' in dob_with_address.columns:
        # Group by address and get earliest date
        dob_address_min = dob_with_address.groupby('address_key', as_index=False).agg({
            'earliest_dob_date': 'min',
            'earliest_dob_date_source': 'first',
            'application_number': 'first'
        })
        
        if 'fully_permitted' in dob_with_address.columns:
            fp_min = dob_with_address.groupby('address_key', as_index=False)['fully_permitted'].first()
            dob_address_min = dob_address_min.merge(fp_min, on='address_key', how='left')
            dob_address_min.rename(columns={'fully_permitted': 'fully_permitted_date'}, inplace=True)
        
        print(f"DOB records with addresses and dates: {len(dob_address_min)}")
        
        # Merge with buildings that need dates
        address_merge = pd.merge(
            buildings_without_dates[['Building ID', 'address_key']],
            dob_address_min,
            on='address_key',
            how='inner'
        )
        
        print(f"Buildings matched via address: {len(address_merge)}")
        
        if not address_merge.empty:
            # Update the dataframe with address-matched dates
            for _, row in address_merge.iterrows():
                bldg_id = row['Building ID']
                mask = hpd_multifamily_finance_new_construction_with_dob_date_df['Building ID'] == bldg_id
                if mask.any():
                    hpd_multifamily_finance_new_construction_with_dob_date_df.loc[mask, 'earliest_dob_date'] = row['earliest_dob_date']
                    hpd_multifamily_finance_new_construction_with_dob_date_df.loc[mask, 'earliest_dob_date_source'] = row['earliest_dob_date_source']
                    hpd_multifamily_finance_new_construction_with_dob_date_df.loc[mask, 'application_number'] = row['application_number']
                    if 'fully_permitted_date' in row and pd.notna(row.get('fully_permitted_date')):
                        hpd_multifamily_finance_new_construction_with_dob_date_df.loc[mask, 'fully_permitted_date'] = row['fully_permitted_date']
            
            print(f"✅ Updated {len(address_merge)} buildings with address-matched DOB dates")
        else:
            print("❌ No buildings matched via address")
    else:
        print("⚠️  No DOB records have both address_key and earliest_dob_date")
else:
    if len(buildings_without_dates) == 0:
        print("✅ All buildings have DOB dates!")
    else:
        print("⚠️  DOB data missing address_key column - run cell 13 first")

# Final count
final_no_date = hpd_multifamily_finance_new_construction_with_dob_date_df['earliest_dob_date'].isna().sum()
print(f"\\nFinal count - Buildings without DOB dates: {final_no_date}")

'''

# Read notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the cell with the assignment and insert address extraction after it
found = False
for i, cell in enumerate(notebook['cells']):
    if cell.get('cell_type') == 'code':
        source = cell.get('source', [])
        if isinstance(source, list):
            source_str = ''.join(source)
        else:
            source_str = source
        
        # Look for the cell that creates hpd_multifamily_finance_new_construction_with_dob_date_df
        if "hpd_multifamily_finance_new_construction_with_dob_date_df = hpd_with_date" in source_str:
            print(f"Found target cell at index {i}")
            
            lines = source if isinstance(source, list) else source.split('\n')
            
            # Find the line with the assignment
            insertion_idx = None
            for idx, line in enumerate(lines):
                if "hpd_multifamily_finance_new_construction_with_dob_date_df = hpd_with_date" in line:
                    insertion_idx = idx + 1  # Insert after this line
                    break
            
            if insertion_idx is not None:
                # Insert address date extraction code
                new_lines = lines[:insertion_idx] + address_date_extraction.split('\n') + lines[insertion_idx:]
                cell['source'] = new_lines
                found = True
                print(f"✅ Inserted address date extraction after line {insertion_idx}")
                break

if found:
    with open(notebook_path, 'w') as f:
        json.dump(notebook, f, indent=1)
    print(f"✅ Updated {notebook_path}")
else:
    print("❌ Could not find target cell")


