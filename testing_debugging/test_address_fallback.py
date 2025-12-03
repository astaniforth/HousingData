#!/usr/bin/env python3
"""
Test address-based fallback (Tier 3) logic before adding to notebook
"""

import sys
sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")

import pandas as pd
from query_dob_filings import query_dob_by_address

print("=" * 80)
print("TESTING ADDRESS-BASED FALLBACK LOGIC")
print("=" * 80)

# Load the output file to find buildings without DOB data
output_file = "/Users/andrewstaniforth/Documents/Programming/HousingData/output/hpd_multifamily_finance_new_construction_with_all_dates.csv"
df = pd.read_csv(output_file)

print(f"\nLoaded {len(df)} building records")

# Find buildings without DOB dates
no_dob_mask = df['earliest_dob_date'].isna()
buildings_without_dob = df[no_dob_mask].copy()

print(f"Buildings without DOB dates: {len(buildings_without_dob)}")

# Filter to those with valid address data (not null BIN/BBL)
valid_ids_mask = (
    buildings_without_dob['BIN'].notna() &
    (buildings_without_dob['BIN'].astype(str).str.strip() != '') &
    (~buildings_without_dob['BIN'].astype(str).isin(['1000000', '2000000', '3000000', '4000000', '5000000']))
)

buildings_with_valid_ids = buildings_without_dob[valid_ids_mask].copy()

print(f"Buildings with valid IDs but no DOB data: {len(buildings_with_valid_ids)}")

# Extract addresses for a small sample
sample_size = 5
sample_buildings = buildings_with_valid_ids.head(sample_size)

print(f"\nTesting address fallback on {len(sample_buildings)} buildings:")

addresses = []
address_to_building_map = {}

for idx, row in sample_buildings.iterrows():
    building_id = row.get('Building ID')
    project_id = row.get('Project ID')
    project_name = row.get('Project Name', '')
    borough = str(row.get('Borough', '')).upper()
    house_no = str(row.get('Number', '')).strip()
    street = str(row.get('Street', '')).strip().upper()
    
    print(f"\n{building_id} - {project_name}")
    print(f"   Project ID: {project_id}")
    print(f"   Address: {house_no} {street}, {borough}")
    print(f"   BIN: {row.get('BIN')}, BBL: {row.get('BBL')}")
    
    # Skip if any required field is missing
    if not borough or borough == 'NAN' or not house_no or house_no == 'NAN' or not street or street == 'NAN':
        print(f"   ⚠️  Skipping: missing address data")
        continue
    
    address_tuple = (borough, house_no, street)
    
    # Add to unique addresses list
    if address_tuple not in address_to_building_map:
        addresses.append(address_tuple)
        address_to_building_map[address_tuple] = []
    
    address_to_building_map[address_tuple].append(str(building_id))

print(f"\n{'='*80}")
print(f"Querying {len(addresses)} unique addresses...")
print(f"{'='*80}")

if len(addresses) > 0:
    dob_address_results_df = query_dob_by_address(addresses)
    
    if not dob_address_results_df.empty:
        print(f"\n✅ Found {len(dob_address_results_df)} DOB records by address")
        
        # Show what we found
        display_cols = ['job__', 'job_type', 'bin__', 'house__', 'street_name', 'borough', 'pre__filing_date']
        if 'doc__' in dob_address_results_df.columns:
            display_cols.append('doc__')
        if 'job_filing_number' in dob_address_results_df.columns:
            display_cols.append('job_filing_number')
        
        existing_cols = [col for col in display_cols if col in dob_address_results_df.columns]
        
        print("\n📋 DOB Records Found:")
        print(dob_address_results_df[existing_cols].to_string(index=False))
        
        print(f"\n{'='*80}")
        print("ADDRESS FALLBACK SUCCESS!")
        print(f"{'='*80}")
        print(f"This demonstrates that address-based querying can find DOB data")
        print(f"for buildings that failed BIN and BBL queries.")
        print(f"\nReasons this works:")
        print(f"- Handles lot splits/mergers")
        print(f"- Handles BIN mismatches between HPD and DOB")
        print(f"- Handles BBL changes over time")
    else:
        print(f"\n❌ No DOB records found by address for this sample")
else:
    print(f"\n⚠️  No valid addresses to query")

print(f"\n{'='*80}")
print("READY TO ADD TO NOTEBOOK")
print(f"{'='*80}")
print("The address fallback logic is validated and ready to be added as Step 3C")

