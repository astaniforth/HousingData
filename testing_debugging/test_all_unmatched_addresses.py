#!/usr/bin/env python3
"""
Test address fallback on ALL buildings without DOB data from output file
"""

import sys
sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")

import pandas as pd
from query_dob_filings import query_dob_by_address

print("=" * 80)
print("TESTING ADDRESS FALLBACK ON ALL UNMATCHED BUILDINGS")
print("=" * 80)

# Load the output file
output_file = "/Users/andrewstaniforth/Documents/Programming/HousingData/output/hpd_multifamily_finance_new_construction_with_all_dates.csv"
df = pd.read_csv(output_file)

print(f"\nLoaded {len(df)} building records")

# Find buildings without DOB dates
no_dob_mask = df['earliest_dob_date'].isna()
buildings_without_dob = df[no_dob_mask].copy()

print(f"Buildings without DOB dates: {len(buildings_without_dob)}")

# Filter to those with valid identifiers (not null/placeholder BIN)
valid_ids_mask = (
    buildings_without_dob['BIN'].notna() &
    (buildings_without_dob['BIN'].astype(str).str.strip() != '') &
    (~buildings_without_dob['BIN'].astype(str).isin(['1000000', '2000000', '3000000', '4000000', '5000000']))
)

buildings_with_valid_ids = buildings_without_dob[valid_ids_mask].copy()

print(f"Buildings with valid IDs but no DOB data: {len(buildings_with_valid_ids)}")

# Extract ALL unique addresses
addresses = []
address_to_buildings = {}

for idx, row in buildings_with_valid_ids.iterrows():
    building_id = row.get('Building ID')
    project_id = row.get('Project ID')
    project_name = row.get('Project Name', '')
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
    
    # Track unique addresses
    if address_tuple not in address_to_buildings:
        addresses.append(address_tuple)
        address_to_buildings[address_tuple] = []
    
    # Map address to building info
    address_to_buildings[address_tuple].append({
        'Building ID': building_id,
        'Project ID': project_id,
        'Project Name': project_name,
        'BIN': row.get('BIN'),
        'BBL': row.get('BBL')
    })

print(f"\nUnique valid addresses to test: {len(addresses)}")

if len(addresses) == 0:
    print("\n⚠️  No valid addresses found to test")
    print("\nChecking why addresses are invalid...")
    
    # Show first few buildings and their address data
    for idx, row in buildings_with_valid_ids.head(10).iterrows():
        print(f"\nBuilding {row.get('Building ID')} - {row.get('Project Name', '')}")
        print(f"  Borough: '{row.get('Borough', '')}'")
        print(f"  Number: '{row.get('Number', '')}'")
        print(f"  Street: '{row.get('Street', '')}'")
    exit(0)

print(f"\n{'='*80}")
print(f"QUERYING DOB APIS BY ADDRESS")
print(f"{'='*80}\n")

# Query DOB by address
dob_results = query_dob_by_address(addresses)

print(f"\n{'='*80}")
print(f"RESULTS")
print(f"{'='*80}")

if not dob_results.empty:
    print(f"\n✅ Found {len(dob_results)} DOB records by address!")
    
    # Show what we found
    display_cols = ['job__', 'job_type', 'bin__', 'borough', 'house__', 'street_name', 'pre__filing_date', 'job_filing_number']
    existing_cols = [col for col in display_cols if col in dob_results.columns]
    
    print("\n📋 DOB Records Found:")
    print(dob_results[existing_cols].to_string(index=False))
    
    # Match back to buildings
    print(f"\n{'='*80}")
    print("MATCHING RESULTS TO BUILDINGS")
    print(f"{'='*80}")
    
    matches_found = 0
    for idx, dob_row in dob_results.iterrows():
        # Get address from DOB record
        dob_borough = str(dob_row.get('borough', '')).upper().strip()
        dob_house = str(dob_row.get('house__', '')).strip()
        dob_street = str(dob_row.get('street_name', '')).strip().upper()
        
        address_key = (dob_borough, dob_house, dob_street)
        
        if address_key in address_to_buildings:
            matches_found += 1
            buildings = address_to_buildings[address_key]
            print(f"\n✅ Match found for address: {dob_house} {dob_street}, {dob_borough}")
            print(f"   DOB Job: {dob_row.get('job__', 'N/A')}")
            print(f"   DOB BIN: {dob_row.get('bin__', 'N/A')}")
            print(f"   Filing Date: {dob_row.get('pre__filing_date', 'N/A')}")
            print(f"   Buildings at this address ({len(buildings)}):")
            for bldg in buildings:
                print(f"     - Building {bldg['Building ID']}: {bldg['Project Name']}")
                print(f"       HPD BIN: {bldg['BIN']}, HPD BBL: {bldg['BBL']}")
    
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Buildings without DOB data: {len(buildings_with_valid_ids)}")
    print(f"Unique addresses queried: {len(addresses)}")
    print(f"DOB records found: {len(dob_results)}")
    print(f"Addresses with matches: {matches_found}")
    print(f"\n✅ Address fallback successfully found {matches_found} matches!")
else:
    print(f"\n❌ No DOB records found by address for any of the {len(addresses)} addresses")
    print(f"\nThis means:")
    print(f"- Either the addresses in HPD don't match DOB exactly")
    print(f"- Or these buildings legitimately don't have NB filings in DOB")
    
    # Show sample of addresses queried
    print(f"\n📋 Sample addresses queried (first 10):")
    for i, (borough, house, street) in enumerate(addresses[:10]):
        print(f"{i+1}. {house} {street}, {borough}")


