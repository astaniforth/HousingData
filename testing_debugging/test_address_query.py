#!/usr/bin/env python3
"""
Test address-based query for 655 Morris Avenue
"""

import sys
sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")

from query_dob_filings import query_dob_by_address
import pandas as pd

# Building 50497: 655 Morris Avenue, Bronx
addresses = [
    ("BRONX", "655", "MORRIS AVENUE"),
    ("BRONX", "635", "MORRIS AVENUE"),  # Also try the address from HPD (635 vs 655)
]

print("=" * 80)
print("TESTING ADDRESS-BASED QUERY FOR 655 MORRIS AVENUE")
print("=" * 80)

for borough, house, street in addresses:
    print(f"\n🔍 Querying: {house} {street}, {borough}")
    
    result = query_dob_by_address([(borough, house, street)])
    
    if not result.empty:
        print(f"✅ Found {len(result)} records")
        
        # Show what we got
        display_cols = ['job__', 'job_type', 'bin__', 'house__', 'street_name', 'pre__filing_date']
        if 'doc__' in result.columns:
            display_cols.append('doc__')
        existing_cols = [col for col in display_cols if col in result.columns]
        
        print(result[existing_cols].to_string(index=False))
    else:
        print(f"❌ No records found")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("""
If address query also finds no NB filings, then this building
legitimately does NOT have a New Building permit in DOB.

Possible reasons:
1. HPD classified it as "New Construction" for financing purposes
   but it was filed as an Alteration in DOB (gut renovation, adaptive reuse)
2. Project hasn't been built yet
3. BIN/address data in HPD is incorrect

For these cases, we have to accept that they won't have NB DOB data.
""")


