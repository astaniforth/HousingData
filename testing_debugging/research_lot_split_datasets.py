#!/usr/bin/env python3
"""
Check if NYC has lot split/merger tracking datasets
Search common NYC Open Data sources
"""

import requests

print("=" * 80)
print("SEARCHING NYC OPEN DATA FOR LOT SPLIT/MERGER DATASETS")
print("=" * 80)

# Known relevant datasets to check
datasets_to_check = [
    {
        "name": "Digital Tax Map: Condominiums",
        "id": "jc5m-4j69",
        "url": "https://data.cityofnewyork.us/resource/jc5m-4j69.json",
        "notes": "Maps condo base BBLs to billing BBLs (lot 7501+)"
    },
    {
        "name": "MapPLUTO",
        "id": "64uk-42ks",  
        "url": "https://data.cityofnewyork.us/resource/64uk-42ks.json",
        "notes": "Tax lot database - might have lot history"
    },
]

# Test each dataset
for dataset in datasets_to_check:
    print(f"\n{'='*80}")
    print(f"📋 {dataset['name']} ({dataset['id']})")
    print(f"   {dataset['notes']}")
    print(f"{'='*80}")
    
    try:
        # Get metadata/columns
        response = requests.get(dataset['url'], params={"$limit": 1}, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"\n✅ Dataset accessible")
                print(f"   Columns: {', '.join(data[0].keys())}")
            else:
                print(f"\n⚠️  Dataset empty or no records returned")
        else:
            print(f"\n❌ Error: HTTP {response.status_code}")
    except Exception as e:
        print(f"\n❌ Exception: {e}")

print("\n" + "=" * 80)
print("ALTERNATIVE: DOB JOB DESCRIPTION FIELD")
print("=" * 80)

# Check if DOB job records have notes/descriptions about lot splits
print("""
The DOB job application records might have notes about lot splits in:
- job_description field
- other_description field
- Special notes fields

However, this requires:
1. Querying DOB for the buildings we DO have data for
2. Parsing the description fields for lot split keywords
3. Extracting the new lot numbers

This is complex and error-prone.

BETTER APPROACH:
Since lot splits typically happen when a property is developed,
we could query DOB for ALL NB filings on the BLOCK (not just specific lot).
Then match by address instead of lot number.

This is essentially what query_dob_by_address() does!
""")

print("\n" + "=" * 80)
print("RECOMMENDATION")
print("=" * 80)
print("""
For buildings without DOB data after BIN/BBL queries:

1. Use query_dob_by_address() as a third-tier fallback
   - Already implemented in the codebase
   - Queries by house number + street name
   - Will find NB filings regardless of lot changes
   
2. This handles:
   - Lot splits (655 Morris Ave might be on new lot 200)
   - Lot mergers (multiple lots combined)
   - BIN corrections
   - BBL mismatches

Currently the notebook does:
- Tier 1: Query by BIN
- Tier 2: Query by BBL (if BIN fails/is placeholder)
- Missing: Tier 3: Query by address

We should add Tier 3!
""")

