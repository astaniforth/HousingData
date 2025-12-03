#!/usr/bin/env python3
"""
Search for lot split/condo information in NYC Open Data
Check what datasets track BBL changes, lot subdivisions, and condo lots
"""

import requests
import pandas as pd

# First, let's check the building 50497 more carefully
# BIN: 2002441, BBL: 2024410001
# Borough: 2 (Bronx), Block: 2441, Lot: 1

print("=" * 80)
print("INVESTIGATING LOT SPLIT FOR BUILDING 50497")
print("=" * 80)
print("\nBuilding: 655 Morris Avenue")
print("HPD BBL: 2024410001 (Borough 2, Block 2441, Lot 1)")
print("Note indicates: NEW LOT 200 = 655 MORRIS AVE")
print()

# Check if DOB has records for lot 200
BISWEB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
DOBNOW_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"

borough = "2"
block = "2441"
lots_to_check = ["200", "1"]  # Check both the new lot and original lot

print("🔍 Checking DOB BISWEB for different lots on Block 2441...")
for lot in lots_to_check:
    print(f"\n--- Lot {lot} ---")
    try:
        response = requests.get(
            BISWEB_URL,
            params={
                "$where": f"borough='{borough}' AND block='{block}' AND lot='{lot}' AND job_type='NB'",
                "$limit": 10
            },
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                print(f"✅ Found {len(df)} NB records for Lot {lot}")
                
                # Show key info
                display_cols = ['job__', 'bin__', 'house__', 'street_name', 'pre__filing_date', 'job_status']
                existing_cols = [col for col in display_cols if col in df.columns]
                print(df[existing_cols].to_string(index=False))
            else:
                print(f"❌ No NB records for Lot {lot}")
    except Exception as e:
        print(f"Error: {e}")

print("\n" + "=" * 80)
print("NYC OPEN DATA: LOT SPLIT/CONDO DATASETS")
print("=" * 80)

# Search for condo/lot split datasets
print("\n📋 Known NYC datasets for lot changes:")
print("""
1. DOB Job Application Filings (ic3t-wcy2)
   - Has 'house__' field which might contain notes about lot changes
   - Has borough/block/lot for each filing
   
2. PLUTO (MapPLUTO)
   - Tax lot database with historical lot info
   - Tracks when lots were created/split
   - Has 'LotType' field (regular, condo, etc.)
   
3. Department of Finance - Property Address Data
   - Tracks BBL to address mappings
   
4. Condo Lots Query Function
   - Our workflow has: query_condo_lots_for_bbl()
   - This might help find related lots!
""")

print("\n🔍 Let's check our own query_condo_lots_for_bbl function...")

# Check if we have a condo lots function
try:
    import sys
    sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")
    from query_dob_filings import query_condo_lots_for_bbl
    
    print("✅ Found query_condo_lots_for_bbl function in our codebase!")
    print("\nLet's see what it does...")
    
    # Check the function signature
    import inspect
    sig = inspect.signature(query_condo_lots_for_bbl)
    print(f"Function signature: {sig}")
    
    # Get the docstring
    doc = inspect.getdoc(query_condo_lots_for_bbl)
    if doc:
        print(f"\nDocstring:\n{doc}")
    
except Exception as e:
    print(f"❌ Error accessing function: {e}")

print("\n" + "=" * 80)
print("RECOMMENDATION")
print("=" * 80)
print("""
For lot splits/mergers, we should:

1. Use query_condo_lots_for_bbl() to find related lots
   - This queries DOB's condo lot mapping
   - Finds all lots associated with a BBL
   
2. When BBL fallback fails, also try:
   - Query all lots on the same block (might be too broad)
   - Look for lot numbers like 200, 7501-7599 (condo ranges)
   - Check if original lot was split into numbered lots
   
3. Enhance BBL fallback to:
   - First try exact BBL match
   - Then try condo lot query
   - Then try adjacent/related lots if identified

The note "NEW LOT 200" suggests lot 1 was split. We should query:
- Lot 1 (original)
- Lot 200 (new split lot for 655 Morris Ave)
- Any other lots created from the split
""")

