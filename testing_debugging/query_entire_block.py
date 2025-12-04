#!/usr/bin/env python3
"""
Test if we can find NB filings by querying ALL lots on block 2441
This is a more aggressive approach for lot splits
"""

import requests
import pandas as pd

BISWEB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"

borough = "2"
block = "2441"

print("=" * 80)
print(f"QUERYING ALL NB FILINGS ON BLOCK {borough}/{block}")
print("=" * 80)

print("\n🔍 Searching for ALL New Building filings on this block...")
print("(This will show us what lots have NB filings)\n")

try:
    response = requests.get(
        BISWEB_URL,
        params={
            "$where": f"borough='{borough}' AND block='{block}' AND job_type='NB'",
            "$limit": 1000,
            "$order": "pre__filing_date DESC"
        },
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        if data:
            df = pd.DataFrame(data)
            print(f"✅ Found {len(df)} NB records on block {block}")
            
            # Group by lot to see what lots have NB filings
            if 'lot' in df.columns:
                lot_counts = df.groupby('lot').size().reset_index(name='count')
                lot_counts = lot_counts.sort_values('count', ascending=False)
                
                print(f"\n📊 New Building filings by lot:")
                for idx, row in lot_counts.iterrows():
                    print(f"   Lot {row['lot']}: {row['count']} NB filings")
                
                # Show details for first few lots
                print(f"\n📋 Sample NB filings on this block:")
                display_cols = ['lot', 'bin__', 'house__', 'street_name', 'job__', 'pre__filing_date', 'job_status', 'doc__']
                existing_cols = [col for col in display_cols if col in df.columns]
                
                # Filter to doc__='01' only
                if 'doc__' in df.columns:
                    doc_01_df = df[df['doc__'].astype(str).str.zfill(2) == '01']
                    print(f"\nShowing only doc__='01' filings ({len(doc_01_df)} records):")
                    print(doc_01_df[existing_cols].head(20).to_string(index=False))
                else:
                    print(df[existing_cols].head(20).to_string(index=False))
                    
                # Check specific address
                if 'house__' in df.columns and 'street_name' in df.columns:
                    morris_ave = df[df['street_name'].str.contains('MORRIS', na=False, case=False)]
                    if not morris_ave.empty:
                        print(f"\n🎯 NB filings on MORRIS AVENUE:")
                        print(morris_ave[existing_cols].to_string(index=False))
        else:
            print(f"❌ No NB records found on block {block}")
    else:
        print(f"❌ API error: {response.status_code}")
except Exception as e:
    print(f"❌ Exception: {e}")

print("\n" + "=" * 80)
print("SOLUTION OPTIONS")
print("=" * 80)
print("""
For lot splits, we have a few options:

1. **Query entire block as fallback** (aggressive)
   - When BBL query fails, query ALL lots on that block
   - Pro: Will find NB filings regardless of lot splits
   - Con: Might match wrong building on same block
   - Implementation: Add a "block-level fallback" after BBL fails

2. **Query multiple lot patterns** (targeted)
   - For lot 1, also try common split patterns:
     * Lot 200, 201, 202... (split lots)
     * Lot 7501+ (condo lots)
   - Pro: More precise than whole block
   - Con: Won't catch all patterns

3. **Address-based fallback** (most reliable)
   - Use query_dob_by_address() for buildings without DOB data
   - Match on house number + street name
   - Pro: Most accurate for finding the right building
   - Con: Address formatting must match

RECOMMENDATION:
Use option 3 (address-based fallback) since we already have this function!
For buildings without DOB data after BIN/BBL queries, fall back to address.
""")


