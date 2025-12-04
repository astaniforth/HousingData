#!/usr/bin/env python3
"""
Check if buildings without NB filings have OTHER job types (A1, A2, A3, etc.)
"""

import requests
import pandas as pd

BISWEB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"

buildings = [
    {"id": 50497, "bin": "2002441", "name": "655 Morris Avenue"},
    {"id": 52722, "bin": "3326478", "name": "Our Lady of Lourdes"},
    {"id": 58479, "bin": "2092605", "name": "SOUNDVIEW HOMES - PHASE III"},
]

print("=" * 80)
print("CHECKING FOR ANY JOB TYPE (not just NB)")
print("=" * 80)

for building in buildings:
    print(f"\n{'='*80}")
    print(f"Building {building['id']}: {building['name']}")
    print(f"BIN: {building['bin']}")
    print(f"{'='*80}")
    
    # Query for ANY job type
    try:
        response = requests.get(
            BISWEB_URL,
            params={
                "$where": f"bin__='{building['bin']}'",
                "$limit": 100,
                "$order": "pre__filing_date DESC"
            },
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                print(f"\n✅ Found {len(df)} total job records")
                
                # Show job types
                if 'job_type' in df.columns:
                    job_types = df['job_type'].value_counts()
                    print(f"\n📋 Job Types:")
                    for jtype, count in job_types.items():
                        print(f"   {jtype}: {count} records")
                
                # Show a few recent jobs
                display_cols = ['job__', 'job_type', 'doc__', 'pre__filing_date', 'job_status']
                existing_cols = [col for col in display_cols if col in df.columns]
                print(f"\n📅 Most recent jobs:")
                print(df[existing_cols].head(5).to_string(index=False))
            else:
                print(f"\n❌ No job records found at all for this BIN")
                print(f"   This BIN may not exist in DOB or has never had any filings")
        else:
            print(f"❌ API error: {response.status_code}")
    except Exception as e:
        print(f"❌ Exception: {e}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print("""
If buildings have NO NB filings in DOB, possible reasons:
1. Project hasn't filed with DOB yet (future/planned)
2. Project is using existing buildings (renovation, not new construction)
3. BIN in HPD data is incorrect/doesn't match DOB
4. Project was never built/cancelled
5. Building filed under a different job type (A1, A2, etc.) not NB

Our workflow only queries for 'NB' (New Building) job types, which is correct
for new construction. If these are truly new construction projects but don't
have NB filings, there may be a data quality issue in either HPD or DOB.
""")


