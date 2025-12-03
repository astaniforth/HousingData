#!/usr/bin/env python3
"""
Check DOB NOW for ANY job type (not just New Building)
"""

import requests
import pandas as pd

DOBNOW_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"

buildings = [
    {"id": 50497, "bin": "2002441", "name": "655 Morris Avenue"},
    {"id": 52722, "bin": "3326478", "name": "Our Lady of Lourdes"},
    {"id": 58479, "bin": "2092605", "name": "SOUNDVIEW HOMES - PHASE III"},
]

print("=" * 80)
print("CHECKING DOB NOW FOR ANY JOB TYPE")
print("=" * 80)

for building in buildings:
    print(f"\n{'='*80}")
    print(f"Building {building['id']}: {building['name']}")
    print(f"BIN: {building['bin']}")
    print(f"{'='*80}")
    
    # Query DOB NOW for ANY job type
    try:
        response = requests.get(
            DOBNOW_URL,
            params={
                "$where": f"bin='{building['bin']}'",
                "$limit": 100,
                "$order": "filing_date DESC"
            },
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                print(f"\n✅ Found {len(df)} total job records in DOB NOW")
                
                # Show job types
                if 'job_type' in df.columns:
                    job_types = df['job_type'].value_counts()
                    print(f"\n📋 Job Types:")
                    for jtype, count in job_types.items():
                        print(f"   {jtype}: {count} records")
                
                # Show job filing numbers and their suffixes
                if 'job_filing_number' in df.columns:
                    print(f"\n📋 Job Filing Number Patterns:")
                    sample_jobs = df['job_filing_number'].head(10).tolist()
                    for job in sample_jobs:
                        print(f"   {job}")
                    
                    # Check how many end with -I1
                    i1_count = df['job_filing_number'].astype(str).str.endswith('-I1', na=False).sum()
                    print(f"\n   Jobs ending with -I1 (initial): {i1_count}")
                    
                # Show a few recent jobs
                display_cols = ['job_filing_number', 'job_type', 'filing_status', 'filing_date', 'approved_date']
                existing_cols = [col for col in display_cols if col in df.columns]
                print(f"\n📅 Most recent jobs:")
                print(df[existing_cols].head(5).to_string(index=False))
            else:
                print(f"\n❌ No job records found in DOB NOW for this BIN")
        else:
            print(f"❌ API error: {response.status_code}")
    except Exception as e:
        print(f"❌ Exception: {e}")

print("\n" + "=" * 80)
print("SUMMARY - DOB NOW vs BISWEB")
print("=" * 80)
print("""
Key Findings:
- BISWEB (legacy system): Has A1/A2/A3 alteration jobs for these buildings
- DOB NOW (new system): Need to check if has any jobs, including alterations

DOB NOW job types might include:
- "New Building" (what we currently query for)
- "Alteration" (various types)
- Other job types

If DOB NOW also has alteration jobs, we may want to include those as well.
""")

