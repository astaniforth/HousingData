#!/usr/bin/env python3
"""
Check BOTH BISWEB and DOB NOW for ALL NB filings on block 2441
"""

import requests
import pandas as pd

BISWEB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
DOBNOW_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"

borough_code = "2"  # Bronx
borough_name = "BRONX"
block = "2441"

print("=" * 80)
print(f"COMPREHENSIVE CHECK: BLOCK {borough_code}/{block} (BRONX)")
print("=" * 80)

# Check BISWEB
print("\n🔍 BISWEB: Checking for NB filings...")
try:
    response = requests.get(
        BISWEB_URL,
        params={
            "$where": f"borough='{borough_name}' AND block='{block}' AND job_type='NB'",
            "$limit": 1000
        },
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        if data:
            df = pd.DataFrame(data)
            print(f"   ✅ Found {len(df)} NB records in BISWEB")
            
            if 'lot' in df.columns:
                lots = df['lot'].unique()
                print(f"   Lots with NB filings: {sorted(lots)}")
        else:
            print(f"   ❌ No NB records found in BISWEB on block {block}")
    else:
        print(f"   ❌ API error: {response.status_code}")
except Exception as e:
    print(f"   ❌ Exception: {e}")

# Check DOB NOW
print("\n🔍 DOB NOW: Checking for New Building filings...")
try:
    response = requests.get(
        DOBNOW_URL,
        params={
            "$where": f"borough='{borough_name}' AND block='{block}' AND job_type='New Building'",
            "$limit": 1000
        },
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        if data:
            df = pd.DataFrame(data)
            print(f"   ✅ Found {len(df)} New Building records in DOB NOW")
            
            if 'lot' in df.columns:
                lots = df['lot'].unique()
                print(f"   Lots with New Building filings: {sorted(lots)}")
                
            # Show the records
            display_cols = ['job_filing_number', 'bin', 'house_no', 'street_name', 'lot', 'filing_date']
            existing_cols = [col for col in display_cols if col in df.columns]
            print(f"\n   Records found:")
            print(df[existing_cols].to_string(index=False))
        else:
            print(f"   ❌ No New Building records found in DOB NOW on block {block}")
    else:
        print(f"   ❌ API error: {response.status_code}")
except Exception as e:
    print(f"   ❌ Exception: {e}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Block {block} (Bronx) - comprehensive check for NB/New Building filings")
print("Result: Check output above to see if either API has NB filings")


