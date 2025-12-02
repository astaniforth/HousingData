#!/usr/bin/env python3
"""
Debug script to investigate CO dates for Building ID 64608 (BIN 3427387)
User reports CO-000051396 from 2024 but we're showing 2025-10-30
"""

import sys
import json
import requests
import pandas as pd

# Building info
building_id = 64608
bin_number = 3427387
bbl = "3020030037"

print(f"🔍 Investigating CO dates for Building ID {building_id}")
print(f"   BIN: {bin_number}")
print(f"   BBL: {bbl}")
print()

# DOB NOW CO API
dobnow_co_api = "https://data.cityofnewyork.us/resource/pkdm-hqz6.json"
# DOB CO API
dob_co_api = "https://data.cityofnewyork.us/resource/bs8b-p36w.json"

print("=" * 80)
print("QUERYING DOB NOW CO API (pkdm-hqz6)")
print("=" * 80)

try:
    # Query by BIN
    response = requests.get(
        dobnow_co_api,
        params={
            "$where": f"bin='{bin_number}'",
            "$limit": 1000,
            "$order": "c_of_o_issuance_date ASC"
        },
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ Found {len(data)} records")
        
        if data:
            df = pd.DataFrame(data)
            print(f"\nColumns: {df.columns.tolist()}")
            
            # Look for date columns
            date_cols = [col for col in df.columns if 'date' in col.lower()]
            print(f"\nDate columns: {date_cols}")
            
            # Show all records with key fields
            display_cols = ['bin', 'c_of_o_issuance_date']
            if 'application_number' in df.columns:
                display_cols.append('application_number')
            if 'c_of_o_filing_type' in df.columns:
                display_cols.append('c_of_o_filing_type')
            if 'c_of_o_status' in df.columns:
                display_cols.append('c_of_o_status')
            
            existing_cols = [col for col in display_cols if col in df.columns]
            print(f"\nAll records (sorted by c_of_o_issuance_date):")
            print(df[existing_cols].to_string(index=False))
            
            # Check for CO-000051396 specifically
            if 'application_number' in df.columns:
                co_match = df[df['application_number'].astype(str).str.contains('000051396', na=False)]
                if not co_match.empty:
                    print(f"\n🎯 FOUND CO-000051396:")
                    print(co_match[existing_cols].to_string(index=False))
        else:
            print("No records found")
    else:
        print(f"❌ Error: HTTP {response.status_code}")
        print(response.text[:500])
        
except Exception as e:
    print(f"❌ Exception: {e}")

print("\n" + "=" * 80)
print("QUERYING DOB CO API (bs8b-p36w)")
print("=" * 80)

try:
    # Query by bin_number
    response = requests.get(
        dob_co_api,
        params={
            "$where": f"bin_number='{bin_number}'",
            "$limit": 1000,
            "$order": "c_o_issue_date ASC"
        },
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ Found {len(data)} records")
        
        if data:
            df = pd.DataFrame(data)
            print(f"\nColumns: {df.columns.tolist()}")
            
            # Look for date columns
            date_cols = [col for col in df.columns if 'date' in col.lower()]
            print(f"\nDate columns: {date_cols}")
            
            # Show all records with key fields
            display_cols = ['bin_number', 'c_o_issue_date']
            if 'job_number' in df.columns:
                display_cols.append('job_number')
            if 'issue_type' in df.columns:
                display_cols.append('issue_type')
            if 'application_status_raw' in df.columns:
                display_cols.append('application_status_raw')
            
            existing_cols = [col for col in display_cols if col in df.columns]
            print(f"\nAll records (sorted by c_o_issue_date):")
            print(df[existing_cols].to_string(index=False))
            
            # Check for CO-000051396 specifically - this is from DOB NOW, so check job_number
            if 'job_number' in df.columns:
                co_match = df[df['job_number'].astype(str).str.contains('000051396', na=False)]
                if not co_match.empty:
                    print(f"\n🎯 FOUND CO-000051396:")
                    print(co_match[existing_cols].to_string(index=False))
        else:
            print("No records found")
    else:
        print(f"❌ Error: HTTP {response.status_code}")
        print(response.text[:500])
        
except Exception as e:
    print(f"❌ Exception: {e}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"User reported: CO-000051396 from 2024")
print(f"We're showing: 2025-10-30")
print(f"Need to verify which CO records exist and their dates")

