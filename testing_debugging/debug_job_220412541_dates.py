#!/usr/bin/env python3
"""
Check what date fields job 220412541 has in BISWEB
"""

import requests
import pandas as pd

BISWEB_URL = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"
bin_number = 2124684
job_number = "220412541"

print(f"🔍 Checking date fields for job {job_number}")
print()

response = requests.get(
    BISWEB_URL,
    params={
        "$where": f"bin__='{bin_number}' AND job__='{job_number}'",
        "$limit": 1000
    },
    timeout=30
)

if response.status_code == 200:
    data = response.json()
    print(f"✅ Found {len(data)} records for job {job_number}")
    
    if data:
        df = pd.DataFrame(data)
        
        # Look for ALL date columns
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        print(f"\n📅 Date columns in the data: {date_cols}")
        
        # Show date values for this job
        if date_cols:
            print(f"\n📊 Date values for job {job_number}:")
            display_cols = ['job__', 'job_type'] + date_cols
            existing_cols = [col for col in display_cols if col in df.columns]
            print(df[existing_cols].drop_duplicates().to_string(index=False))
        
        # Check doc__ values
        if 'job_doc___' in df.columns:
            doc_values = df['job_doc___'].unique()
            print(f"\n📋 job_doc___ values: {doc_values}")
        
        # Check job_type
        if 'job_type' in df.columns:
            job_type_values = df['job_type'].unique()
            print(f"📋 job_type values: {job_type_values}")
        
        # Check filing_status
        if 'filing_status' in df.columns:
            filing_status_values = df['filing_status'].unique()
            print(f"📋 filing_status values: {filing_status_values}")
        
        # Show all unique combinations
        print(f"\n📊 All unique records (removing duplicate permits):")
        key_cols = ['job__', 'job_type', 'job_doc___', 'filing_date', 'issuance_date', 'permit_status', 'filing_status']
        existing_cols = [col for col in key_cols if col in df.columns]
        print(df[existing_cols].drop_duplicates().to_string(index=False))
        
else:
    print(f"❌ Error: {response.status_code}")
    print(response.text[:500])

