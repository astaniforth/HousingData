#!/usr/bin/env python3
"""
Check if job 220412541 exists in the CORRECT BISWEB API (ic3t-wcy2 - Job Application Filings)
This is the API our workflow actually uses!
"""

import requests
import pandas as pd

# This is the API our workflow uses!
BISWEB_JOB_API = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
BISWEB_PERMIT_API = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"

bin_number = 2124684
job_number = "220412541"

print("=" * 80)
print("QUERYING CORRECT API: BISWEB JOB APPLICATION FILINGS (ic3t-wcy2)")
print("This is the API our workflow uses!")
print("=" * 80)

response = requests.get(
    BISWEB_JOB_API,
    params={
        "$where": f"bin__='{bin_number}'",
        "$limit": 1000,
        "$order": "job__ DESC"
    },
    timeout=30
)

if response.status_code == 200:
    data = response.json()
    print(f"\n✅ Found {len(data)} job records for BIN {bin_number}")
    
    if data:
        df = pd.DataFrame(data)
        
        print(f"\nColumns available: {sorted(df.columns.tolist())}")
        
        # Look for date columns
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        print(f"\n📅 Date columns: {date_cols}")
        
        # Show all jobs
        display_cols = ['job__', 'doc__', 'job_type', 'bin__', 'job_status']
        if 'pre__filing_date' in df.columns:
            display_cols.append('pre__filing_date')
        if 'paid' in df.columns:
            display_cols.append('paid')
        if 'fully_permitted' in df.columns:
            display_cols.append('fully_permitted')
        
        existing_cols = [col for col in display_cols if col in df.columns]
        
        print(f"\n📊 All job filings for BIN {bin_number}:")
        print(df[existing_cols].to_string(index=False))
        
        # Check for the specific job
        if 'job__' in df.columns:
            job_match = df[df['job__'].astype(str) == job_number]
            if not job_match.empty:
                print(f"\n🎯 FOUND job {job_number}!")
                print(job_match[existing_cols].to_string(index=False))
                
                # Show ALL columns for this job
                print(f"\n📋 All data for job {job_number}:")
                for col in sorted(job_match.columns):
                    val = job_match[col].values[0]
                    print(f"  {col}: {val}")
            else:
                print(f"\n❌ Job {job_number} NOT FOUND in this API!")
                print(f"\n⚠️  This means the job exists in the PERMIT API but NOT in the JOB FILING API")
                print(f"    Our workflow queries the JOB FILING API, so it won't find this job.")
    else:
        print("No records found")
else:
    print(f"❌ Error: {response.status_code}")
    print(response.text[:500])

print("\n" + "=" * 80)
print("COMPARISON: PERMIT API vs JOB FILING API")
print("=" * 80)
print("\nPERMIT API (ipu4-2q9a) - What we found earlier:")
print(f"  - Job {job_number} EXISTS ✅")
print(f"  - Has 13 permit records")
print(f"  - Has dates: filing_date, issuance_date, etc.")
print(f"  - This is PERMIT-level data (multiple permits per job)")

print("\nJOB FILING API (ic3t-wcy2) - What our workflow uses:")
print(f"  - Need to check if job {job_number} exists here")
print(f"  - This is JOB-level data (one record per job filing)")
print(f"  - Our workflow queries THIS API, not the permit API")

