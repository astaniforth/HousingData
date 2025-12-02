#!/usr/bin/env python3
"""
Debug why DOB NOW job S00587462-I1 isn't showing up in results
"""

import sys
sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")

import requests
import pandas as pd

DOB_NOW_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"
job_number = "S00587462-I1"

print("=" * 80)
print("INVESTIGATING DOB NOW JOB:", job_number)
print("=" * 80)

# Try to find this job in DOB NOW
print("\nQuerying DOB NOW API for this job...")
response = requests.get(
    DOB_NOW_URL,
    params={
        "$where": f"job_filing_number='{job_number}'",
        "$limit": 10
    },
    timeout=30
)

if response.status_code == 200:
    data = response.json()
    print(f"\n✅ Found {len(data)} records")
    
    if data:
        df = pd.DataFrame(data)
        
        # Show key fields
        print(f"\nColumns: {df.columns.tolist()}")
        
        # Show the record
        display_cols = ['job_filing_number', 'job_type', 'filing_status', 'bin', 'borough', 'house_no', 'street_name']
        date_cols = ['filing_date', 'approved_date', 'first_permit_date']
        for col in date_cols:
            if col in df.columns:
                display_cols.append(col)
        
        existing_cols = [col for col in display_cols if col in df.columns]
        print(df[existing_cols].to_string(index=False))
        
        # Check if it has a BIN
        if 'bin' in df.columns:
            bin_val = df['bin'].values[0]
            print(f"\n📍 BIN: {bin_val}")
            
            # Check if this BIN is in our HPD data
            print(f"\nChecking if BIN {bin_val} is in HPD data...")
            hpd_file = "/Users/andrewstaniforth/Documents/Programming/HousingData/data/raw/Affordable_Housing_Production_by_Building.csv"
            try:
                hpd_df = pd.read_csv(hpd_file)
                matches = hpd_df[hpd_df['BIN'].astype(str).str.contains(str(bin_val), na=False)]
                if not matches.empty:
                    print(f"✅ BIN {bin_val} IS in HPD data ({len(matches)} buildings)")
                    print(f"   Project IDs: {matches['Project ID'].tolist()}")
                else:
                    print(f"❌ BIN {bin_val} NOT in HPD data")
            except Exception as e:
                print(f"Could not check HPD data: {e}")
        
        # Check the job type
        if 'job_type' in df.columns:
            job_type = df['job_type'].values[0]
            print(f"\n📋 Job Type: {job_type}")
            if job_type != 'New Building':
                print(f"⚠️  Job type is '{job_type}', not 'New Building' - this might be filtered out!")
        
        # Check filing status
        if 'filing_status' in df.columns:
            status = df['filing_status'].values[0]
            print(f"📋 Filing Status: {status}")
    else:
        print("❌ No records found for this job number")
else:
    print(f"❌ Error: HTTP {response.status_code}")
    print(response.text[:500])

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Job: {job_number}")
print("\nPossible issues:")
print("1. Job type might not be 'New Building' (we filter for NB/New Building only)")
print("2. BIN might not be in our HPD filtered dataset")
print("3. Job might be filtered out by our doc__/I1 filtering logic")

