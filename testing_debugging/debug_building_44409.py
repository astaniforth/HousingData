#!/usr/bin/env python3
"""
Debug script to investigate why Building ID 44409 (BIN 2124684) has no DOB record
User can see NB 220412541 in BIS but our pipeline didn't find it.
"""

import sys
import requests
import pandas as pd

# Building info
building_id = 44409
project_name = "Crotona Terrace II"
bin_number = 2124684
bbl = "2029847503"
address = "1825 BOSTON ROAD, Bronx"
known_job = "220412541"

print(f"🔍 Investigating DOB records for Building ID {building_id}")
print(f"   Project: {project_name}")
print(f"   BIN: {bin_number}")
print(f"   BBL: {bbl}")
print(f"   Address: {address}")
print(f"   Known Job: NB {known_job}")
print()

# DOB APIs
BISWEB_URL = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"
DOBNOW_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"

print("=" * 80)
print("STEP 1: CHECK OUTPUT FILE - Does building have DOB data?")
print("=" * 80)

# Check if building has DOB data in our output
output_file = "/Users/andrewstaniforth/Documents/Programming/HousingData/output/hpd_multifamily_finance_new_construction_with_all_dates.csv"
try:
    df = pd.read_csv(output_file)
    building_row = df[df['Project ID'] == building_id]
    if not building_row.empty:
        print(f"\n✅ Found building in output file")
        print(f"   earliest_dob_date: {building_row['earliest_dob_date'].values[0]}")
        print(f"   earliest_dob_date_source: {building_row['earliest_dob_date_source'].values[0] if 'earliest_dob_date_source' in building_row.columns else 'N/A'}")
        print(f"   application_number: {building_row['application_number'].values[0] if 'application_number' in building_row.columns else 'N/A'}")
        print(f"   earliest_co_date: {building_row['earliest_co_date'].values[0] if 'earliest_co_date' in building_row.columns else 'N/A'}")
    else:
        print(f"\n❌ Building not found in output file")
except Exception as e:
    print(f"❌ Error reading output file: {e}")

print("\n" + "=" * 80)
print("STEP 2: QUERY DOB BISWEB BY BIN")
print("=" * 80)

try:
    response = requests.get(
        BISWEB_URL,
        params={
            "$where": f"bin__='{bin_number}'",
            "$limit": 1000,
            "$order": "job__ DESC"
        },
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ Found {len(data)} records")
        
        if data:
            df_bisweb = pd.DataFrame(data)
            print(f"\nColumns available: {df_bisweb.columns.tolist()}")
            
            # Show key fields
            display_cols = ['job__', 'doc__', 'job_type', 'bin__', 'pre__filing_date', 'paid', 'fully_permitted', 'job_status']
            existing_cols = [col for col in display_cols if col in df_bisweb.columns]
            
            print(f"\nAll records:")
            print(df_bisweb[existing_cols].to_string(index=False))
            
            # Check for the known job
            if 'job__' in df_bisweb.columns:
                job_match = df_bisweb[df_bisweb['job__'].astype(str).str.contains(known_job, na=False)]
                if not job_match.empty:
                    print(f"\n🎯 FOUND known job {known_job}:")
                    print(job_match[existing_cols].to_string(index=False))
                else:
                    print(f"\n⚠️ Known job {known_job} NOT found in results")
        else:
            print("No records found")
    else:
        print(f"❌ Error: HTTP {response.status_code}")
        print(response.text[:500])
        
except Exception as e:
    print(f"❌ Exception: {e}")

print("\n" + "=" * 80)
print("STEP 3: QUERY DOB NOW BY BIN")
print("=" * 80)

try:
    response = requests.get(
        DOBNOW_URL,
        params={
            "$where": f"bin='{bin_number}'",
            "$limit": 1000,
            "$order": "job_filing_number DESC"
        },
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ Found {len(data)} records")
        
        if data:
            df_dobnow = pd.DataFrame(data)
            print(f"\nColumns available: {df_dobnow.columns.tolist()}")
            
            # Show key fields
            display_cols = ['job_filing_number', 'filing_status', 'job_type', 'bin', 'filing_date', 'approved_date', 'first_permit_date']
            existing_cols = [col for col in display_cols if col in df_dobnow.columns]
            
            print(f"\nAll records:")
            print(df_dobnow[existing_cols].to_string(index=False))
            
            # Check for the known job
            if 'job_filing_number' in df_dobnow.columns:
                job_match = df_dobnow[df_dobnow['job_filing_number'].astype(str).str.contains(known_job, na=False)]
                if not job_match.empty:
                    print(f"\n🎯 FOUND known job {known_job}:")
                    print(job_match[existing_cols].to_string(index=False))
                else:
                    print(f"\n⚠️ Known job {known_job} NOT found in results")
        else:
            print("No records found")
    else:
        print(f"❌ Error: HTTP {response.status_code}")
        print(response.text[:500])
        
except Exception as e:
    print(f"❌ Exception: {e}")

print("\n" + "=" * 80)
print("STEP 4: QUERY DOB BISWEB BY BBL (Fallback)")
print("=" * 80)

# Decompose BBL
bbl_int = int(bbl)
borough = str(bbl_int)[0]
block = str(bbl_int)[1:6].lstrip('0')
lot = str(bbl_int)[6:10].lstrip('0')

print(f"BBL Decomposition: {bbl} → Borough: {borough}, Block: {block}, Lot: {lot}")

try:
    response = requests.get(
        BISWEB_URL,
        params={
            "$where": f"boro='{borough}' AND block='{block}' AND lot='{lot}'",
            "$limit": 1000,
            "$order": "job__ DESC"
        },
        timeout=30
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ Found {len(data)} records")
        
        if data:
            df_bisweb_bbl = pd.DataFrame(data)
            
            # Show key fields
            display_cols = ['job__', 'doc__', 'job_type', 'bin__', 'boro', 'block', 'lot', 'pre__filing_date', 'job_status']
            existing_cols = [col for col in display_cols if col in df_bisweb_bbl.columns]
            
            print(f"\nAll records:")
            print(df_bisweb_bbl[existing_cols].to_string(index=False))
            
            # Check for the known job
            if 'job__' in df_bisweb_bbl.columns:
                job_match = df_bisweb_bbl[df_bisweb_bbl['job__'].astype(str).str.contains(known_job, na=False)]
                if not job_match.empty:
                    print(f"\n🎯 FOUND known job {known_job}:")
                    print(job_match[existing_cols].to_string(index=False))
                    
                    # Check if this is NB
                    if 'job_type' in job_match.columns:
                        job_type = job_match['job_type'].values[0]
                        print(f"   Job Type: {job_type}")
                    if 'doc__' in job_match.columns:
                        doc = job_match['doc__'].values[0]
                        print(f"   Doc Type: {doc}")
                else:
                    print(f"\n⚠️ Known job {known_job} NOT found in BBL results")
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
print(f"Building: {building_id} - {project_name}")
print(f"Expected to find: NB {known_job}")
print(f"Need to determine:")
print(f"  1. Is the job in the API responses above?")
print(f"  2. If yes, why didn't our pipeline capture it?")
print(f"  3. If no, is there a query issue?")

