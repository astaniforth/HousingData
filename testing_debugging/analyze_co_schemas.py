#!/usr/bin/env python3
"""
Analyze the column schemas of both CO APIs to identify equivalent columns.
"""

import requests
import pandas as pd

# DOB NOW CO API
dobnow_co_api = "https://data.cityofnewyork.us/resource/pkdm-hqz6.json"
# DOB CO API  
dob_co_api = "https://data.cityofnewyork.us/resource/bs8b-p36w.json"

print("=" * 80)
print("ANALYZING CO API SCHEMAS")
print("=" * 80)

# Get a sample record from each API to see all columns
print("\n📋 DOB NOW CO API (pkdm-hqz6) - Fetching sample...")
response1 = requests.get(dobnow_co_api, params={"$limit": 5}, timeout=30)
if response1.status_code == 200:
    data1 = response1.json()
    if data1:
        df1 = pd.DataFrame(data1)
        dobnow_cols = sorted(df1.columns.tolist())
        print(f"\n✅ DOB NOW CO has {len(dobnow_cols)} columns:")
        for col in dobnow_cols:
            print(f"  - {col}")
    else:
        print("No data returned")
        dobnow_cols = []
else:
    print(f"Error: {response1.status_code}")
    dobnow_cols = []

print("\n📋 Legacy DOB CO API (bs8b-p36w) - Fetching sample...")
response2 = requests.get(dob_co_api, params={"$limit": 5}, timeout=30)
if response2.status_code == 200:
    data2 = response2.json()
    if data2:
        df2 = pd.DataFrame(data2)
        dob_cols = sorted(df2.columns.tolist())
        print(f"\n✅ Legacy DOB CO has {len(dob_cols)} columns:")
        for col in dob_cols:
            print(f"  - {col}")
    else:
        print("No data returned")
        dob_cols = []
else:
    print(f"Error: {response2.status_code}")
    dob_cols = []

# Identify equivalent columns
print("\n" + "=" * 80)
print("COLUMN MAPPING - Same Info, Different Names")
print("=" * 80)

# Define known equivalencies based on the data
equivalencies = [
    ("bin", "bin_number", "Building Identification Number"),
    ("c_of_o_issuance_date", "c_o_issue_date", "Certificate of Occupancy issue date"),
    ("zip_code", "postcode", "ZIP/Postal code"),
    ("c_of_o_status", "filing_status_raw", "Status of the CO filing"),
    ("c_of_o_filing_type", "issue_type", "Type of CO (Initial, Final, Renewal, etc.)"),
    ("application_number", "job_number", "Unique identifier for the CO filing"),
    ("job_filing_name", "job_number", "Job/Filing identifier"),
    ("community_board", "community_board", "Community board (same name!)"),
    ("borough", "borough", "Borough (same name!)"),
    ("house_no", "house_number", "House number"),
    ("street_name", "street_name", "Street name (same name!)"),
    ("block", "block", "Tax block (same name!)"),
    ("lot", "lot", "Tax lot (same name!)"),
    ("latitude", "latitude", "Latitude (same name!)"),
    ("longitude", "longitude", "Longitude (same name!)"),
    ("bbl", "bbl", "Borough-Block-Lot (same name!)"),
]

print("\n🔄 Equivalent columns (same information, different names):\n")
for dobnow_col, dob_col, description in equivalencies:
    dobnow_has = "✅" if dobnow_col in dobnow_cols else "❌"
    dob_has = "✅" if dob_col in dob_cols else "❌"
    print(f"  {dobnow_has} DOB NOW: {dobnow_col:30s} ↔️  {dob_has} Legacy: {dob_col:25s} | {description}")

# Find columns unique to each API
print("\n" + "=" * 80)
print("UNIQUE COLUMNS")
print("=" * 80)

# Normalize column names for comparison (lowercase, remove underscores)
def normalize(col):
    return col.lower().replace('_', '').replace(' ', '')

dobnow_normalized = {normalize(col): col for col in dobnow_cols}
dob_normalized = {normalize(col): col for col in dob_cols}

unique_to_dobnow = []
unique_to_dob = []

for col in dobnow_cols:
    norm = normalize(col)
    # Check if this normalized version exists in the other API
    if norm not in dob_normalized:
        # Also check if it's not in our equivalencies list
        is_equivalent = any(col == eq[0] for eq in equivalencies)
        if not is_equivalent:
            unique_to_dobnow.append(col)

for col in dob_cols:
    norm = normalize(col)
    if norm not in dobnow_normalized:
        is_equivalent = any(col == eq[1] for eq in equivalencies)
        if not is_equivalent:
            unique_to_dob.append(col)

print(f"\n📌 Unique to DOB NOW CO ({len(unique_to_dobnow)} columns):")
for col in sorted(unique_to_dobnow):
    print(f"  - {col}")

print(f"\n📌 Unique to Legacy DOB CO ({len(unique_to_dob)} columns):")
for col in sorted(unique_to_dob):
    print(f"  - {col}")

print("\n" + "=" * 80)
print("SAMPLE DATA COMPARISON")
print("=" * 80)

if data1 and data2:
    print("\n📊 DOB NOW CO - Sample record:")
    sample1 = data1[0]
    for key in ['bin', 'c_of_o_issuance_date', 'c_of_o_status', 'c_of_o_filing_type', 'application_number']:
        if key in sample1:
            print(f"  {key}: {sample1[key]}")
    
    print("\n📊 Legacy DOB CO - Sample record:")
    sample2 = data2[0]
    for key in ['bin_number', 'c_o_issue_date', 'filing_status_raw', 'issue_type', 'job_number']:
        if key in sample2:
            print(f"  {key}: {sample2[key]}")

