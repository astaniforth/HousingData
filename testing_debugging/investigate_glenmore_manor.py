#!/usr/bin/env python3
"""
Investigate why building 1004735 (GLENMORE MANOR) with BBL 3036920001
is not showing DOB data despite having a New Building permit in DOB NOW.

This script will:
1. Query DOB APIs directly to see if we can find the permit
2. Check if the issue is in our fetch logic
3. Check if the issue is in our merge/join logic
"""

import sys
import requests
import pandas as pd

sys.path.insert(0, '/Users/andrewstaniforth/Documents/Programming/HousingData')

from query_dob_filings import (
    query_dob_bisweb_bin, query_dob_bisweb_bbl,
    query_dobnow_bin, query_dobnow_bbl,
    decompose_bbl, pad_block, pad_lot,
    batch_get_condo_base_bbls, query_dob_for_condo_bbls
)

# Building details
BUILDING_ID = "1004735"
PROJECT_ID = "67647"
PROJECT_NAME = "GLENMORE MANOR"
BIN = "3000000"  # This looks like a placeholder BIN
BBL = "3036920001"

DOB_NOW_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"
DOB_BISWEB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"

print("=" * 70)
print(f"INVESTIGATING: {PROJECT_NAME}")
print(f"Building ID: {BUILDING_ID}, BBL: {BBL}, BIN: {BIN}")
print("=" * 70)

# Step 1: Query DOB NOW directly by BBL
print("\n📍 STEP 1: Query DOB NOW API directly by BBL")
print("-" * 50)

# Decompose BBL
borough, block, lot = decompose_bbl(BBL)[:3]
print(f"Decomposed BBL: Borough={borough}, Block={block}, Lot={lot}")

# Query DOB NOW for New Building
# DOB NOW uses unpadded block/lot
block_unpadded = str(int(block))
lot_unpadded = str(int(lot))

query = f"job_type='New Building' AND borough='{borough}' AND block='{block_unpadded}' AND lot='{lot_unpadded}'"
print(f"DOB NOW Query: {query}")

params = {'$where': query, '$limit': 100}
response = requests.get(DOB_NOW_URL, params=params, timeout=30)
dobnow_data = response.json()

if dobnow_data:
    print(f"✅ Found {len(dobnow_data)} New Building records in DOB NOW!")
    for record in dobnow_data[:3]:
        print(f"  Job: {record.get('job_filing_number')}")
        print(f"  BIN: {record.get('bin')}")
        print(f"  Filing Date: {record.get('filing_date')}")
        print(f"  Status: {record.get('filing_status')}")
        print()
else:
    print("❌ No New Building records found in DOB NOW")

# Also try with any job type
print("\nTrying DOB NOW with ANY job type...")
query_any = f"borough='{borough}' AND block='{block_unpadded}' AND lot='{lot_unpadded}'"
params = {'$where': query_any, '$limit': 100}
response = requests.get(DOB_NOW_URL, params=params, timeout=30)
dobnow_any_data = response.json()

if dobnow_any_data:
    job_types = set(r.get('job_type') for r in dobnow_any_data)
    print(f"Found {len(dobnow_any_data)} total records, job types: {job_types}")
else:
    print("No records at all in DOB NOW for this BBL")

# Step 2: Query BISWEB directly by BBL
print("\n📍 STEP 2: Query BISWEB API directly by BBL")
print("-" * 50)

# BISWEB uses padded block/lot
query_bisweb = f"job_type='NB' AND borough='{borough}' AND block='{block}' AND lot='{lot}'"
print(f"BISWEB Query: {query_bisweb}")

params = {'$where': query_bisweb, '$limit': 100}
response = requests.get(DOB_BISWEB_URL, params=params, timeout=30)
bisweb_data = response.json()

if bisweb_data:
    print(f"✅ Found {len(bisweb_data)} NB records in BISWEB!")
    for record in bisweb_data[:3]:
        print(f"  Job: {record.get('job__')}")
        print(f"  BIN: {record.get('bin__')}")
        print(f"  Pre-filing: {record.get('pre__filing_date')}")
        print()
else:
    print("❌ No NB records found in BISWEB")

# Step 3: Check if it's a condo
print("\n📍 STEP 3: Check if this is a condo property")
print("-" * 50)

condo_result = batch_get_condo_base_bbls([BBL])
if condo_result:
    print(f"✅ This IS a condo property!")
    print(f"Related BBLs: {condo_result}")
else:
    print("❌ This is NOT a condo property")

# Step 4: Test our query functions
print("\n📍 STEP 4: Test our query_dobnow_bbl function")
print("-" * 50)

bbl_tuple = (borough, block, lot)
print(f"Calling query_dobnow_bbl([{bbl_tuple}])")
dobnow_result = query_dobnow_bbl([bbl_tuple])

if not dobnow_result.empty:
    print(f"✅ query_dobnow_bbl found {len(dobnow_result)} records")
    print(dobnow_result[['job_filing_number', 'bin', 'job_type', 'filing_date']].head() if 'job_filing_number' in dobnow_result.columns else dobnow_result.head())
else:
    print("❌ query_dobnow_bbl returned empty DataFrame")

# Step 5: Check the actual DOB NOW query our function makes
print("\n📍 STEP 5: Debug query_dobnow_bbl query construction")
print("-" * 50)

# The function uses unpadded values, let's verify
block_for_query = str(int(block)) if block else block
lot_for_query = str(int(lot)) if lot else lot
print(f"Our function should query: borough='{borough}', block='{block_for_query}', lot='{lot_for_query}'")

# Compare to what works
print(f"Direct query that worked: block='{block_unpadded}', lot='{lot_unpadded}'")

# Step 6: Check the BIN situation
print("\n📍 STEP 6: Investigate the BIN")
print("-" * 50)
print(f"HPD BIN: {BIN}")

if dobnow_data:
    dob_bins = set(r.get('bin') for r in dobnow_data)
    print(f"DOB NOW BINs for this BBL: {dob_bins}")
    
    if BIN in dob_bins or str(BIN) in [str(b) for b in dob_bins]:
        print("✅ BINs match!")
    else:
        print("❌ BINs DON'T match - this is why BIN query fails")
        print("   The BBL query should still find it though...")

# Summary
print("\n" + "=" * 70)
print("DIAGNOSIS SUMMARY")
print("=" * 70)

if dobnow_data:
    print("✅ DOB NOW has New Building permits for this BBL")
    print("   Issue is likely in our FETCH or MERGE logic")
    
    if not dobnow_result.empty:
        print("✅ Our query_dobnow_bbl function DOES find the records")
        print("   Issue is likely in the MERGE/JOIN logic in the notebook")
    else:
        print("❌ Our query_dobnow_bbl function does NOT find the records")
        print("   Issue is in our FETCH logic - check query construction")
else:
    print("❌ DOB NOW does NOT have New Building permits for this BBL")
    print("   The data simply doesn't exist in DOB NOW")

