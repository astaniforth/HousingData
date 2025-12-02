#!/usr/bin/env python3
"""
Investigate job 220124381 to see what the actual pre__filing_date is in the raw DOB data.
"""

import pandas as pd
import requests
import json

print("=" * 70)
print("INVESTIGATING JOB 220124381 - DIRECT API QUERY")
print("=" * 70)

# Query the DOB BISWEB API directly for this job
job_number = "220124381"
bin_number = "2129098"

# DOB Job Application Filings API
url = f"https://data.cityofnewyork.us/resource/ic3t-wcy2.json?job__={job_number}"
print(f"\n1. Querying DOB BISWEB API for job {job_number}...")
print(f"   URL: {url}")

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    print(f"   Found {len(data)} records")
    
    if data:
        for i, record in enumerate(data):
            print(f"\n   Record {i}:")
            print(f"     job__: {record.get('job__')}")
            print(f"     doc__: {record.get('doc__')}")
            print(f"     bin__: {record.get('bin__')}")
            print(f"     pre__filing_date: {record.get('pre__filing_date')}")
            print(f"     paid: {record.get('paid')}")
            print(f"     approved: {record.get('approved')}")
            print(f"     assigned: {record.get('assigned')}")
            print(f"     fully_paid: {record.get('fully_paid')}")
            print(f"     fully_permitted: {record.get('fully_permitted')}")
            print(f"     job_type: {record.get('job_type')}")
            print(f"     job_status: {record.get('job_status')}")
else:
    print(f"   Error: {response.status_code}")

# Also query by BIN to see all applications for this building
print(f"\n2. Querying DOB BISWEB API for BIN {bin_number}...")
url_bin = f"https://data.cityofnewyork.us/resource/ic3t-wcy2.json?bin__={bin_number}&job_type=NB"
print(f"   URL: {url_bin}")

response_bin = requests.get(url_bin)
if response_bin.status_code == 200:
    data_bin = response_bin.json()
    print(f"   Found {len(data_bin)} NB records for BIN {bin_number}")
    
    if data_bin:
        print("\n   All NB applications for this BIN:")
        for i, record in enumerate(data_bin):
            print(f"\n   Record {i}:")
            print(f"     job__: {record.get('job__')}")
            print(f"     doc__: {record.get('doc__')}")
            print(f"     pre__filing_date: {record.get('pre__filing_date')}")
            print(f"     paid: {record.get('paid')}")
            print(f"     approved: {record.get('approved')}")
            print(f"     job_status: {record.get('job_status')}")
else:
    print(f"   Error: {response_bin.status_code}")

print("\n" + "=" * 70)
print("ANALYSIS COMPLETE")
print("=" * 70)

