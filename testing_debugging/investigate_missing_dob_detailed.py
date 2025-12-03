#!/usr/bin/env python3
"""
Investigate specific buildings that have valid BIN/BBL but no DOB data
"""

import sys
sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")

import requests
import pandas as pd
from query_dob_filings import query_dob_bisweb_bin, query_dobnow_bin

# Sample buildings to investigate
buildings = [
    {"id": 50497, "bin": "2002441", "bbl": "2024410001", "address": "635 MORRIS AVENUE, Bronx"},
    {"id": 52722, "bin": "3326478", "bbl": "3034680050", "address": "2025-11-21T00:00:00.000 DESALES PLACE, Brooklyn"},
    {"id": 58479, "bin": "2092605", "bbl": "2035150001", "address": "1715 LACOMBE AVENUE, Bronx"},
]

BISWEB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
DOBNOW_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"

for building in buildings[:3]:  # Check first 3
    print("=" * 80)
    print(f"Building {building['id']}: {building['address']}")
    print(f"BIN: {building['bin']}, BBL: {building['bbl']}")
    print("=" * 80)
    
    # Query BISWEB by BIN
    print(f"\n🔍 Querying BISWEB for BIN {building['bin']}...")
    try:
        response = requests.get(
            BISWEB_URL,
            params={
                "$where": f"bin__='{building['bin']}' AND job_type='NB'",
                "$limit": 100
            },
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"   ✅ Found {len(data)} NB records in BISWEB")
                df = pd.DataFrame(data)
                if 'doc__' in df.columns:
                    doc_01_count = (df['doc__'].astype(str).str.zfill(2) == '01').sum()
                    print(f"   📋 Records with doc__='01': {doc_01_count}")
                    if doc_01_count > 0:
                        doc_01 = df[df['doc__'].astype(str).str.zfill(2) == '01']
                        print(f"   📅 Sample: Job {doc_01['job__'].values[0]}, Date: {doc_01.get('pre__filing_date', pd.Series([None])).values[0]}")
            else:
                print(f"   ❌ No NB records found in BISWEB")
        else:
            print(f"   ❌ API error: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Query DOB NOW by BIN
    print(f"\n🔍 Querying DOB NOW for BIN {building['bin']}...")
    try:
        response = requests.get(
            DOBNOW_URL,
            params={
                "$where": f"bin='{building['bin']}' AND job_type='New Building'",
                "$limit": 100
            },
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"   ✅ Found {len(data)} New Building records in DOB NOW")
                df = pd.DataFrame(data)
                if 'job_filing_number' in df.columns:
                    i1_count = df['job_filing_number'].astype(str).str.endswith('I1').sum()
                    print(f"   📋 Records ending with -I1: {i1_count}")
                    if i1_count > 0:
                        i1_records = df[df['job_filing_number'].astype(str).str.endswith('I1')]
                        print(f"   📅 Sample: Job {i1_records['job_filing_number'].values[0]}, Date: {i1_records.get('filing_date', pd.Series([None])).values[0]}")
            else:
                print(f"   ❌ No New Building records found in DOB NOW")
        else:
            print(f"   ❌ API error: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Check by BBL
    print(f"\n🔍 Querying BISWEB by BBL {building['bbl']}...")
    bbl_int = int(building['bbl'])
    borough = str(bbl_int)[0]
    block = str(bbl_int)[1:6].lstrip('0')
    lot = str(bbl_int)[6:10].lstrip('0')
    print(f"   BBL decomposed: Borough={borough}, Block={block}, Lot={lot}")
    
    try:
        response = requests.get(
            BISWEB_URL,
            params={
                "$where": f"borough='{borough}' AND block='{block}' AND lot='{lot}' AND job_type='NB'",
                "$limit": 100
            },
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            if data:
                print(f"   ✅ Found {len(data)} NB records by BBL in BISWEB")
                df = pd.DataFrame(data)
                if 'doc__' in df.columns:
                    doc_01_count = (df['doc__'].astype(str).str.zfill(2) == '01').sum()
                    print(f"   📋 Records with doc__='01': {doc_01_count}")
            else:
                print(f"   ❌ No NB records found by BBL in BISWEB")
        else:
            print(f"   ❌ API error: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    print()

