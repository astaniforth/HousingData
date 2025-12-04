#!/usr/bin/env python3
"""
Test script to verify condo BBL lookup logic for building 995045.

The building at 45 Commercial Street (Greenpoint Landing H1H2) has:
- HPD BBL: 3024727504 (this is a billing/condo BBL with lot 7504)
- Expected base BBL: 3024720070 (lot 70)

The condo fallback should:
1. Search 3024727504 in condo_billing_bbl → find base BBL 3024720070
2. Search 3024720070 in condo_base_bbl → find ALL related billing BBLs
3. Query DOB with all those BBLs to find NB filings
"""

import requests
import pandas as pd

CONDO_BILLING_URL = "https://data.cityofnewyork.us/resource/p8u6-a6it.json"
DOB_BISWEB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"

def test_building_995045():
    """Test the condo fallback logic for building 995045"""
    
    hpd_bbl = "3024727504"  # The BBL from HPD data
    print(f"=" * 70)
    print(f"Testing Condo BBL Lookup for Building 995045")
    print(f"HPD BBL: {hpd_bbl}")
    print(f"=" * 70)
    
    # Step 1: Search HPD BBL in condo_billing_bbl
    print(f"\n📍 Step 1: Search {hpd_bbl} in condo_billing_bbl")
    params = {
        '$where': f"condo_billing_bbl='{hpd_bbl}'",
        '$limit': 100
    }
    response = requests.get(CONDO_BILLING_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    if data:
        print(f"✅ Found {len(data)} record(s)")
        for record in data[:3]:
            print(f"   Base BBL: {record.get('condo_base_bbl')}")
            print(f"   Condo Name: {record.get('condo_name')}")
        base_bbl = data[0].get('condo_base_bbl')
    else:
        print(f"❌ No records found in condo_billing_bbl")
        
        # Step 1b: Also try searching in condo_base_bbl
        print(f"\n📍 Step 1b: Search {hpd_bbl} in condo_base_bbl")
        params = {
            '$where': f"condo_base_bbl='{hpd_bbl}'",
            '$limit': 100
        }
        response = requests.get(CONDO_BILLING_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data:
            print(f"✅ Found {len(data)} record(s) - this IS a base BBL")
            base_bbl = hpd_bbl
        else:
            print(f"❌ Not found in either column - not a condo")
            return
    
    # Step 2: Search base_bbl in condo_base_bbl to get ALL billing BBLs
    print(f"\n📍 Step 2: Search base BBL {base_bbl} to get ALL related billing BBLs")
    params = {
        '$where': f"condo_base_bbl='{base_bbl}'",
        '$limit': 1000  # Get all related billing BBLs
    }
    response = requests.get(CONDO_BILLING_URL, params=params, timeout=30)
    response.raise_for_status()
    all_records = response.json()
    
    if all_records:
        print(f"✅ Found {len(all_records)} billing BBLs for base {base_bbl}")
        billing_bbls = set()
        billing_bbls.add(base_bbl)  # Include the base BBL itself
        for record in all_records:
            billing_bbl = record.get('condo_billing_bbl')
            if billing_bbl:
                billing_bbls.add(billing_bbl)
        
        print(f"   Total unique BBLs to query: {len(billing_bbls)}")
        print(f"   Sample BBLs: {list(billing_bbls)[:5]}")
    else:
        print(f"❌ No billing BBLs found for base {base_bbl}")
        return
    
    # Step 3: Query DOB BISWEB for NB filings on these BBLs
    print(f"\n📍 Step 3: Query DOB BISWEB for NB filings on all {len(billing_bbls)} BBLs")
    
    # Build query for all BBLs
    all_nb_filings = []
    
    for bbl in billing_bbls:
        # Decompose BBL to borough/block/lot
        bbl_str = str(bbl).zfill(10)
        borough_code = bbl_str[0]
        # BISWEB requires PADDED block (5 digits) and lot (5 digits)
        block = bbl_str[1:6]  # Keep leading zeros for BISWEB
        lot = bbl_str[6:].zfill(5)  # Pad lot to 5 digits
        
        borough_map = {'1': 'MANHATTAN', '2': 'BRONX', '3': 'BROOKLYN', '4': 'QUEENS', '5': 'STATEN ISLAND'}
        borough = borough_map.get(borough_code, 'UNKNOWN')
        
        # Query BISWEB
        query = f"job_type='NB' AND borough='{borough}' AND block='{block}' AND lot='{lot}'"
        params = {
            '$where': query,
            '$limit': 100
        }
        
        try:
            response = requests.get(DOB_BISWEB_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data:
                print(f"   ✅ BBL {bbl} ({borough}/{block}/{lot}): Found {len(data)} NB filings")
                for record in data[:2]:
                    print(f"      Job: {record.get('job__')}, Pre-filing: {record.get('pre__filing_date')}")
                all_nb_filings.extend(data)
        except Exception as e:
            print(f"   ❌ BBL {bbl}: Error - {str(e)[:50]}")
    
    print(f"\n" + "=" * 70)
    print(f"SUMMARY")
    print(f"=" * 70)
    print(f"HPD BBL: {hpd_bbl}")
    print(f"Found base BBL: {base_bbl}")
    print(f"Total related BBLs: {len(billing_bbls)}")
    print(f"Total NB filings found: {len(all_nb_filings)}")
    
    if all_nb_filings:
        print(f"\n✅ SUCCESS! Condo fallback would find {len(all_nb_filings)} NB filings")
        print(f"\nFirst NB filing details:")
        first = all_nb_filings[0]
        print(f"   Job: {first.get('job__')}")
        print(f"   Pre-filing date: {first.get('pre__filing_date')}")
        print(f"   Paid date: {first.get('paid')}")
        print(f"   Fully permitted: {first.get('fully_permitted')}")
    else:
        print(f"\n❌ No NB filings found even with condo fallback")
    
    return all_nb_filings


if __name__ == "__main__":
    test_building_995045()

