#!/usr/bin/env python3
"""
Test script to verify if DOB APIs support batched OR queries for BBL components.
"""

import requests
import time

# NYC Open Data API endpoints
DOB_BISWEB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
DOB_NOW_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"

def test_batched_bbl_query():
    """
    Test if we can batch multiple BBL queries using OR logic.
    """

    # Test with real BBLs from our processed data
    test_bbls = [
        ('BROOKLYN', '04586', '00202'),  # From the data: BBL 30458600202
        ('BROOKLYN', '05229', '00017'),  # From the data: BBL 30522900017
        ('MANHATTAN', '02038', '00055'), # From the data: BBL 10203800055
    ]

    print("Testing batched BBL query concept...")
    print(f"Test BBLs: {test_bbls}")

    # Construct batched OR query
    conditions = []
    for borough, block, lot in test_bbls:
        condition = f"(job_type='NB' AND borough='{borough}' AND block='{block}' AND lot='{lot}')"
        conditions.append(condition)

    batched_query = " OR ".join(conditions)
    print(f"\nBatched query: {batched_query}")

    params = {
        '$where': batched_query,
        '$limit': 100
    }

    try:
        print("\nTesting DOB BISWEB API...")
        response = requests.get(DOB_BISWEB_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        print(f"BISWEB: Found {len(data)} records")
        if data:
            print("SUCCESS: Batched OR query works for BISWEB!")
            print("Sample record keys:", list(data[0].keys()) if data else "No data")
        else:
            print("No data found - but query syntax worked")

    except Exception as e:
        print(f"BISWEB: Error - {str(e)}")
        if hasattr(e, 'response') and e.response:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text[:200]}")

    # Test DOB NOW with different column requirements
    print("\nTesting DOB NOW API...")
    # DOB NOW uses unpadded block/lot
    test_bbls_dobnow = [
        ('BROOKLYN', '4586', '202'),   # Unpadded: block=4586, lot=202
        ('BROOKLYN', '5229', '17'),    # Unpadded: block=5229, lot=17
        ('MANHATTAN', '2038', '55'),   # Unpadded: block=2038, lot=55
    ]

    conditions_dobnow = []
    for borough, block, lot in test_bbls_dobnow:
        condition = f"(job_type='New Building' AND borough='{borough}' AND block='{block}' AND lot='{lot}')"
        conditions_dobnow.append(condition)

    batched_query_dobnow = " OR ".join(conditions_dobnow)
    print(f"DOB NOW batched query: {batched_query_dobnow}")

    params_dobnow = {
        '$where': batched_query_dobnow,
        '$limit': 100
    }

    try:
        response = requests.get(DOB_NOW_URL, params=params_dobnow, timeout=30)
        response.raise_for_status()
        data = response.json()

        print(f"DOB NOW: Found {len(data)} records")
        if data:
            print("SUCCESS: Batched OR query works for DOB NOW!")
            print("Sample record keys:", list(data[0].keys()) if data else "No data")
        else:
            print("No data found - but query syntax worked")

    except Exception as e:
        print(f"DOB NOW: Error - {str(e)}")
        if hasattr(e, 'response') and e.response:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text[:200]}")

    # Test with larger batch to see if there are limits
    print("\nTesting larger batch (10 BBLs)...")

    # Create 10 test BBLs
    large_test_bbls = []
    for i in range(10):
        # Use some variation
        borough = ['MANHATTAN', 'BRONX', 'BROOKLYN'][i % 3]
        block = f"{1000 + i:04d}"
        lot = f"{100 + i:03d}"
        large_test_bbls.append((borough, block, lot))

    conditions_large = []
    for borough, block, lot in large_test_bbls:
        condition = f"(job_type='NB' AND borough='{borough}' AND block='{block}' AND lot='{lot}')"
        conditions_large.append(condition)

    batched_query_large = " OR ".join(conditions_large)
    print(f"Large query length: {len(batched_query_large)} characters")

    params_large = {
        '$where': batched_query_large,
        '$limit': 100
    }

    try:
        response = requests.get(DOB_BISWEB_URL, params=params_large, timeout=30)
        response.raise_for_status()
        data = response.json()

        print(f"Large batch BISWEB: Found {len(data)} records")
        print("SUCCESS: Large batched OR query works!")

    except Exception as e:
        print(f"Large batch BISWEB: Error - {str(e)}")
        if hasattr(e, 'response') and e.response:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text[:200]}")

if __name__ == "__main__":
    test_batched_bbl_query()
