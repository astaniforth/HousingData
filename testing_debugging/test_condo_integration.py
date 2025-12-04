#!/usr/bin/env python3
"""
Integration test for the condo fallback flow.
Tests the actual functions that will be used in the notebook.
"""

import sys
sys.path.insert(0, '/Users/andrewstaniforth/Documents/Programming/HousingData')

import pandas as pd
from query_dob_filings import (
    get_all_condo_related_bbls,
    query_dob_for_condo_bbls,
    query_dob_bisweb_bbl,
    query_dobnow_bbl,
    decompose_bbl
)

def test_condo_integration():
    """Test the full condo fallback integration for building 995045"""
    
    print("=" * 70)
    print("CONDO FALLBACK INTEGRATION TEST")
    print("Building 995045 - 45 Commercial Street (Greenpoint Landing H1H2)")
    print("HPD BBL: 3024727504")
    print("=" * 70)
    
    # Simulate the HPD data for this building
    hpd_bbl = "3024727504"
    
    # Step 1: Test get_all_condo_related_bbls
    print("\n📍 Step 1: Testing get_all_condo_related_bbls()")
    related_bbls = get_all_condo_related_bbls(hpd_bbl)
    
    if related_bbls:
        print(f"✅ Found {len(related_bbls)} related BBLs:")
        for bbl in sorted(related_bbls):
            print(f"   - {bbl}")
    else:
        print(f"❌ No related BBLs found")
        return False
    
    # Step 2: Test query_dob_for_condo_bbls with a list of BBLs
    print("\n📍 Step 2: Testing query_dob_for_condo_bbls()")
    # Simulate a list of BBLs as it would come from the notebook
    test_bbls = [hpd_bbl]  # Just our test BBL
    
    dob_condo_df = query_dob_for_condo_bbls(test_bbls)
    
    if not dob_condo_df.empty:
        print(f"\n✅ SUCCESS! Found {len(dob_condo_df)} DOB records via condo fallback")
        print(f"\nSample columns: {list(dob_condo_df.columns)[:10]}")
        
        # Show key details
        if 'job__' in dob_condo_df.columns:
            print(f"\nJob numbers found: {dob_condo_df['job__'].unique().tolist()}")
        if 'pre__filing_date' in dob_condo_df.columns:
            print(f"Pre-filing dates: {dob_condo_df['pre__filing_date'].unique().tolist()}")
        if 'source' in dob_condo_df.columns:
            print(f"Source: {dob_condo_df['source'].unique().tolist()}")
        if 'bin__' in dob_condo_df.columns:
            print(f"BINs found: {dob_condo_df['bin__'].unique().tolist()}")
        
        return True
    else:
        print(f"❌ No DOB records found")
        return False


def test_multiple_condos():
    """Test with multiple BBLs to simulate notebook scenario"""
    
    print("\n" + "=" * 70)
    print("TESTING MULTIPLE BBL SCENARIO")
    print("=" * 70)
    
    # Simulate multiple BBLs from unmatched projects
    # Some are condos, some are not
    test_bbls = [
        "3024727504",  # Building 995045 - is a condo
        "2029847503",  # Random BBL - may or may not be condo
        "1000010001",  # Random Manhattan BBL
    ]
    
    print(f"Testing with {len(test_bbls)} BBLs...")
    
    dob_condo_df = query_dob_for_condo_bbls(test_bbls)
    
    if not dob_condo_df.empty:
        print(f"\n✅ Found {len(dob_condo_df)} total records")
    else:
        print(f"\n⚠️ No records found (some BBLs may not be condos)")
    
    return True


if __name__ == "__main__":
    success1 = test_condo_integration()
    success2 = test_multiple_condos()
    
    print("\n" + "=" * 70)
    print("FINAL RESULT")
    print("=" * 70)
    
    if success1:
        print("✅ Condo fallback integration test PASSED")
        print("   Building 995045 will now find NB filings via condo lookup")
    else:
        print("❌ Condo fallback integration test FAILED")

