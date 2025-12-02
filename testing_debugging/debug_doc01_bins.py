"""
Debug script to investigate why doc__ = 1 / I1 records don't match HPD BINs.

This script:
1. Loads the combined DOB data from the notebook's Step 3B output
2. Checks what BINs have doc__ = 1 records
3. Compares with HPD BINs to find the overlap
"""

import pandas as pd
import sys
sys.path.append(".")

print("=" * 70)
print("DEBUG: Investigating doc__ = 1 / I1 filter issue")
print("=" * 70)

# Load HPD data
hpd_path = "data/raw/Affordable_Housing_Production_by_Building.csv"
hpd_df = pd.read_csv(hpd_path)
hpd_df = hpd_df[hpd_df["Reporting Construction Type"] == "New Construction"]
hpd_df = hpd_df[hpd_df["Program Group"] == "Multifamily Finance Program"]

hpd_bins = set(hpd_df['BIN'].dropna().astype(str).str.replace('.0', ''))
print(f"\nHPD Multifamily Finance New Construction: {len(hpd_df)} rows, {len(hpd_bins)} unique BINs")
print(f"Sample HPD BINs: {sorted(list(hpd_bins))[:10]}")

# Check if DOB data files exist
import os
dob_bisweb_path = "data/processed/multifamily_finance_dob_bisweb_bin.csv"
dob_now_path = "data/processed/multifamily_finance_dob_now_bin.csv"

if os.path.exists(dob_bisweb_path):
    dob_bisweb = pd.read_csv(dob_bisweb_path, low_memory=False)
    print(f"\nDOB BISWEB data loaded: {len(dob_bisweb)} records")
    
    if 'bin__' in dob_bisweb.columns:
        dob_bisweb['bin_normalized'] = dob_bisweb['bin__'].astype(str).str.replace('.0', '')
    
    # Check doc__ values
    if 'doc__' in dob_bisweb.columns:
        print(f"\ndoc__ value distribution:")
        print(dob_bisweb['doc__'].value_counts())
        
        # Filter to doc__ = 1
        doc01_df = dob_bisweb[dob_bisweb['doc__'] == 1]
        print(f"\nRecords with doc__ = 1: {len(doc01_df)}")
        
        doc01_bins = set(doc01_df['bin_normalized'].dropna())
        print(f"Unique BINs with doc__ = 1: {len(doc01_bins)}")
        print(f"Sample doc 01 BINs: {sorted(list(doc01_bins))[:10]}")
        
        # Check overlap with HPD
        overlap = doc01_bins.intersection(hpd_bins)
        print(f"\n🔍 BIN OVERLAP (doc 01 vs HPD): {len(overlap)} BINs")
        if len(overlap) > 0:
            print(f"   Sample overlapping: {sorted(list(overlap))[:10]}")
        else:
            print("   ❌ NO OVERLAP - doc 01 BINs don't match HPD BINs!")
            
            # Check if ANY BISWEB BINs overlap with HPD
            all_bisweb_bins = set(dob_bisweb['bin_normalized'].dropna())
            all_overlap = all_bisweb_bins.intersection(hpd_bins)
            print(f"\n   All BISWEB BINs: {len(all_bisweb_bins)}")
            print(f"   All BISWEB overlap with HPD: {len(all_overlap)}")
            
            if len(all_overlap) > 0:
                # Find which doc types match HPD
                matching_df = dob_bisweb[dob_bisweb['bin_normalized'].isin(hpd_bins)]
                print(f"\n   BISWEB records matching HPD BINs: {len(matching_df)}")
                print(f"   doc__ values in matching records:")
                print(matching_df['doc__'].value_counts())
    else:
        print("❌ No doc__ column in BISWEB data")
else:
    print(f"\n❌ BISWEB file not found: {dob_bisweb_path}")
    print("   Need to run Step 3A/3B in the notebook first")

if os.path.exists(dob_now_path):
    dob_now = pd.read_csv(dob_now_path, low_memory=False)
    print(f"\n\nDOB NOW data loaded: {len(dob_now)} records")
    
    if 'bin' in dob_now.columns:
        dob_now['bin_normalized'] = dob_now['bin'].astype(str).str.replace('.0', '')
    
    # Check job_filing_number values
    if 'job_filing_number' in dob_now.columns:
        # Check I1 suffix
        i1_df = dob_now[dob_now['job_filing_number'].astype(str).str.endswith('I1')]
        print(f"\nRecords with I1 suffix: {len(i1_df)}")
        
        i1_bins = set(i1_df['bin_normalized'].dropna())
        print(f"Unique BINs with I1 suffix: {len(i1_bins)}")
        
        # Check overlap with HPD
        overlap = i1_bins.intersection(hpd_bins)
        print(f"\n🔍 BIN OVERLAP (I1 vs HPD): {len(overlap)} BINs")
        if len(overlap) > 0:
            print(f"   Sample overlapping: {sorted(list(overlap))[:10]}")
else:
    print(f"\n❌ DOB NOW file not found: {dob_now_path}")

print("\n" + "=" * 70)
print("CONCLUSION")
print("=" * 70)

