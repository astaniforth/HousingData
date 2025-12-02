#!/usr/bin/env python3
"""
Debug what's in the combined DOB dataframe.
"""

import pandas as pd
from pathlib import Path

print("=" * 70)
print("DEBUGGING COMBINED DOB DATAFRAME")
print("=" * 70)

# Load the DOB data files
dob_bisweb = pd.read_csv("data/processed/multifamily_finance_dob_bisweb_bin.csv", low_memory=False)
print(f"\n1. BISWEB BIN data: {len(dob_bisweb)} records")
print(f"   doc__ column exists: {'doc__' in dob_bisweb.columns}")
if 'doc__' in dob_bisweb.columns:
    print(f"   doc__ dtype: {dob_bisweb['doc__'].dtype}")
    print(f"   doc__ unique values: {dob_bisweb['doc__'].unique()[:10]}")
    print(f"   doc__ == 1: {(dob_bisweb['doc__'] == 1).sum()} records")
    print(f"   doc__ == '1': {(dob_bisweb['doc__'] == '1').sum()} records")
    print(f"   doc__ == '01': {(dob_bisweb['doc__'] == '01').sum()} records")

dob_now_path = Path("data/processed/multifamily_finance_dob_now_bin.csv")
if dob_now_path.exists():
    dob_now = pd.read_csv(dob_now_path, low_memory=False)
    print(f"\n2. DOB NOW BIN data: {len(dob_now)} records")
    print(f"   doc__ column exists: {'doc__' in dob_now.columns}")
    print(f"   job_filing_number column exists: {'job_filing_number' in dob_now.columns}")
else:
    print(f"\n2. DOB NOW BIN data: NOT FOUND")
    dob_now = pd.DataFrame()

# Combine them like the notebook does
print("\n3. Simulating notebook combination:")
all_dfs = [dob_bisweb]
if not dob_now.empty:
    all_dfs.append(dob_now)

combined = pd.concat(all_dfs, ignore_index=True)
print(f"   Combined: {len(combined)} records")
print(f"   doc__ column exists: {'doc__' in combined.columns}")
if 'doc__' in combined.columns:
    print(f"   doc__ dtype: {combined['doc__'].dtype}")
    print(f"   doc__ value counts (top 5):")
    print(combined['doc__'].value_counts().head())
    
    # Test various filters
    print(f"\n4. Testing filters on combined data:")
    print(f"   doc__ == 1 (int): {(combined['doc__'] == 1).sum()} records")
    print(f"   doc__ == 1.0 (float): {(combined['doc__'] == 1.0).sum()} records")
    print(f"   doc__.fillna(-1) == 1: {(combined['doc__'].fillna(-1) == 1).sum()} records")
    
    # Check if there are NaN values
    print(f"\n5. NaN values in doc__: {combined['doc__'].isna().sum()}")

print("\n" + "=" * 70)

