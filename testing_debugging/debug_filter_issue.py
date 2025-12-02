#!/usr/bin/env python3
"""
Debug why the doc__ filter isn't working even after the fix.
"""

import pandas as pd
from pathlib import Path

print("=" * 70)
print("DEBUGGING FILTER ISSUE")
print("=" * 70)

# Load the processed DOB data (same as notebook)
dob_path = Path("data/processed/multifamily_finance_dob_bisweb_bin.csv")
df = pd.read_csv(dob_path, low_memory=False)

print(f"\n1. Loaded {len(df)} records from {dob_path}")
print(f"   Columns: {list(df.columns)[:15]}...")

# Check if 'source' column exists
print(f"\n2. 'source' column exists: {'source' in df.columns}")

# Check doc__ column
print(f"\n3. doc__ column dtype: {df['doc__'].dtype}")
print(f"   doc__ unique values: {df['doc__'].unique()}")

# Test the filter
print(f"\n4. Testing doc__ filters:")
print(f"   doc__ == '01': {(df['doc__'] == '01').sum()} records")
print(f"   doc__ == 1: {(df['doc__'] == 1).sum()} records")
print(f"   doc__.astype(str) == '01': {(df['doc__'].astype(str) == '01').sum()} records")
print(f"   doc__.astype(str) == '1': {(df['doc__'].astype(str) == '1').sum()} records")
print(f"   doc__.astype(str).str.zfill(2) == '01': {(df['doc__'].astype(str).str.zfill(2) == '01').sum()} records")

# Check BIN 2129098
bin_df = df[df['bin__'].astype(str).str.replace('.0', '') == '2129098']
print(f"\n5. BIN 2129098 records: {len(bin_df)}")
print(f"   doc__ values: {bin_df['doc__'].tolist()}")

# Filter to doc__ = 1 (integer)
bin_df_filtered = bin_df[bin_df['doc__'] == 1]
print(f"\n6. BIN 2129098 with doc__ == 1: {len(bin_df_filtered)} records")
if len(bin_df_filtered) > 0:
    print(f"   job__ values: {bin_df_filtered['job__'].tolist()}")
    print(f"   pre__filing_date values: {bin_df_filtered['pre__filing_date'].tolist()}")

# Check what the notebook's combined_dob_with_normalized_bbl_df would look like
# It combines multiple DOB dataframes
print("\n7. Checking all DOB data files:")
dob_files = [
    "data/processed/multifamily_finance_dob_bisweb_bin.csv",
    "data/processed/multifamily_finance_dob_now_bin.csv",
    "data/processed/multifamily_finance_dob_bisweb_bbl.csv",
    "data/processed/multifamily_finance_dob_now_bbl.csv",
]

for f in dob_files:
    if Path(f).exists():
        temp_df = pd.read_csv(f, low_memory=False)
        print(f"\n   {f}:")
        print(f"     Records: {len(temp_df)}")
        print(f"     'source' column: {'source' in temp_df.columns}")
        if 'doc__' in temp_df.columns:
            print(f"     doc__ dtype: {temp_df['doc__'].dtype}")
        
        # Check for BIN 2129098
        if 'bin__' in temp_df.columns:
            bin_check = temp_df[temp_df['bin__'].astype(str).str.replace('.0', '') == '2129098']
        elif 'bin_normalized' in temp_df.columns:
            bin_check = temp_df[temp_df['bin_normalized'].astype(str) == '2129098']
        elif 'bin' in temp_df.columns:
            bin_check = temp_df[temp_df['bin'].astype(str).str.replace('.0', '') == '2129098']
        else:
            bin_check = pd.DataFrame()
        
        if len(bin_check) > 0:
            print(f"     BIN 2129098 records: {len(bin_check)}")
    else:
        print(f"\n   {f}: NOT FOUND")

print("\n" + "=" * 70)
print("CONCLUSION:")
print("=" * 70)
print("The issue is that the 'source' column doesn't exist in the CSV files,")
print("so the filter condition 'if source in dob_df.columns' fails and")
print("the doc__ filter is never applied!")
print("\nThe fix should either:")
print("1. Add a source column when loading the data")
print("2. Or apply the doc__ filter unconditionally for BISWEB data")

