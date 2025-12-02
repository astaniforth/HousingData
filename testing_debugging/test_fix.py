#!/usr/bin/env python3
"""
Test the fix for doc__ filtering.
"""

import pandas as pd
from pathlib import Path

print("=" * 70)
print("TESTING THE FIX")
print("=" * 70)

# Load the processed DOB data (simulating what the notebook does)
dob_path = Path("data/processed/multifamily_finance_dob_bisweb_bin.csv")
dob_df = pd.read_csv(dob_path, low_memory=False)

print(f"\n1. Loaded {len(dob_df)} records")

# Apply the new filter
if 'doc__' in dob_df.columns:
    dob_df = dob_df[dob_df['doc__'] == 1]
    print(f"2. Filtered to doc__ = 1: {len(dob_df)} records remaining")

# Check BIN 2129098
if 'bin__' in dob_df.columns:
    bin_df = dob_df[dob_df['bin__'].astype(str).str.replace('.0', '') == '2129098']
elif 'bin_normalized' in dob_df.columns:
    bin_df = dob_df[dob_df['bin_normalized'].astype(str) == '2129098']
else:
    bin_df = pd.DataFrame()

print(f"\n3. BIN 2129098 records after filter: {len(bin_df)}")

if len(bin_df) > 0:
    print(f"   job__ values: {bin_df['job__'].tolist()}")
    print(f"   doc__ values: {bin_df['doc__'].tolist()}")
    print(f"   pre__filing_date values: {bin_df['pre__filing_date'].tolist()}")
    
    # Convert dates and find earliest
    bin_df = bin_df.copy()
    bin_df['pre__filing_date'] = pd.to_datetime(bin_df['pre__filing_date'], errors='coerce')
    
    # Find the row with job 220124381
    job_df = bin_df[bin_df['job__'].astype(str) == '220124381']
    if len(job_df) > 0:
        print(f"\n4. Job 220124381 records:")
        print(f"   pre__filing_date: {job_df['pre__filing_date'].tolist()}")
        
        # This should be 2011-06-14
        expected_date = pd.Timestamp('2011-06-14')
        actual_date = job_df['pre__filing_date'].iloc[0]
        print(f"\n5. Expected date: {expected_date}")
        print(f"   Actual date: {actual_date}")
        print(f"   Match: {actual_date == expected_date}")

print("\n" + "=" * 70)

