#!/usr/bin/env python3
"""
Simulate the exact filtering logic from the notebook to see what happens to job 220412541
"""

import sys
sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")

from query_dob_filings import query_dob_bisweb_bin, query_dobnow_bin
import pandas as pd

bin_to_test = 2124684
expected_job = "220412541"

print("=" * 80)
print("SIMULATING EXACT NOTEBOOK FILTERING LOGIC")
print("=" * 80)

# Step 1: Query BISWEB
print("\nStep 1: Query BISWEB")
bisweb_df = query_dob_bisweb_bin([bin_to_test])
print(f"BISWEB results: {len(bisweb_df)} records")

# Step 2: Query DOB NOW
print("\nStep 2: Query DOB NOW")
dobnow_df = query_dobnow_bin([bin_to_test])
print(f"DOB NOW results: {len(dobnow_df)} records")

# Step 3: Combine (as the notebook does)
print("\nStep 3: Combine dataframes")
if not bisweb_df.empty and not dobnow_df.empty:
    all_cols = list(set(bisweb_df.columns.tolist() + dobnow_df.columns.tolist()))
    bisweb_aligned = bisweb_df.reindex(columns=all_cols)
    dobnow_aligned = dobnow_df.reindex(columns=all_cols)
    dob_df = pd.concat([bisweb_aligned, dobnow_aligned], ignore_index=True)
elif not bisweb_df.empty:
    dob_df = bisweb_df
elif not dobnow_df.empty:
    dob_df = dobnow_df
else:
    dob_df = pd.DataFrame()

print(f"Combined: {len(dob_df)} records")

# Check if our job is in the combined data
if 'job__' in dob_df.columns:
    job_match = dob_df[dob_df['job__'].astype(str) == expected_job]
    print(f"\nJob {expected_job} in combined data: {'YES ✅' if not job_match.empty else 'NO ❌'}")
    if not job_match.empty:
        print(f"  doc__: {job_match['doc__'].values}")
        if 'job_filing_number' in job_match.columns:
            print(f"  job_filing_number: {job_match['job_filing_number'].values}")

# Step 4: Filter doc__ = '01'
print("\nStep 4: Filter BISWEB to doc__='01'")
original_count = len(dob_df)

if 'doc__' in dob_df.columns:
    doc_01_mask = dob_df['doc__'].astype(str).str.zfill(2) == '01'
    doc_nan_mask = dob_df['doc__'].isna()
    dob_df = dob_df[doc_01_mask | doc_nan_mask]
    print(f"After doc__ filter: {len(dob_df)} records (from {original_count})")

# Check if our job survived
if 'job__' in dob_df.columns:
    job_match = dob_df[dob_df['job__'].astype(str) == expected_job]
    print(f"\nJob {expected_job} after doc__ filter: {'YES ✅' if not job_match.empty else 'NO ❌'}")

# Step 5: Filter DOB NOW to I1
print("\nStep 5: Filter DOB NOW to I1 suffix")
if 'job_filing_number' in dob_df.columns:
    print(f"job_filing_number column exists")
    print(f"  Total rows: {len(dob_df)}")
    print(f"  Rows with non-null job_filing_number: {dob_df['job_filing_number'].notna().sum()}")
    print(f"  Rows with null job_filing_number (BISWEB): {dob_df['job_filing_number'].isna().sum()}")
    
    dobnow_mask = dob_df['job_filing_number'].notna()
    if dobnow_mask.any():
        i1_mask = dob_df['job_filing_number'].astype(str).str.endswith('I1', na=False)
        
        print(f"\n  dobnow_mask (has job_filing_number): {dobnow_mask.sum()} rows")
        print(f"  i1_mask (ends with I1): {i1_mask.sum()} rows")
        print(f"  ~dobnow_mask (no job_filing_number, i.e. BISWEB): {(~dobnow_mask).sum()} rows")
        print(f"  Final keep mask (~dobnow_mask | i1_mask): {((~dobnow_mask) | i1_mask).sum()} rows")
        
        dob_df = dob_df[(~dobnow_mask) | i1_mask]
        print(f"\nAfter I1 filter: {len(dob_df)} records")

# Final check
print("\n" + "=" * 80)
print("FINAL RESULT")
print("=" * 80)
if 'job__' in dob_df.columns:
    job_match = dob_df[dob_df['job__'].astype(str) == expected_job]
    if not job_match.empty:
        print(f"✅ Job {expected_job} SURVIVED all filters!")
        display_cols = ['job__', 'doc__', 'job_type', 'pre__filing_date']
        if 'job_filing_number' in job_match.columns:
            display_cols.append('job_filing_number')
        existing_cols = [col for col in display_cols if col in job_match.columns]
        print(job_match[existing_cols].to_string(index=False))
    else:
        print(f"❌ Job {expected_job} was FILTERED OUT!")
        print(f"\nRemaining jobs:")
        for job in dob_df['job__'].unique()[:10]:
            print(f"  - {job}")

