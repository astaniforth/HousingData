#!/usr/bin/env python3
"""
Analyze Building ID 927748 to understand why earliest_dob_date is wrong.
"""

import pandas as pd
import sys
from pathlib import Path

# Add current directory to path
sys.path.append(".")

print("=" * 70)
print("ANALYZING BUILDING ID 927748")
print("=" * 70)

# Load the processed DOB data
dob_bisweb_path = Path("data/processed/multifamily_finance_dob_bisweb_bin.csv")
dob_now_path = Path("data/processed/multifamily_finance_dob_now_bin.csv")

print("\n1. Loading DOB data files...")
if dob_bisweb_path.exists():
    dob_bisweb = pd.read_csv(dob_bisweb_path)
    print(f"   BISWEB records: {len(dob_bisweb)}")
    if 'bin_normalized' in dob_bisweb.columns:
        bisweb_927748 = dob_bisweb[dob_bisweb['bin_normalized'] == '2129098']
        print(f"   BISWEB records for BIN 2129098: {len(bisweb_927748)}")
        if len(bisweb_927748) > 0:
            print("\n   BISWEB records for BIN 2129098:")
            date_cols = [c for c in bisweb_927748.columns if 'date' in c.lower() or c in ['pre__filing_date', 'paid', 'approved', 'assigned', 'fully_paid', 'fully_permitted']]
            print(f"   Date columns found: {date_cols}")
            for idx, row in bisweb_927748.iterrows():
                print(f"\n   Row {idx}:")
                print(f"     job__: {row.get('job__', 'N/A')}")
                print(f"     doc__: {row.get('doc__', 'N/A')}")
                for col in date_cols:
                    if col in row and pd.notna(row[col]):
                        print(f"     {col}: {row[col]}")
else:
    print("   BISWEB file not found")

if dob_now_path.exists():
    dob_now = pd.read_csv(dob_now_path)
    print(f"\n   DOB NOW records: {len(dob_now)}")
    if 'bin_normalized' in dob_now.columns:
        now_927748 = dob_now[dob_now['bin_normalized'] == '2129098']
        print(f"   DOB NOW records for BIN 2129098: {len(now_927748)}")
        if len(now_927748) > 0:
            print("\n   DOB NOW records for BIN 2129098:")
            date_cols = [c for c in now_927748.columns if 'date' in c.lower() or c in ['filing_date', 'first_permit_date', 'approved_date', 'pre__filing_date']]
            print(f"   Date columns found: {date_cols}")
            for idx, row in now_927748.iterrows():
                print(f"\n   Row {idx}:")
                print(f"     job_filing_number: {row.get('job_filing_number', 'N/A')}")
                for col in date_cols:
                    if col in row and pd.notna(row[col]):
                        print(f"     {col}: {row[col]}")
else:
    print("   DOB NOW file not found")

# Check the raw DOB data files
print("\n2. Checking raw DOB data files...")
raw_dob_paths = [
    Path("data/raw/new_construction_bins_dob_filings.csv"),
    Path("data/raw/all_construction_bins_co_filings.csv")
]

for path in raw_dob_paths:
    if path.exists():
        print(f"\n   Checking {path.name}...")
        df = pd.read_csv(path, low_memory=False)
        if 'bin__' in df.columns or 'bin' in df.columns:
            bin_col = 'bin__' if 'bin__' in df.columns else 'bin'
            df_bin = df[df[bin_col].astype(str).str.replace('.0', '') == '2129098']
            if len(df_bin) > 0:
                print(f"     Found {len(df_bin)} records for BIN 2129098")
                # Check for job 220124381
                if 'job__' in df_bin.columns:
                    job_df = df_bin[df_bin['job__'].astype(str) == '220124381']
                    if len(job_df) > 0:
                        print(f"     Found job 220124381!")
                        for idx, row in job_df.iterrows():
                            print(f"\n     Row {idx}:")
                            if 'pre__filing_date' in row:
                                print(f"       pre__filing_date: {row['pre__filing_date']}")
                            if 'paid' in row:
                                print(f"       paid: {row['paid']}")
                            if 'approved' in row:
                                print(f"       approved: {row['approved']}")
                            if 'doc__' in row:
                                print(f"       doc__: {row['doc__']}")

print("\n" + "=" * 70)
print("ANALYSIS COMPLETE")
print("=" * 70)

