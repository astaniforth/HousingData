#!/usr/bin/env python3
"""
Check if BISWEB records have job_filing_number column and what values it has
"""

import sys
sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")

from query_dob_filings import query_dob_bisweb_bin
import pandas as pd

bin_to_test = 2124684

print("Querying BISWEB for BIN", bin_to_test)
result_df = query_dob_bisweb_bin([bin_to_test])

if not result_df.empty:
    print(f"\n✅ Got {len(result_df)} records")
    print(f"\nColumns: {sorted(result_df.columns.tolist())}")
    
    # Check if job_filing_number exists
    if 'job_filing_number' in result_df.columns:
        print(f"\n⚠️  job_filing_number column EXISTS in BISWEB data!")
        print(f"Values: {result_df['job_filing_number'].unique()}")
        print(f"Non-null count: {result_df['job_filing_number'].notna().sum()}")
    else:
        print(f"\n✅ job_filing_number column does NOT exist in BISWEB data (as expected)")
    
    # Show the doc__='01' records
    doc_01 = result_df[result_df['doc__'].astype(str).str.zfill(2) == '01']
    print(f"\n📋 Records with doc__='01': {len(doc_01)}")
    if not doc_01.empty:
        display_cols = ['job__', 'doc__', 'job_type', 'pre__filing_date']
        print(doc_01[display_cols].to_string(index=False))

