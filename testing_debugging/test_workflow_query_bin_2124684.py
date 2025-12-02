#!/usr/bin/env python3
"""
Test the exact query our workflow uses for BIN 2124684
"""

import sys
sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")

from query_dob_filings import query_dob_bisweb_bin
import pandas as pd

bin_to_test = 2124684
expected_job = "220412541"

print("=" * 80)
print("TESTING DOB BISWEB BIN QUERY - EXACTLY AS WORKFLOW DOES IT")
print("=" * 80)
print(f"\nTesting BIN: {bin_to_test}")
print(f"Expected to find job: {expected_job}")
print()

# Query using our workflow function
result_df = query_dob_bisweb_bin([bin_to_test])

if result_df.empty:
    print("\n❌ ERROR: No results returned!")
else:
    print(f"\n✅ Got {len(result_df)} records")
    
    # Show what we got
    print(f"\nColumns: {result_df.columns.tolist()}")
    
    # Show key fields
    display_cols = ['job__', 'doc__', 'job_type', 'bin__', 'job_status']
    if 'pre__filing_date' in result_df.columns:
        display_cols.append('pre__filing_date')
    if 'paid' in result_df.columns:
        display_cols.append('paid')
    if 'fully_permitted' in result_df.columns:
        display_cols.append('fully_permitted')
    
    existing_cols = [col for col in display_cols if col in result_df.columns]
    
    print(f"\nAll records returned:")
    print(result_df[existing_cols].to_string(index=False))
    
    # Check for the expected job
    if 'job__' in result_df.columns:
        job_match = result_df[result_df['job__'].astype(str) == expected_job]
        if not job_match.empty:
            print(f"\n🎯 FOUND expected job {expected_job}!")
            print(job_match[existing_cols].to_string(index=False))
        else:
            print(f"\n❌ Expected job {expected_job} NOT found in results")
            print(f"\nJobs that WERE returned:")
            if 'job__' in result_df.columns:
                for job in result_df['job__'].unique():
                    print(f"  - {job}")
    
    # Check doc__ values
    if 'doc__' in result_df.columns:
        doc_values = result_df['doc__'].unique()
        print(f"\n📋 doc__ values in results: {doc_values}")
        
        # Show how many of each
        doc_counts = result_df['doc__'].value_counts()
        print(f"\nDoc__ distribution:")
        for doc, count in doc_counts.items():
            print(f"  doc__={doc}: {count} records")

