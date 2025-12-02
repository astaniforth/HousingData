#!/usr/bin/env python3
"""
Test script to understand why project 75925 is not being identified correctly.
"""

import pandas as pd
import sys

# Simulate the logic from the notebook
# We need to understand:
# 1. Does project 75925 appear multiple times in the full dataset?
# 2. Does at least one row have null BIN/BBL and missing DOB date?

print("=" * 70)
print("TESTING PROJECT 75925 ANALYSIS")
print("=" * 70)

# This would need to be run in the notebook context, but let's document the logic
print("""
The correct logic should be:

1. Get the FULL dataset: hpd_multifamily_finance_new_construction_with_dob_date_df
2. Find Project IDs that appear multiple times in the FULL dataset
3. Among those duplicated Project IDs, check if ANY row has:
   - BIN is null
   - BBL is null  
   - earliest_dob_date is NaT (missing)

The current logic only looks in no_dob_milestone_df, but we need to check
if the Project ID is duplicated in the FULL dataset, not just in the missing subset.
""")

print("\nExpected logic:")
print("""
# Step 1: Find all duplicated Project IDs in the FULL dataset
full_df = hpd_multifamily_finance_new_construction_with_dob_date_df
duplicated_project_ids_in_full = full_df['Project ID'][full_df['Project ID'].duplicated(keep=False)].unique()

# Step 2: Among rows missing DOB dates, find those with null BIN/BBL
no_dob_milestone_df = full_df[full_df['earliest_dob_date'].isna()]
null_bin_bbl_mask = no_dob_milestone_df['BIN'].isna() & no_dob_milestone_df['BBL'].isna()
null_bin_bbl_df = no_dob_milestone_df[null_bin_bbl_mask]

# Step 3: Find rows with null BIN/BBL where the Project ID appears multiple times in FULL dataset
null_bin_bbl_with_duplicated_projid = null_bin_bbl_df[
    null_bin_bbl_df['Project ID'].isin(duplicated_project_ids_in_full)
]

# This should catch project 75925 if:
# - It appears multiple times in the full dataset
# - At least one of those rows has null BIN/BBL and missing DOB date
""")

print("\n" + "=" * 70)
print("Run this in the notebook to test:")
print("=" * 70)
print("""
# Test for project 75925 specifically
test_project_id = 75925
full_df = hpd_multifamily_finance_new_construction_with_dob_date_df

# Check if it appears multiple times
project_rows = full_df[full_df['Project ID'] == test_project_id]
print(f"Project {test_project_id} appears {len(project_rows)} times in full dataset")
print(project_rows[['Project ID', 'BIN', 'BBL', 'earliest_dob_date']])

# Check which rows are missing DOB dates
missing_dob = project_rows[project_rows['earliest_dob_date'].isna()]
print(f"\\nRows missing DOB date: {len(missing_dob)}")
if len(missing_dob) > 0:
    print(missing_dob[['Project ID', 'BIN', 'BBL', 'earliest_dob_date']])

# Check which have null BIN/BBL
null_bin_bbl = missing_dob[missing_dob['BIN'].isna() & missing_dob['BBL'].isna()]
print(f"\\nRows with null BIN/BBL and missing DOB: {len(null_bin_bbl)}")
if len(null_bin_bbl) > 0:
    print(null_bin_bbl[['Project ID', 'BIN', 'BBL', 'earliest_dob_date']])
""")

