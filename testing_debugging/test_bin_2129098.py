#!/usr/bin/env python3
"""
Test script to debug the date extraction issue for BIN 2129098.
Simulates the exact logic from the notebook to see what's happening.
"""

import pandas as pd
import numpy as np

print("=" * 70)
print("TESTING DATE EXTRACTION FOR BIN 2129098")
print("=" * 70)

# Simulate the DOB data for BIN 2129098
# Based on user's info: job 220124381, doc__ = '01', pre__filing_date should be 2011-06-14
# But we're getting 2013-12-31

# Create test data that simulates what might be in the actual data
test_data = {
    'bin_normalized': ['2129098', '2129098'],  # Two applications for same BIN
    'job__': ['220124381', '220124382'],  # Two different jobs
    'doc__': ['01', '01'],
    'pre__filing_date': [pd.Timestamp('2011-06-14'), pd.NaT],  # First has pre-filing, second doesn't
    'paid': [pd.Timestamp('2013-12-31'), pd.Timestamp('2014-01-15')],  # Both have paid dates
    'approved': [pd.Timestamp('2014-02-01'), pd.Timestamp('2014-03-01')],
    'source': ['DOB_Job_Applications', 'DOB_Job_Applications']
}

dob_df = pd.DataFrame(test_data)

print("\n1. Simulated DOB data for BIN 2129098:")
print(dob_df[['bin_normalized', 'job__', 'doc__', 'pre__filing_date', 'paid', 'approved']].to_string())

# Filter to doc__ = '01' (as per requirements)
dob_df_filtered = dob_df[dob_df['doc__'] == '01'].copy()
print(f"\n2. After filtering doc__ = '01': {len(dob_df_filtered)} rows")

# Define date columns to check
date_cols = ['pre__filing_date', 'paid', 'approved', 'assigned', 'fully_paid', 'fully_permitted']

# Function to get earliest date (from notebook logic)
def _get_earliest_date(row, date_cols):
    """Get the earliest date and the column name that provided it."""
    date_values = []
    for col in date_cols:
        v = row.get(col, None)
        if pd.notna(v):
            try:
                date_val = pd.to_datetime(v, errors='coerce')
                if pd.notna(date_val):
                    date_values.append((date_val, col))
            except:
                pass
    if date_values:
        earliest = min(date_values, key=lambda x: x[0])
        return earliest[0], earliest[1]
    return pd.NaT, None

# Function to get application date (for selecting most recent application)
def get_application_date(row):
    """Get the application date for sorting (most recent first)."""
    # Try various date columns to determine application recency
    date_cols = ['pre__filing_date', 'paid', 'approved', 'assigned']
    for col in date_cols:
        if col in row and pd.notna(row[col]):
            return pd.to_datetime(row[col], errors='coerce')
    return pd.NaT

# Add application_date column
dob_df_filtered['application_date'] = dob_df_filtered.apply(get_application_date, axis=1)
print("\n3. Application dates for sorting:")
print(dob_df_filtered[['job__', 'application_date', 'pre__filing_date', 'paid']].to_string())

# Sort by application_date descending (most recent first)
dob_df_sorted = dob_df_filtered.sort_values('application_date', ascending=False, na_position='last')
print("\n4. After sorting by application_date (descending):")
print(dob_df_sorted[['job__', 'application_date', 'pre__filing_date', 'paid']].to_string())

# Get earliest date for each row
print("\n5. Getting earliest date for each row:")
earliest_results = dob_df_sorted.apply(lambda row: _get_earliest_date(row, date_cols), axis=1)
for idx, result in earliest_results.items():
    row = dob_df_sorted.loc[idx]
    print(f"   Row {idx} (job {row['job__']}): earliest = {result[0]}, source = {result[1]}")

# Group by BIN and take first (most recent application)
dob_bin_min_temp = dob_df_sorted.groupby('bin_normalized', as_index=False).first()
print("\n6. After groupby().first() (most recent application):")
print(dob_bin_min_temp[['bin_normalized', 'job__', 'pre__filing_date', 'paid', 'approved']].to_string())

# Extract earliest date and source
earliest_dates = []
earliest_sources = []
for orig_idx in earliest_results.index:
    result = earliest_results.loc[orig_idx]
    if isinstance(result, tuple) and len(result) >= 2:
        source_col = result[1]
        row = dob_df_sorted.loc[orig_idx]
        if source_col and source_col in row and pd.notna(row[source_col]):
            source_date = pd.to_datetime(row[source_col], errors='coerce')
            earliest_dates.append(source_date)
        else:
            earliest_dates.append(result[0])
        earliest_sources.append(source_col)
    else:
        earliest_dates.append(result[0] if not isinstance(result, tuple) else pd.NaT)
        earliest_sources.append(None)

dob_df_sorted['earliest_dob_date'] = pd.Series(earliest_dates, index=earliest_results.index)
dob_df_sorted['earliest_dob_date_source'] = pd.Series(earliest_sources, index=earliest_results.index)

# Now group by BIN and get the first row (most recent application)
dob_bin_min_temp = dob_df_sorted.groupby('bin_normalized', as_index=False).first()
print("\n7. After groupby().first() with earliest dates:")
print(dob_bin_min_temp[['bin_normalized', 'job__', 'earliest_dob_date', 'earliest_dob_date_source', 'pre__filing_date', 'paid']].to_string())

# Re-read date from source column (current fix)
print("\n8. Re-reading date from source column (current fix):")
for idx in dob_bin_min_temp.index:
    source_col = dob_bin_min_temp.loc[idx, 'earliest_dob_date_source']
    if source_col and pd.notna(source_col):
        orig_row = dob_bin_min_temp.loc[idx]
        if source_col in orig_row and pd.notna(orig_row[source_col]):
            new_date = pd.to_datetime(orig_row[source_col], errors='coerce')
            print(f"   Row {idx}: source_col = {source_col}, value in row = {orig_row[source_col]}, new_date = {new_date}")
            dob_bin_min_temp.loc[idx, 'earliest_dob_date'] = new_date

print("\n9. Final result:")
print(dob_bin_min_temp[['bin_normalized', 'job__', 'earliest_dob_date', 'earliest_dob_date_source']].to_string())

print("\n" + "=" * 70)
print("ANALYSIS:")
print("=" * 70)
print("The issue is likely that:")
print("1. We select the most recent application (based on application_date)")
print("2. That application might not have the earliest pre__filing_date")
print("3. So we get the earliest date from that application, which is 2013-12-31 (paid)")
print("4. But the actual earliest date across ALL applications is 2011-06-14 (pre__filing_date)")
print("\nSOLUTION:")
print("After selecting the most recent application, we should find the earliest date")
print("across ALL applications for that BIN, not just from the selected application.")

