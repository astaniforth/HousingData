#!/usr/bin/env python3
"""
Test the actual scenario: job 220124381, doc__ = '01', pre__filing_date = 2011-06-14
But we're getting 2013-12-31 as earliest_dob_date
"""

import pandas as pd
import numpy as np

print("=" * 70)
print("TESTING ACTUAL SCENARIO: Job 220124381")
print("=" * 70)

# Based on user's info: job 220124381 should have pre__filing_date = 2011-06-14
# But earliest_dob_date is showing 2013-12-31
# This suggests job 220124381 IS the most recent application, but we're reading the wrong date

# Create test data matching the user's description
test_data = {
    'bin_normalized': ['2129098'],
    'job__': ['220124381'],
    'doc__': ['01'],
    'pre__filing_date': [pd.Timestamp('2011-06-14')],  # This is the correct earliest date
    'paid': [pd.Timestamp('2013-12-31')],  # This is what we're incorrectly getting
    'approved': [pd.Timestamp('2014-02-01')],
    'source': ['DOB_Job_Applications']
}

dob_df = pd.DataFrame(test_data)

print("\n1. DOB data for job 220124381:")
print(dob_df[['bin_normalized', 'job__', 'doc__', 'pre__filing_date', 'paid', 'approved']].to_string())

# Filter to doc__ = '01'
dob_df_filtered = dob_df[dob_df['doc__'] == '01'].copy()
print(f"\n2. After filtering doc__ = '01': {len(dob_df_filtered)} rows")

# Define date columns
date_cols = ['pre__filing_date', 'paid', 'approved', 'assigned', 'fully_paid', 'fully_permitted']

# Function to get earliest date
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

# Get earliest date for the row
print("\n3. Getting earliest date:")
earliest_result = _get_earliest_date(dob_df_filtered.iloc[0], date_cols)
print(f"   Result: date = {earliest_result[0]}, source = {earliest_result[1]}")

# Check what's actually in the row
row = dob_df_filtered.iloc[0]
print(f"\n4. Values in row:")
print(f"   pre__filing_date: {row['pre__filing_date']} (type: {type(row['pre__filing_date'])})")
print(f"   paid: {row['paid']} (type: {type(row['paid'])})")
print(f"   approved: {row['approved']} (type: {type(row['approved'])})")

# Check if dates are being compared correctly
print(f"\n5. Date comparison:")
pre_filing = pd.to_datetime(row['pre__filing_date'], errors='coerce')
paid = pd.to_datetime(row['paid'], errors='coerce')
print(f"   pre__filing_date as datetime: {pre_filing}")
print(f"   paid as datetime: {paid}")
print(f"   pre__filing_date < paid: {pre_filing < paid}")

# Simulate what happens after groupby
print("\n6. Simulating groupby().first():")
dob_bin_min_temp = dob_df_filtered.groupby('bin_normalized', as_index=False).first()
print(dob_bin_min_temp[['bin_normalized', 'job__', 'pre__filing_date', 'paid']].to_string())

# Check if the dates are preserved after groupby
print(f"\n7. After groupby, checking dates:")
row_after = dob_bin_min_temp.iloc[0]
print(f"   pre__filing_date: {row_after['pre__filing_date']} (type: {type(row_after['pre__filing_date'])})")
print(f"   paid: {row_after['paid']} (type: {type(row_after['paid'])})")

# Now simulate the re-reading logic
print("\n8. Simulating re-read from source column:")
source_col = 'pre__filing_date'  # This should be the source
if source_col in row_after and pd.notna(row_after[source_col]):
    re_read_date = pd.to_datetime(row_after[source_col], errors='coerce')
    print(f"   Source column: {source_col}")
    print(f"   Value in row: {row_after[source_col]}")
    print(f"   Re-read date: {re_read_date}")
else:
    print(f"   Source column {source_col} not found or is NaT")

print("\n" + "=" * 70)
print("POSSIBLE ISSUES:")
print("=" * 70)
print("1. The date columns might not be converted to datetime before comparison")
print("2. The groupby might be changing the data types")
print("3. The source column might be getting the wrong value assigned")
print("4. There might be multiple rows and we're selecting the wrong one")

