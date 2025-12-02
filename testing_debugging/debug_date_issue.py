#!/usr/bin/env python3
"""
Debug script to analyze why earliest_dob_date is showing 2013-12-31 
when pre__filing_date is 2011-06-14 for Building ID 927748, BIN 2129098
"""

import pandas as pd
import numpy as np

print("=" * 70)
print("DEBUGGING DATE EXTRACTION ISSUE")
print("=" * 70)

# Simulate the scenario
print("\n1. Simulating DOB row data:")
print("   BIN: 2129098")
print("   pre__filing_date: 2011-06-14")
print("   paid: 2013-12-31")
print("   approved: (some later date)")

# Create test data
test_data = {
    'bin_normalized': ['2129098'],
    'pre__filing_date': [pd.Timestamp('2011-06-14')],
    'paid': [pd.Timestamp('2013-12-31')],
    'approved': [pd.Timestamp('2014-01-15')],
    'filing_date': [pd.NaT],
    'first_permit_date': [pd.NaT]
}

test_df = pd.DataFrame(test_data)
print("\n2. Test DataFrame:")
print(test_df)

# Test the _get_earliest_date function
def _get_earliest_date(row, date_cols):
    """Get the earliest date and the column name that provided it."""
    date_values = []
    for col in date_cols:
        v = row.get(col, None)
        if pd.notna(v):
            # Ensure it's a datetime for proper comparison
            try:
                date_val = pd.to_datetime(v, errors='coerce')
                if pd.notna(date_val):
                    date_values.append((date_val, col))
            except:
                pass
    if date_values:
        earliest = min(date_values, key=lambda x: x[0])
        return earliest[0], earliest[1]  # Return (date, column_name)
    return pd.NaT, None

date_cols = ['pre__filing_date', 'paid', 'approved', 'filing_date', 'first_permit_date']
print(f"\n3. Date columns to check: {date_cols}")

result = _get_earliest_date(test_df.iloc[0], date_cols)
print(f"\n4. Function result: {result}")
print(f"   Date: {result[0]}")
print(f"   Source: {result[1]}")

# Test reading from source column
if result[1]:
    source_col = result[1]
    row = test_df.iloc[0]
    source_date = pd.to_datetime(row[source_col], errors='coerce')
    print(f"\n5. Reading from source column '{source_col}':")
    print(f"   Value in column: {row[source_col]}")
    print(f"   Converted: {source_date}")
    print(f"   Match with function result: {result[0] == source_date}")

print("\n" + "=" * 70)
print("ANALYSIS:")
print("=" * 70)
print("If the function is working correctly, it should:")
print("  1. Find pre__filing_date (2011-06-14) as the earliest")
print("  2. Return (2011-06-14, 'pre__filing_date')")
print("  3. When reading from source column, get 2011-06-14")
print("\nIf it's showing 2013-12-31, possible issues:")
print("  1. pre__filing_date might not be in date_cols_final")
print("  2. pre__filing_date might be NaT/None in the actual data")
print("  3. Date comparison might be failing")
print("  4. Multiple applications and wrong one selected")
print("  5. Date column might have different name (pre-filing_date vs pre__filing_date)")
