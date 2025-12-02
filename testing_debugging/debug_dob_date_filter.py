"""
Debug script to test DOB date extraction with doc__ = 1 and I1 filters.

The issue: Current filtering removes all matching records BEFORE groupby.
The solution: Filter WITHIN each BIN group to keep only the most recent initial filing.

This script tests the fix before applying to the notebook.
"""

import pandas as pd
import sys
sys.path.append(".")

# Load the combined DOB data from Step 3B output
# We need to test with actual data
print("=" * 70)
print("DEBUG: Testing DOB date extraction with doc__ = 1 and I1 filters")
print("=" * 70)

# Simulating the issue:
# Current logic filters globally, removing all matching data
# Correct logic: filter within each BIN group

def test_filter_approach():
    """
    Test different filtering approaches.
    
    Current (broken): 
        1. Filter ALL data to doc__ = 1 / I1
        2. Groupby BIN
        Result: Most matching BINs are filtered out
    
    Proposed (fix):
        1. Groupby BIN first
        2. Within each BIN group, prefer doc__ = 1 / I1 records
        3. If no doc__ = 1 / I1 exists for a BIN, use whatever is available
        Result: Keep all BIN matches, but prefer initial filings
    """
    
    print("\n--- Testing Filter Approaches ---\n")
    
    # Create sample data to demonstrate the issue
    sample_data = pd.DataFrame({
        'bin_normalized': ['1054682', '1054682', '1054682', '2129098', '2129098'],
        'doc__': [2.0, 3.0, 1.0, 2.0, 3.0],  # BIN 1054682 has doc 1, BIN 2129098 doesn't
        'job_filing_number': [None, None, None, None, None],  # BISWEB data
        'paid': ['2017-12-02', '2018-01-15', '2017-11-01', '2011-06-14', '2012-03-20'],
    })
    sample_data['paid'] = pd.to_datetime(sample_data['paid'])
    
    print("Sample DOB data:")
    print(sample_data)
    
    # CURRENT APPROACH (broken): Filter globally first
    print("\n--- Current Approach (Broken) ---")
    filtered = sample_data[(sample_data['doc__'].isna()) | (sample_data['doc__'] == 1)]
    print(f"After doc__ = 1 filter: {len(filtered)} records")
    print(filtered)
    
    if not filtered.empty:
        bin_min_broken = filtered.groupby('bin_normalized', as_index=False)['paid'].min()
        print(f"\nResult: {len(bin_min_broken)} BINs with dates")
        print(bin_min_broken)
    else:
        print("Result: NO DATA (all filtered out)")
    
    # PROPOSED APPROACH (fix): Prefer doc__ = 1, but fallback to any doc
    print("\n--- Proposed Approach (Fix) ---")
    
    def get_best_record(group):
        """For each BIN, prefer doc__ = 1 records, fallback to any record."""
        # Try to find doc__ = 1 records
        doc1_records = group[group['doc__'] == 1]
        if not doc1_records.empty:
            # Return the earliest date from doc 1 records
            return doc1_records.loc[doc1_records['paid'].idxmin()]
        else:
            # No doc 1, use the record with earliest date
            return group.loc[group['paid'].idxmin()]
    
    bin_min_fixed = sample_data.groupby('bin_normalized').apply(get_best_record).reset_index(drop=True)
    print(f"Result: {len(bin_min_fixed)} BINs with dates")
    print(bin_min_fixed[['bin_normalized', 'doc__', 'paid']])
    
    print("\n--- Comparison ---")
    print("Broken approach: Only BIN 1054682 has a date (2129098 was filtered out)")
    print("Fixed approach: Both BINs have dates (2129098 uses doc 2 as fallback)")

if __name__ == "__main__":
    test_filter_approach()
    
    print("\n" + "=" * 70)
    print("RECOMMENDATION:")
    print("=" * 70)
    print("""
The fix is to change the filtering logic from:

CURRENT (broken):
    1. Filter ALL records to doc__ = 1 / I1 
    2. Then groupby BIN
    
TO:
    
PROPOSED (working):
    1. Groupby BIN
    2. Within each group, PREFER doc__ = 1 / I1 records
    3. If no doc__ = 1 / I1 exists for that BIN, use any available record
    
This keeps the preference for initial filings (doc 01 / I1) while
not losing BINs that don't have initial filings in the data.
""")

