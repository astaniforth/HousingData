#!/usr/bin/env python3
"""
Trace the entire data flow to find where dates are being lost.
"""

import pandas as pd
from pathlib import Path

print("=" * 70)
print("TRACING DATA FLOW")
print("=" * 70)

# Step 1: Load the raw DOB data
print("\n1. Loading raw DOB data...")
dob_bisweb = pd.read_csv("data/processed/multifamily_finance_dob_bisweb_bin.csv", low_memory=False)
print(f"   BISWEB: {len(dob_bisweb)} records")

dob_now_path = Path("data/processed/multifamily_finance_dob_now_bin.csv")
if dob_now_path.exists():
    dob_now = pd.read_csv(dob_now_path, low_memory=False)
    print(f"   DOB NOW: {len(dob_now)} records")
else:
    dob_now = pd.DataFrame()
    print("   DOB NOW: NOT FOUND")

# Step 2: Combine
print("\n2. Combining data...")
combined = pd.concat([dob_bisweb, dob_now], ignore_index=True)
print(f"   Combined: {len(combined)} records")

# Step 3: Apply filters
print("\n3. Applying filters...")
dob_df = combined.copy()

# Filter BISWEB
if 'doc__' in dob_df.columns:
    before = len(dob_df)
    dob_df = dob_df[(dob_df['doc__'].isna()) | (dob_df['doc__'] == 1)]
    print(f"   After doc__ filter: {len(dob_df)} records (removed {before - len(dob_df)})")

# Filter DOB NOW
if 'job_filing_number' in dob_df.columns:
    dobnow_mask = dob_df['job_filing_number'].notna()
    if dobnow_mask.any():
        before = len(dob_df)
        dob_df = dob_df[
            (~dobnow_mask) | (dob_df['job_filing_number'].astype(str).str.endswith('I1', na=False))
        ]
        print(f"   After I1 filter: {len(dob_df)} records (removed {before - len(dob_df)})")

print(f"\n   Final filtered records: {len(dob_df)}")

# Step 4: Check BIN 2129098
print("\n4. Checking BIN 2129098...")
if 'bin__' in dob_df.columns:
    bin_col = 'bin__'
elif 'bin_normalized' in dob_df.columns:
    bin_col = 'bin_normalized'
elif 'bin' in dob_df.columns:
    bin_col = 'bin'
else:
    bin_col = None
    print("   No BIN column found!")

if bin_col:
    dob_df[bin_col] = dob_df[bin_col].astype(str).str.replace('.0', '')
    bin_df = dob_df[dob_df[bin_col] == '2129098']
    print(f"   Records for BIN 2129098: {len(bin_df)}")
    
    if len(bin_df) > 0:
        print(f"   Columns: {list(bin_df.columns)[:15]}...")
        if 'pre__filing_date' in bin_df.columns:
            print(f"   pre__filing_date values: {bin_df['pre__filing_date'].tolist()}")
        if 'job__' in bin_df.columns:
            print(f"   job__ values: {bin_df['job__'].tolist()}")

# Step 5: Check bin_normalized creation
print("\n5. Checking bin_normalized...")
if 'bin_normalized' not in dob_df.columns:
    if 'bin__' in dob_df.columns:
        dob_df['bin_normalized'] = dob_df['bin__'].astype(str).str.replace('.0', '')
        print("   Created bin_normalized from bin__")
    elif 'bin' in dob_df.columns:
        dob_df['bin_normalized'] = dob_df['bin'].astype(str).str.replace('.0', '')
        print("   Created bin_normalized from bin")

# Step 6: Check date columns
print("\n6. Checking date columns...")
date_cols = ['pre__filing_date', 'paid', 'approved', 'assigned', 'fully_paid', 'fully_permitted',
             'filing_date', 'first_permit_date', 'approved_date']
found_date_cols = [c for c in date_cols if c in dob_df.columns]
print(f"   Found date columns: {found_date_cols}")

# Step 7: Check HPD data
print("\n7. Loading HPD data...")
hpd_path = Path("data/raw/Affordable_Housing_Production_by_Building.csv")
if hpd_path.exists():
    hpd_df = pd.read_csv(hpd_path, low_memory=False)
    print(f"   HPD records: {len(hpd_df)}")
    
    # Check BIN 2129098
    hpd_bin = hpd_df[hpd_df['BIN'].astype(str).str.replace('.0', '') == '2129098']
    print(f"   HPD records for BIN 2129098: {len(hpd_bin)}")
    if len(hpd_bin) > 0:
        print(f"   Building IDs: {hpd_bin['Building ID'].tolist()}")
        print(f"   BIN values: {hpd_bin['BIN'].tolist()}")

# Step 8: Test the join
print("\n8. Testing the join...")
if 'bin_normalized' in dob_df.columns:
    # Get unique BINs from DOB data
    dob_bins = set(dob_df['bin_normalized'].dropna().unique())
    print(f"   Unique BINs in DOB data: {len(dob_bins)}")
    print(f"   '2129098' in DOB BINs: {'2129098' in dob_bins}")
    
    # Check sample BINs
    sample_bins = list(dob_bins)[:5]
    print(f"   Sample DOB BINs: {sample_bins}")

print("\n" + "=" * 70)

