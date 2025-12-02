#!/usr/bin/env python3
"""
Debug script to analyze why earliest_dob_date is showing 2013-12-31 
when pre__filing_date is 2011-06-14 for Building ID 927748, BIN 2129098, BBL 2022927501
"""

import pandas as pd
import numpy as np
from pathlib import Path

print("=" * 70)
print("DEBUGGING BUILDING ID 927748 DATE EXTRACTION")
print("=" * 70)

# Load the DOB data
print("\n1. Loading DOB data...")
print("-" * 70)

# Try to find DOB data files
dob_files = [
    'data/processed/multifamily_finance_dob_bisweb_bin.csv',
    'data/processed/multifamily_finance_dob_now_bin.csv',
]

dob_data = []
for file_path in dob_files:
    if Path(file_path).exists():
        print(f"Loading {file_path}...")
        df = pd.read_csv(file_path)
        print(f"  Loaded {len(df)} rows")
        dob_data.append(df)
    else:
        print(f"  {file_path} not found")

if not dob_data:
    print("ERROR: No DOB data files found!")
    print("Looking for any DOB files...")
    import glob
    dob_files_all = glob.glob('data/**/*dob*.csv', recursive=True)
    print(f"Found DOB files: {dob_files_all}")
    if dob_files_all:
        print(f"Loading first file: {dob_files_all[0]}")
        dob_data.append(pd.read_csv(dob_files_all[0]))

if dob_data:
    combined_dob = pd.concat(dob_data, ignore_index=True)
    print(f"\nTotal DOB records: {len(combined_dob)}")
    
    # Filter for the specific BIN/BBL
    test_bin = '2129098'
    test_bbl = '2022927501'
    
    print(f"\n2. Filtering for BIN {test_bin} or BBL {test_bbl}...")
    print("-" * 70)
    
    # Find BIN column
    bin_col = None
    for col in ['bin__', 'bin', 'bin_normalized']:
        if col in combined_dob.columns:
            bin_col = col
            break
    
    # Find BBL column
    bbl_col = None
    for col in ['bbl', 'bbl_normalized', 'bbl_reconstructed']:
        if col in combined_dob.columns:
            bbl_col = col
            break
    
    print(f"Using BIN column: {bin_col}")
    print(f"Using BBL column: {bbl_col}")
    
    # Filter rows
    if bin_col:
        combined_dob[bin_col] = combined_dob[bin_col].astype(str).str.replace('.0', '')
        bin_filter = combined_dob[bin_col] == test_bin
    else:
        bin_filter = pd.Series([False] * len(combined_dob))
    
    if bbl_col:
        combined_dob[bbl_col] = combined_dob[bbl_col].astype(str).str.replace('.0', '')
        bbl_filter = combined_dob[bbl_col] == test_bbl
    else:
        bbl_filter = pd.Series([False] * len(combined_dob))
    
    filtered = combined_dob[bin_filter | bbl_filter].copy()
    
    if not filtered.empty:
        print(f"Found {len(filtered)} matching rows")
        
        # Show date columns
        print("\n3. Date columns in filtered data:")
        print("-" * 70)
        date_cols = [col for col in filtered.columns if 'date' in col.lower() or col in ['paid', 'assigned', 'approved']]
        print(f"Date columns found: {date_cols}")
        
        for col in date_cols:
            if col in filtered.columns:
                non_null = filtered[col].dropna()
                if len(non_null) > 0:
                    print(f"\n  {col}:")
                    print(f"    dtype: {filtered[col].dtype}")
                    print(f"    non-null values: {len(non_null)}")
                    print(f"    sample values: {non_null.head(5).tolist()}")
        
        # Test date extraction
        print("\n4. Testing date extraction logic:")
        print("-" * 70)
        
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
        
        # Test on each row
        for idx, row in filtered.iterrows():
            print(f"\n  Row {idx}:")
            result = _get_earliest_date(row, date_cols)
            print(f"    Earliest date: {result[0]}")
            print(f"    Source column: {result[1]}")
            
            # Show all date values for this row
            print(f"    All date values:")
            for col in date_cols:
                if col in row and pd.notna(row[col]):
                    try:
                        date_val = pd.to_datetime(row[col], errors='coerce')
                        if pd.notna(date_val):
                            print(f"      {col}: {date_val} ({type(date_val).__name__})")
                    except:
                        print(f"      {col}: {row[col]} (could not convert)")
        
        # Check for job__ = '220124381' and doc__ = '01'
        print("\n5. Checking for specific job (220124381, doc__='01'):")
        print("-" * 70)
        if 'job__' in filtered.columns and 'doc__' in filtered.columns:
            specific_job = filtered[(filtered['job__'] == '220124381') & (filtered['doc__'] == '01')]
            if not specific_job.empty:
                print(f"Found {len(specific_job)} rows with job__='220124381' and doc__='01'")
                for idx, row in specific_job.iterrows():
                    print(f"\n  Row {idx}:")
                    if 'pre__filing_date' in row:
                        print(f"    pre__filing_date: {row['pre__filing_date']} (type: {type(row['pre__filing_date']).__name__})")
                    if 'paid' in row:
                        print(f"    paid: {row['paid']} (type: {type(row['paid']).__name__})")
                    
                    result = _get_earliest_date(row, date_cols)
                    print(f"    Earliest date result: {result[0]} from {result[1]}")
            else:
                print("No rows found with job__='220124381' and doc__='01'")
        else:
            print("Columns 'job__' or 'doc__' not found")
        
        # Show full row details
        print("\n6. Full row details for matching rows:")
        print("-" * 70)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(filtered.to_string())
        
    else:
        print(f"No rows found for BIN {test_bin} or BBL {test_bbl}")
        print("\nAvailable BINs (sample):")
        if bin_col:
            print(combined_dob[bin_col].dropna().unique()[:10])
        print("\nAvailable BBLs (sample):")
        if bbl_col:
            print(combined_dob[bbl_col].dropna().unique()[:10])
else:
    print("ERROR: Could not load any DOB data!")

print("\n" + "=" * 70)
print("ANALYSIS COMPLETE")
print("=" * 70)

