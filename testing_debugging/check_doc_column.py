#!/usr/bin/env python3
"""
Check the doc__ column values in the processed DOB data.
"""

import pandas as pd
from pathlib import Path

print("=" * 70)
print("CHECKING DOC__ COLUMN IN PROCESSED DOB DATA")
print("=" * 70)

# Load the processed DOB data
dob_path = Path("data/processed/multifamily_finance_dob_bisweb_bin.csv")

if dob_path.exists():
    df = pd.read_csv(dob_path, low_memory=False)
    print(f"\n1. Loaded {len(df)} records from {dob_path}")
    
    if 'doc__' in df.columns:
        print(f"\n2. doc__ column dtype: {df['doc__'].dtype}")
        print(f"   doc__ value counts:")
        print(df['doc__'].value_counts())
        
        print(f"\n3. Sample doc__ values (first 10):")
        print(df['doc__'].head(10).tolist())
        
        print(f"\n4. Unique doc__ values:")
        print(df['doc__'].unique())
        
        # Check for BIN 2129098
        if 'bin_normalized' in df.columns:
            bin_df = df[df['bin_normalized'].astype(str) == '2129098']
        elif 'bin__' in df.columns:
            bin_df = df[df['bin__'].astype(str).str.replace('.0', '') == '2129098']
        else:
            bin_df = pd.DataFrame()
        
        if len(bin_df) > 0:
            print(f"\n5. Records for BIN 2129098: {len(bin_df)}")
            print(f"   doc__ values: {bin_df['doc__'].tolist()}")
            print(f"   job__ values: {bin_df['job__'].tolist() if 'job__' in bin_df.columns else 'N/A'}")
            
            # Filter to doc__ = '01'
            doc01_df = bin_df[bin_df['doc__'] == '01']
            print(f"\n6. After filtering doc__ == '01': {len(doc01_df)} records")
            
            # Try with string conversion
            doc01_str_df = bin_df[bin_df['doc__'].astype(str) == '01']
            print(f"   After filtering doc__.astype(str) == '01': {len(doc01_str_df)} records")
            
            # Try with strip
            doc01_strip_df = bin_df[bin_df['doc__'].astype(str).str.strip() == '01']
            print(f"   After filtering doc__.astype(str).str.strip() == '01': {len(doc01_strip_df)} records")
            
            if len(doc01_df) > 0:
                print(f"\n7. doc__ = '01' records for BIN 2129098:")
                cols = ['job__', 'doc__', 'pre__filing_date', 'paid', 'approved']
                cols = [c for c in cols if c in doc01_df.columns]
                print(doc01_df[cols].to_string())
    else:
        print("   doc__ column not found!")
        print(f"   Available columns: {list(df.columns)[:20]}")
else:
    print(f"   File not found: {dob_path}")

# Also check if there's a source column
if dob_path.exists():
    if 'source' in df.columns:
        print(f"\n8. source column values:")
        print(df['source'].value_counts())
    else:
        print(f"\n8. source column not found")

print("\n" + "=" * 70)

