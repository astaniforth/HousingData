#!/usr/bin/env python3
"""
Debug the doc__ filtering to see if we're correctly getting doc__ = '01' records.
"""

import pandas as pd
import requests

print("=" * 70)
print("DEBUGGING DOC__ FILTER FOR JOB 220124381")
print("=" * 70)

# Simulate what the notebook does
# Query the API
url = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json?bin__=2129098&job_type=NB"
response = requests.get(url)
data = response.json()

# Create DataFrame
df = pd.DataFrame(data)
print(f"\n1. Raw data from API: {len(df)} records")
print(f"   Columns: {list(df.columns)[:10]}...")

# Check doc__ values
print(f"\n2. doc__ value counts:")
print(df['doc__'].value_counts())

# Filter to doc__ = '01'
df_filtered = df[df['doc__'] == '01']
print(f"\n3. After filtering to doc__ = '01': {len(df_filtered)} records")

# Check job 220124381
job_df = df_filtered[df_filtered['job__'] == '220124381']
print(f"\n4. Job 220124381 with doc__ = '01': {len(job_df)} records")

if len(job_df) > 0:
    print("\n   Records:")
    for idx, row in job_df.iterrows():
        print(f"     pre__filing_date: {row['pre__filing_date']}")
        print(f"     paid: {row['paid']}")
        print(f"     approved: {row['approved']}")

# Now simulate the sorting and groupby
print("\n5. Simulating the notebook's sorting and groupby logic:")

# Add bin_normalized
df_filtered = df_filtered.copy()
df_filtered['bin_normalized'] = df_filtered['bin__'].astype(str)

# Convert dates
df_filtered['pre__filing_date'] = pd.to_datetime(df_filtered['pre__filing_date'], errors='coerce')
df_filtered['paid'] = pd.to_datetime(df_filtered['paid'], errors='coerce')

# Get application_date (first non-null date)
def get_application_date(row):
    for col in ['pre__filing_date', 'paid']:
        if pd.notna(row.get(col)):
            return row[col]
    return pd.NaT

df_filtered['application_date'] = df_filtered.apply(get_application_date, axis=1)

print(f"\n   Application dates for BIN 2129098:")
bin_df = df_filtered[df_filtered['bin_normalized'] == '2129098']
print(bin_df[['job__', 'doc__', 'pre__filing_date', 'paid', 'application_date']].to_string())

# Sort by application_date descending
df_sorted = df_filtered.sort_values('application_date', ascending=False, na_position='last')

print(f"\n6. After sorting by application_date (descending):")
bin_sorted = df_sorted[df_sorted['bin_normalized'] == '2129098']
print(bin_sorted[['job__', 'doc__', 'pre__filing_date', 'paid', 'application_date']].to_string())

# Groupby and take first
df_grouped = df_sorted.groupby('bin_normalized', as_index=False).first()
print(f"\n7. After groupby().first():")
bin_grouped = df_grouped[df_grouped['bin_normalized'] == '2129098']
print(bin_grouped[['job__', 'doc__', 'pre__filing_date', 'paid', 'application_date']].to_string())

print("\n" + "=" * 70)
print("ANALYSIS:")
print("=" * 70)
print("The issue is that after sorting by application_date descending,")
print("the MOST RECENT application is selected (2011-06-14), which is correct.")
print("But then we're using the earliest date from THAT application only,")
print("not the earliest date across ALL applications.")
print("\nFor BIN 2129098, the actual earliest pre__filing_date is 08/30/2000")
print("from jobs 200636749 and 200636856 (status P = permit issued).")
print("\nBut we're getting 2011-06-14 from job 220124381 (status X = withdrawn).")
print("\nWait - the user said the date should be 2011-06-14, not 2000.")
print("So the issue is that we're getting 2013-12-31 instead of 2011-06-14.")
print("\nThe 2013-12-31 date comes from doc__ = '02' or '03', not '01'!")
print("This means the doc__ filter might not be working correctly.")

