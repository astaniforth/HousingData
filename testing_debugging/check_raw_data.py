#!/usr/bin/env python3
import pandas as pd

# Check raw DOB data for BIN 2129098
df = pd.read_csv('data/raw/new_construction_bins_dob_filings.csv', low_memory=False)
bin_df = df[df['bin__'].astype(str).str.replace('.0', '') == '2129098']
print(f'Records for BIN 2129098: {len(bin_df)}')

if len(bin_df) > 0:
    job_df = bin_df[bin_df['job__'].astype(str) == '220124381']
    print(f'\nRecords for job 220124381: {len(job_df)}')
    if len(job_df) > 0:
        cols = ['job__', 'doc__', 'pre__filing_date', 'paid', 'approved', 'bin__']
        print(job_df[cols].to_string())
        print(f'\npre__filing_date value: {job_df.iloc[0]["pre__filing_date"]}')
        print(f'paid value: {job_df.iloc[0]["paid"]}')
        print(f'approved value: {job_df.iloc[0]["approved"]}')
    else:
        print('\nAll records for BIN 2129098:')
        cols = ['job__', 'doc__', 'pre__filing_date', 'paid', 'approved', 'bin__']
        print(bin_df[cols].to_string())

