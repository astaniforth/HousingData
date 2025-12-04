#!/usr/bin/env python3
"""Test DOB query for block 2472 in Brooklyn"""

import requests

DOB_BISWEB_URL = 'https://data.cityofnewyork.us/resource/ic3t-wcy2.json'

# Query for BBL 3024720070 (base BBL)
# Borough 3 = Brooklyn, Block = 02472, Lot = 0070
print('Querying DOB BISWEB for BBL 3024720070...')

# Try with padded values
query1 = "job_type='NB' AND borough='BROOKLYN' AND block='02472' AND lot='00070'"
params = {'$where': query1, '$limit': 100}
response = requests.get(DOB_BISWEB_URL, params=params, timeout=30)
data = response.json()
print(f'With padded values (02472/00070): {len(data)} records')

# Try with unpadded values
query2 = "job_type='NB' AND borough='BROOKLYN' AND block='2472' AND lot='70'"
params = {'$where': query2, '$limit': 100}
response = requests.get(DOB_BISWEB_URL, params=params, timeout=30)
data = response.json()
print(f'With unpadded values (2472/70): {len(data)} records')

if data:
    for r in data[:3]:
        print(f"  Job: {r.get('job__')}, Type: {r.get('job_type')}, Pre-filing: {r.get('pre__filing_date')}")
        
# Also query entire block 2472 for any NB
print('\nQuerying entire block 2472 for any NB...')
query3 = "job_type='NB' AND borough='BROOKLYN' AND block='2472'"
params = {'$where': query3, '$limit': 100}
response = requests.get(DOB_BISWEB_URL, params=params, timeout=30)
data = response.json()
print(f'Total NB on block 2472: {len(data)} records')

if data:
    lots = set()
    for r in data:
        lots.add(r.get('lot'))
    print(f'Lots with NB filings: {sorted([int(l) for l in lots if l])}')
    for r in data[:5]:
        print(f"  Job: {r.get('job__')}, Lot: {r.get('lot')}, Pre-filing: {r.get('pre__filing_date')}")

