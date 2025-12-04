import pandas as pd
import requests
import time
import sys
import os
from pathlib import Path
from urllib.parse import quote

# NYC Open Data API endpoints
DOB_BISWEB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
DOB_NOW_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"
CONDO_BILLING_URL = "https://data.cityofnewyork.us/resource/p8u6-a6it.json"  # Digital Tax Map: Condominiums


def pad_block(block):
    """
    Pad block to 5 digits with leading zeros.

    Args:
        block: Block value (string or numeric)

    Returns:
        str: 5-digit block with leading zeros
    """
    if pd.isna(block):
        return None
    block_str = str(int(float(block)))
    return block_str.zfill(5)

def pad_lot(lot):
    """
    Pad lot to 5 digits with leading zeros.

    Args:
        lot: Lot value (string or numeric)

    Returns:
        str: 5-digit lot with leading zeros
    """
    if pd.isna(lot):
        return None
    lot_str = str(int(float(lot)))
    return lot_str.zfill(5)

def create_bbl(borough_code, block, lot):
    """
    Create a properly formatted BBL from borough code, block, and lot.
    Ensures proper padding of block and lot.

    Args:
        borough_code: Borough code (1-5)
        block: Block number
        lot: Lot number

    Returns:
        str: 10-digit BBL
    """
    if pd.isna(borough_code) or pd.isna(block) or pd.isna(lot):
        return None

    borough_str = str(int(borough_code))
    block_str = pad_block(block)
    lot_str = pad_lot(lot)

    if not block_str or not lot_str:
        return None

    return f"{borough_str}{block_str}{lot_str}"

def validate_bbl_borough_consistency(bbl, borough_name):
    """
    Validate that BBL borough code matches the provided borough name.

    Args:
        bbl: BBL value (can be string or numeric)
        borough_name: Borough name from the data

    Returns:
        tuple: (is_valid, expected_borough, actual_borough_code)
    """
    if pd.isna(bbl) or pd.isna(borough_name):
        return False, None, None

    try:
        bbl_str = str(int(float(bbl)))
        if len(bbl_str) != 10:
            return False, None, None

        borough_code = bbl_str[0]

        # Borough mapping
        borough_mapping = {
            '1': 'MANHATTAN',
            '2': 'BRONX',
            '3': 'BROOKLYN',
            '4': 'QUEENS',
            '5': 'STATEN ISLAND'
        }

        expected_borough = borough_mapping.get(borough_code)
        actual_borough = str(borough_name).upper().strip()

        is_valid = expected_borough == actual_borough
        return is_valid, expected_borough, actual_borough

    except (ValueError, KeyError):
        return False, None, None

def decompose_bbl(bbl, borough_name=None):
    """
    Decompose BBL into borough, block, lot components for DOB API searching.
    Ensures block and lot are properly padded.

    Args:
        bbl: BBL value to decompose
        borough_name: Optional borough name for validation

    Returns:
        tuple: (borough_name, block_str, lot_str) or (borough_name, block_str, lot_str, is_valid) if validation requested
    """
    if pd.isna(bbl):
        return None, None, None

    bbl_str = str(int(float(bbl)))  # Convert to string, remove .0

    if len(bbl_str) != 10:
        return None, None, None

    borough_code = bbl_str[0]
    block_raw = bbl_str[1:6]  # positions 1-5 (5 digits)
    lot_raw = bbl_str[6:]     # positions 6-9 (4 digits)

    # Ensure proper padding (block: 5 digits, lot: 5 digits)
    block_str = pad_block(block_raw)
    lot_str = pad_lot(lot_raw)

    # Convert borough code to name (DOB APIs use names, not codes)
    borough_mapping = {
        '1': 'MANHATTAN',
        '2': 'BRONX',
        '3': 'BROOKLYN',
        '4': 'QUEENS',
        '5': 'STATEN ISLAND'
    }

    borough_name_from_bbl = borough_mapping.get(borough_code, borough_code)

    # Validate consistency if borough_name provided
    if borough_name is not None:
        is_valid, expected, actual = validate_bbl_borough_consistency(bbl, borough_name)
        return borough_name_from_bbl, block_str, lot_str, is_valid
    else:
        return borough_name_from_bbl, block_str, lot_str

def query_dob_bisweb_bin(search_list, limit=50000):
    """
    Query DOB BISWEB API for job filings matching BINs.

    Args:
        search_list: List of BINs to search for
        limit: Maximum number of records to retrieve

    Returns:
        DataFrame with matching records
    """
    print("\nQuerying DOB BISWEB API by BIN")
    print(f"Looking for job type: NB")
    print(f"Number of BINs to check: {len(search_list)}")

    all_results = []
    batch_size = 300  # Optimal batch size for BIN searches

    for i in range(0, len(search_list), batch_size):
        batch = search_list[i:i+batch_size]

        bin_filter = " OR ".join([f"bin__='{bin_num}'" for bin_num in batch])
        query = f"job_type='NB' AND ({bin_filter})"

        params = {
            '$where': query,
            '$limit': limit
        }

        try:
            print(f"  Querying batch {i//batch_size + 1} (BINs {i+1}-{min(i+batch_size, len(search_list))})...")
            response = requests.get(DOB_BISWEB_URL, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data:
                all_results.extend(data)
                print(f"    Found {len(data)} records")
            else:
                print("    No records found")

            # Rate limiting
            time.sleep(0.1)

        except Exception as e:
            print(f"    Error querying batch: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"    Response: {e.response.text[:200]}")
            continue

    if all_results:
        df = pd.DataFrame(all_results)
        print(f"\nTotal records found: {len(df)}")
        return df
    else:
        print("\nNo records found")
        return pd.DataFrame()


def query_dob_bisweb_bbl(search_list, limit=50000):
    """
    Query DOB BISWEB API for job filings matching BBL components using batched OR queries.

    Args:
        search_list: List of BBL tuples (borough, block, lot) to search for
        limit: Maximum number of records to retrieve

    Returns:
        DataFrame with matching records
    """
    print("\nQuerying DOB BISWEB API by BBL")
    print(f"Looking for job type: NB")
    print(f"Number of BBLs to check: {len(search_list)}")

    all_results = []
    batch_size = 50  # Larger batches now that we're using OR queries

    for i in range(0, len(search_list), batch_size):
        batch = search_list[i:i+batch_size]

        # Build batched OR query
        conditions = []
        for bbl_tuple in batch:
            if not bbl_tuple or len(bbl_tuple) != 3:
                continue

            borough, block, lot = bbl_tuple
            condition = f"(job_type='NB' AND borough='{borough}' AND block='{block}' AND lot='{lot}')"
            conditions.append(condition)

        if not conditions:
            continue

        batched_query = " OR ".join(conditions)

        params = {
            '$where': batched_query,
            '$limit': limit
        }

        try:
            print(f"  Querying batch {i//batch_size + 1} ({len(conditions)} BBLs)...")
            response = requests.get(DOB_BISWEB_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data:
                all_results.extend(data)
                print(f"    Found {len(data)} records")
            else:
                print("    No records found")

            # Rate limiting
            time.sleep(0.1)

        except Exception as e:
            print(f"    Error querying batch: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"    Response: {e.response.text[:200]}")
            continue

    if all_results:
        df = pd.DataFrame(all_results)
        print(f"\nTotal records found: {len(df)}")
        return df
    else:
        print("\nNo records found")
        return pd.DataFrame()


def query_dobnow_bin(search_list, limit=50000):
    """
    Query DOB NOW API for job filings matching BINs.

    Args:
        search_list: List of BINs to search for
        limit: Maximum number of records to retrieve

    Returns:
        DataFrame with matching records
    """
    print("\nQuerying DOB NOW API by BIN")
    print(f"Looking for job type: New Building")
    print(f"Number of BINs to check: {len(search_list)}")

    all_results = []
    batch_size = 300  # Optimal batch size for BIN searches

    for i in range(0, len(search_list), batch_size):
        batch = search_list[i:i+batch_size]

        bin_filter = " OR ".join([f"bin='{bin_num}'" for bin_num in batch])
        query = f"job_type='New Building' AND ({bin_filter})"

        params = {
            '$where': query,
            '$limit': limit
        }

        try:
            print(f"  Querying batch {i//batch_size + 1} (BINs {i+1}-{min(i+batch_size, len(search_list))})...")
            response = requests.get(DOB_NOW_URL, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data:
                all_results.extend(data)
                print(f"    Found {len(data)} records")
            else:
                print("    No records found")

            # Rate limiting
            time.sleep(0.1)

        except Exception as e:
            print(f"    Error querying batch: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"    Response: {e.response.text[:200]}")
            continue

    if all_results:
        df = pd.DataFrame(all_results)
        print(f"\nTotal records found: {len(df)}")
        return df
    else:
        print("\nNo records found")
        return pd.DataFrame()


def get_condo_billing_bbl(base_bbl):
    """
    Query NYC Condominiums API to find the billing BBL for a base BBL.
    
    Uses the Digital Tax Map: Condominiums dataset which maps base BBLs
    to their billing BBLs (typically lot 7501 format).
    
    Args:
        base_bbl: Base BBL as integer or string (e.g., 2024410001)
    
    Returns:
        Tuple (borough, block, lot) for the billing BBL, or None if not found
    """
    try:
        base_bbl_str = str(int(float(base_bbl))).zfill(10)
        
        # Query condominiums API for this base BBL
        params = {
            '$where': f"condo_base_bbl='{base_bbl_str}'",
            '$limit': 1
        }
        
        response = requests.get(CONDO_BILLING_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            return None
        
        # Get the billing BBL from the first record
        record = data[0]
        billing_bbl = record.get('condo_billing_bbl')
        
        if not billing_bbl:
            return None
        
        # Decompose billing BBL to (borough, block, lot)
        result = decompose_bbl(int(billing_bbl))
        
        if result and len(result) >= 3:
            borough, block, lot = result[:3]
            # Pad block and lot to 5 digits
            block_padded = pad_block(block)
            lot_padded = pad_lot(lot)
            return (borough, block_padded, lot_padded)
        
        return None
        
    except Exception as e:
        print(f"    Error querying condominiums API for BBL {base_bbl}: {str(e)[:50]}")
        return None


def get_all_condo_related_bbls(bbl):
    """
    Find all BBLs related to a condo by searching both directions:
    1. If input BBL is a billing BBL → find base BBL
    2. If input BBL is a base BBL → find all billing BBLs
    3. Then get all BBLs in the condo complex
    
    This handles the case where HPD has the billing BBL (e.g., lot 7504)
    but DOB permits are filed on the base BBL (e.g., lot 70).
    
    Args:
        bbl: BBL value (string or int) to look up
    
    Returns:
        set: All related BBLs (base + billing), empty set if not a condo
    """
    try:
        bbl_str = str(int(float(bbl))).zfill(10)
        related_bbls = set()
        base_bbl = None
        
        # Step 1: Check if this is a billing BBL (search condo_billing_bbl)
        params = {
            '$where': f"condo_billing_bbl='{bbl_str}'",
            '$limit': 1
        }
        response = requests.get(CONDO_BILLING_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data:
            # Found as billing BBL - get the base BBL
            base_bbl = data[0].get('condo_base_bbl')
            related_bbls.add(bbl_str)
        else:
            # Step 1b: Check if this is a base BBL (search condo_base_bbl)
            params = {
                '$where': f"condo_base_bbl='{bbl_str}'",
                '$limit': 1
            }
            response = requests.get(CONDO_BILLING_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data:
                # This IS a base BBL
                base_bbl = bbl_str
            else:
                # Not a condo property
                return set()
        
        if not base_bbl:
            return set()
        
        # Step 2: Get ALL billing BBLs for this base BBL
        params = {
            '$where': f"condo_base_bbl='{base_bbl}'",
            '$limit': 1000  # Get all related billing BBLs
        }
        response = requests.get(CONDO_BILLING_URL, params=params, timeout=30)
        response.raise_for_status()
        all_records = response.json()
        
        # Add base BBL and all billing BBLs
        related_bbls.add(base_bbl)
        for record in all_records:
            billing_bbl = record.get('condo_billing_bbl')
            if billing_bbl:
                related_bbls.add(str(billing_bbl).zfill(10))
        
        return related_bbls
        
    except Exception as e:
        print(f"    Error querying condo API for BBL {bbl}: {str(e)[:50]}")
        return set()


def query_dob_for_condo_bbls(bbl_list, limit=50000):
    """
    Query DOB BISWEB and DOB NOW for NB filings on condo-related BBLs.
    
    For each input BBL:
    1. Find all condo-related BBLs (base + billing)
    2. Query DOB APIs for NB filings on all related BBLs
    
    Args:
        bbl_list: List of BBLs to check for condo relationships
        limit: Maximum records per query
    
    Returns:
        DataFrame with all NB filings found via condo lookup
    """
    print("\n" + "=" * 70)
    print("CONDO FALLBACK")
    print("=" * 70)
    print(f"Checking {len(bbl_list)} BBLs for condo relationships...")
    
    all_results = []
    condo_count = 0
    bbls_to_query = set()
    
    # Step 1: Collect all condo-related BBLs
    for bbl in bbl_list:
        related_bbls = get_all_condo_related_bbls(bbl)
        if related_bbls:
            condo_count += 1
            bbls_to_query.update(related_bbls)
    
    if not bbls_to_query:
        print(f"  No condo properties found among {len(bbl_list)} BBLs")
        return pd.DataFrame()
    
    print(f"  Found {condo_count} condo properties with {len(bbls_to_query)} total BBLs to query")
    
    # Step 2: Convert BBLs to query tuples (borough, block, lot)
    bbl_tuples = []
    for bbl_str in bbls_to_query:
        bbl_padded = str(bbl_str).zfill(10)
        borough_code = bbl_padded[0]
        # BISWEB requires PADDED block (5 digits) and lot (5 digits)
        block = bbl_padded[1:6]
        lot = bbl_padded[6:].zfill(5)
        
        borough_map = {'1': 'MANHATTAN', '2': 'BRONX', '3': 'BROOKLYN', '4': 'QUEENS', '5': 'STATEN ISLAND'}
        borough = borough_map.get(borough_code)
        
        if borough:
            bbl_tuples.append((borough, block, lot))
    
    # Step 3: Query BISWEB
    print(f"  Querying BISWEB for {len(bbl_tuples)} condo BBLs...")
    bisweb_results = query_dob_bisweb_bbl(bbl_tuples, limit)
    if not bisweb_results.empty:
        bisweb_results['source'] = 'CONDO_FALLBACK_BISWEB'
        all_results.append(bisweb_results)
        print(f"    Found {len(bisweb_results)} BISWEB records")
    
    # Step 4: Query DOB NOW
    print(f"  Querying DOB NOW for {len(bbl_tuples)} condo BBLs...")
    dobnow_results = query_dobnow_bbl(bbl_tuples, limit)
    if not dobnow_results.empty:
        dobnow_results['source'] = 'CONDO_FALLBACK_DOBNOW'
        all_results.append(dobnow_results)
        print(f"    Found {len(dobnow_results)} DOB NOW records")
    
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        print(f"\n  ✅ Condo fallback found {len(combined)} total records")
        return combined
    else:
        print(f"\n  ❌ No DOB records found via condo fallback")
        return pd.DataFrame()


def query_condo_lots_for_bbl(borough, block, base_lot, base_bbl=None, limit=50000):
    """
    Query DOB BISWEB API for condo billing BBL when base lot doesn't match.
    
    Uses the NYC Condominiums API to find the billing BBL (typically lot 7501)
    for a given base BBL, then queries DOB for permits on that billing BBL.
    
    Args:
        borough: Borough name (e.g., 'BRONX')
        block: Block number (padded to 5 digits)
        base_lot: Base lot number that didn't match
        base_bbl: Base BBL as integer or string (optional, will be constructed if not provided)
        limit: Maximum number of records to retrieve
    
    Returns:
        DataFrame with matching records, or empty DataFrame if none found
    """
    # Construct base BBL if not provided
    if base_bbl is None:
        borough_map = {'MANHATTAN': '1', 'BRONX': '2', 'BROOKLYN': '3', 'QUEENS': '4', 'STATEN ISLAND': '5'}
        borough_code = borough_map.get(borough.upper())
        if not borough_code:
            return pd.DataFrame()
        
        block_clean = str(int(float(block.replace('.0', ''))))
        lot_clean = str(int(float(base_lot.replace('.0', ''))))
        base_bbl = borough_code + block_clean.zfill(5) + lot_clean.zfill(4)
    
    print(f"\nQuerying condo billing BBL for {borough}/{block}/{base_lot} (base BBL: {base_bbl})")
    
    # Get billing BBL from the Condominiums API
    billing_bbl_tuple = get_condo_billing_bbl(base_bbl)
    
    if not billing_bbl_tuple:
        print(f"  No condo billing BBL found for base BBL {base_bbl}")
        return pd.DataFrame()
    
    billing_borough, billing_block, billing_lot = billing_bbl_tuple
    print(f"  Found billing BBL: {billing_borough}/{billing_block}/{billing_lot}")
    
    # Query DOB for the billing BBL
    query = f"job_type='NB' AND borough='{billing_borough}' AND block='{billing_block}' AND lot='{billing_lot}'"
    
    params = {
        '$where': query,
        '$limit': limit
    }
    
    try:
        response = requests.get(DOB_BISWEB_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data:
            df = pd.DataFrame(data)
            print(f"  Found {len(df)} records on billing BBL {billing_borough}/{billing_block}/{billing_lot}")
            return df
        else:
            print(f"  No DOB records found on billing BBL")
            return pd.DataFrame()
    except Exception as e:
        print(f"  Error querying DOB for billing BBL: {str(e)[:50]}")
        return pd.DataFrame()


def query_dob_by_address(address_list, limit=50000):
    """
    Query DOB BISWEB and DOB NOW APIs by address as a last fallback.
    
    This searches for New Building permits by house number and street name
    when BIN and BBL queries have failed.
    
    Args:
        address_list: List of address tuples (borough, house_number, street_name)
                     e.g., [('BRONX', '655', 'MORRIS AVENUE'), ...]
        limit: Maximum number of records to retrieve per address
    
    Returns:
        DataFrame with matching records from both APIs
    """
    if not address_list:
        return pd.DataFrame()
    
    print("\nQuerying DOB APIs by address (last fallback)")
    print(f"Number of addresses to check: {len(address_list)}")
    
    all_results = []
    
    for borough, house_number, street_name in address_list:
        if not borough or not house_number or not street_name:
            continue
        
        # Clean up address components
        house_clean = str(house_number).strip()
        street_clean = str(street_name).strip().upper()
        
        print(f"  Searching: {house_clean} {street_clean}, {borough}")
        
        # Query BISWEB
        try:
            # BISWEB uses house__ and street_name columns
            query_bisweb = f"job_type='NB' AND borough='{borough}' AND house__='{house_clean}' AND street_name LIKE '%{street_clean}%'"
            params_bisweb = {
                '$where': query_bisweb,
                '$limit': limit
            }
            response_bisweb = requests.get(DOB_BISWEB_URL, params=params_bisweb, timeout=30)
            response_bisweb.raise_for_status()
            data_bisweb = response_bisweb.json()
            if data_bisweb:
                all_results.extend(data_bisweb)
                print(f"    BISWEB: Found {len(data_bisweb)} records")
        except Exception as e:
            print(f"    BISWEB: Error - {str(e)[:50]}")
        
        # Query DOB NOW
        try:
            # DOB NOW uses house_no and street_name columns
            query_dobnow = f"job_type='New Building' AND borough='{borough}' AND house_no='{house_clean}' AND street_name LIKE '%{street_clean}%'"
            params_dobnow = {
                '$where': query_dobnow,
                '$limit': limit
            }
            response_dobnow = requests.get(DOB_NOW_URL, params=params_dobnow, timeout=30)
            response_dobnow.raise_for_status()
            data_dobnow = response_dobnow.json()
            if data_dobnow:
                all_results.extend(data_dobnow)
                print(f"    DOB NOW: Found {len(data_dobnow)} records")
        except Exception as e:
            print(f"    DOB NOW: Error - {str(e)[:50]}")
    
    if all_results:
        df = pd.DataFrame(all_results)
        print(f"\nTotal records found by address: {len(df)}")
        return df
    else:
        print("\nNo records found by address")
        return pd.DataFrame()


def query_dobnow_bbl(search_list, limit=50000):
    """
    Query DOB NOW API for job filings matching BBL components using batched OR queries.

    Args:
        search_list: List of BBL tuples (borough, block, lot) to search for
        limit: Maximum number of records to retrieve

    Returns:
        DataFrame with matching records
    """
    print("\nQuerying DOB NOW API by BBL")
    print(f"Looking for job type: New Building")
    print(f"Number of BBLs to check: {len(search_list)}")

    all_results = []
    batch_size = 50  # Larger batches now that we're using OR queries

    for i in range(0, len(search_list), batch_size):
        batch = search_list[i:i+batch_size]

        # Build batched OR query
        conditions = []
        for bbl_tuple in batch:
            if not bbl_tuple or len(bbl_tuple) != 3:
                continue

            borough, block, lot = bbl_tuple
            # DOB NOW requires unpadded block and lot values (unlike BISWEB which needs padded)
            block_unpadded = str(int(block)) if block else block
            lot_unpadded = str(int(lot)) if lot else lot
            condition = f"(job_type='New Building' AND borough='{borough}' AND block='{block_unpadded}' AND lot='{lot_unpadded}')"
            conditions.append(condition)

        if not conditions:
            continue

        batched_query = " OR ".join(conditions)

        params = {
            '$where': batched_query,
            '$limit': limit
        }

        try:
            print(f"  Querying batch {i//batch_size + 1} ({len(conditions)} BBLs)...")
            response = requests.get(DOB_NOW_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data:
                all_results.extend(data)
                print(f"    Found {len(data)} records")
            else:
                print("    No records found")

            # Rate limiting
            time.sleep(0.1)

        except Exception as e:
            print(f"    Error querying batch: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"    Response: {e.response.text[:200]}")
            continue

    if all_results:
        df = pd.DataFrame(all_results)
        print(f"\nTotal records found: {len(df)}")
        return df
    else:
        print("\nNo records found")
        return pd.DataFrame()

def query_dob_filings(search_file_path, output_path=None, use_bbl_fallback=True):
    """
    Query both DOB APIs for new building filings associated with BINs or BBLs.

    Args:
        search_file_path: Path to file containing BINs/BBLs (one per line) or CSV with search data
        output_path: Path to save results CSV
        use_bbl_fallback: Whether to use BBL fallback for missing BINs
    """

    # Try to read as CSV first (with BIN/BBL columns), fall back to text file
    search_df = None
    try:
        search_df = pd.read_csv(search_file_path)
        print(f"Reading search data from CSV: {search_file_path}")
    except:
        # Fall back to reading as text file with BINs
        print(f"Reading BINs from text file: {search_file_path}")
        with open(search_file_path, 'r') as f:
            bins_from_file = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        # Create a simple dataframe for processing
        search_df = pd.DataFrame({'BIN': bins_from_file})

    # Extract all BINs for initial search
    bins = []
    if 'BIN_normalized' in search_df.columns:
        bins = [str(b).replace('.0', '') for b in search_df['BIN_normalized'].dropna() if str(b) != 'nan']
    elif 'BIN' in search_df.columns:
        bins = [str(b).replace('.0', '') for b in search_df['BIN'].dropna() if str(b) != 'nan']

    print(f"Found {len(bins)} BINs to search initially")

    # Step 1: Query by BIN for all properties (both BISWEB and DOB NOW)
    print("=" * 70)
    print("STEP 1A: QUERYING DOB BISWEB BY BIN")
    print("=" * 70)
    dob_filings_bin = query_dob_bisweb_bin(bins)

    print("=" * 70)
    print("STEP 1B: QUERYING DOB NOW BY BIN")
    print("=" * 70)
    dob_now_filings_bin = query_dobnow_bin(bins)

    # Identify which BINs got matches from each API separately
    bisweb_matched_bins = set()
    dobnow_matched_bins = set()

    if not dob_filings_bin.empty and 'bin__' in dob_filings_bin.columns:
        bisweb_matched_bins.update(dob_filings_bin['bin__'].dropna().astype(str).unique())
    if not dob_now_filings_bin.empty and 'bin' in dob_now_filings_bin.columns:
        dobnow_matched_bins.update(dob_now_filings_bin['bin'].dropna().astype(str).unique())

    # BBL fallback: only for BINs that didn't match in their respective API
    bisweb_unmatched_bins = [b for b in bins if b not in bisweb_matched_bins]
    dobnow_unmatched_bins = [b for b in bins if b not in dobnow_matched_bins]

    print(f"BISWEB BIN found matches for {len(bisweb_matched_bins)} BINs")
    print(f"DOB NOW BIN found matches for {len(dobnow_matched_bins)} BINs")
    print(f"{len(bisweb_unmatched_bins)} BINs need BISWEB BBL fallback")
    print(f"{len(dobnow_unmatched_bins)} BINs need DOB NOW BBL fallback")

    # Track BIN matching performance
    all_matched_bins = bisweb_matched_bins.union(dobnow_matched_bins)

    # Step 2: BBL fallback - separate for each API
    dob_filings_bbl = pd.DataFrame()
    dob_now_filings_bbl = pd.DataFrame()

    # Step 2A: BISWEB BBL fallback for BINs that didn't match in BISWEB BIN
    if use_bbl_fallback and len(bisweb_unmatched_bins) > 0 and search_df is not None and 'BBL' in search_df.columns:
            print("\n" + "=" * 70)
            print("STEP 2A: QUERYING DOB BISWEB BY BBL (FALLBACK)")
            print("=" * 70)

            # Get BBLs for unmatched BINs
            bbl_tuples = []
            validation_warnings = []

            for bin_val in bisweb_unmatched_bins:
                # Find the corresponding row in search_df
                if 'BIN_normalized' in search_df.columns:
                    mask = search_df['BIN_normalized'].astype(str).str.replace('.0', '') == bin_val
                elif 'BIN' in search_df.columns:
                    mask = search_df['BIN'].astype(str).str.replace('.0', '') == bin_val
                else:
                    continue

                matching_rows = search_df[mask]
                if not matching_rows.empty:
                    row = matching_rows.iloc[0]
                    bbl_val = row['BBL']
                    borough_name = row.get('Borough')  # Get borough from data

                    # Validate BBL-borough consistency
                    bbl_result = decompose_bbl(bbl_val, borough_name)

                    if len(bbl_result) == 4:  # Validation included
                        borough_from_bbl, block_str, lot_str, is_valid = bbl_result
                        if not is_valid:
                            warning_msg = f"WARNING: BIN {bin_val} - BBL {bbl_val} suggests {borough_from_bbl} but data shows {borough_name}"
                            validation_warnings.append(warning_msg)
                            print(f"  ⚠️  {warning_msg}")
                        bbl_tuple = (borough_from_bbl, block_str, lot_str)
                    else:  # No validation
                        bbl_tuple = bbl_result

                    if bbl_tuple[0] is not None:  # Valid BBL
                        bbl_tuples.append(bbl_tuple)

            if validation_warnings:
                print(f"\n⚠️  Found {len(validation_warnings)} BBL-borough inconsistencies!")
                print("These may indicate data quality issues in the HPD dataset.")

            if bbl_tuples:
                print(f"Searching {len(bbl_tuples)} BBLs for BISWEB BIN unmatched...")
                dob_filings_bbl = query_dob_bisweb_bbl(bbl_tuples)

    # Step 2B: DOB NOW BBL fallback for BINs that didn't match in DOB NOW BIN
    if use_bbl_fallback and len(dobnow_unmatched_bins) > 0 and search_df is not None and 'BBL' in search_df.columns:
            print("\n" + "=" * 70)
            print("STEP 2B: QUERYING DOB NOW BY BBL (FALLBACK)")
            print("=" * 70)

            # Get BBLs for DOB NOW unmatched BINs
            bbl_tuples_dobnow = []
            validation_warnings_dobnow = []

            for bin_val in dobnow_unmatched_bins:
                # Find the corresponding row in search_df
                if 'BIN_normalized' in search_df.columns:
                    mask = search_df['BIN_normalized'].astype(str).str.replace('.0', '') == bin_val
                elif 'BIN' in search_df.columns:
                    mask = search_df['BIN'].astype(str).str.replace('.0', '') == bin_val
                else:
                    continue

                matching_rows = search_df[mask]
                if not matching_rows.empty:
                    row = matching_rows.iloc[0]
                    bbl_val = row['BBL']
                    borough_name = row.get('Borough')  # Get borough from data

                    # Validate BBL-borough consistency
                    bbl_result = decompose_bbl(bbl_val, borough_name)

                    if len(bbl_result) == 4:  # Validation included
                        borough_from_bbl, block_str, lot_str, is_valid = bbl_result
                        if not is_valid:
                            warning_msg = f"WARNING: BIN {bin_val} - BBL {bbl_val} suggests {borough_from_bbl} but data shows {borough_name}"
                            validation_warnings_dobnow.append(warning_msg)
                            print(f"  ⚠️  {warning_msg}")
                        bbl_tuple = (borough_from_bbl, block_str, lot_str)
                    else:  # No validation
                        bbl_tuple = bbl_result

                    if bbl_tuple[0] is not None:  # Valid BBL
                        bbl_tuples_dobnow.append(bbl_tuple)

            if validation_warnings_dobnow:
                print(f"\n⚠️  Found {len(validation_warnings_dobnow)} BBL-borough inconsistencies!")
                print("These may indicate data quality issues in the HPD dataset.")

            if bbl_tuples_dobnow:
                print(f"Searching {len(bbl_tuples_dobnow)} BBLs for DOB NOW BIN unmatched...")
                dob_now_filings_bbl = query_dobnow_bbl(bbl_tuples_dobnow)

    # Combine all results
    dob_filings = pd.concat([dob_filings_bin, dob_filings_bbl], ignore_index=True) if not dob_filings_bbl.empty else dob_filings_bin
    dob_now_filings = pd.concat([dob_now_filings_bin, dob_now_filings_bbl], ignore_index=True) if not dob_now_filings_bbl.empty else dob_now_filings_bin
    
    # Combine results
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    if not dob_filings.empty:
        print(f"\nDOB Job Application Filings: {len(dob_filings)} records")
        print(f"Columns: {', '.join(dob_filings.columns.tolist())}")
        # Add source column
        dob_filings['source'] = 'DOB_Job_Applications'
    
    if not dob_now_filings.empty:
        print(f"\nDOB NOW Job Applications: {len(dob_now_filings)} records")
        print(f"Columns: {', '.join(dob_now_filings.columns.tolist())}")
        # Add source column
        dob_now_filings['source'] = 'DOB_NOW'
    
    # Normalize BIN columns before combining
    # DOB Job Applications uses "bin__", DOB NOW uses "bin"
    if not dob_filings.empty and 'bin__' in dob_filings.columns:
        dob_filings['bin_normalized'] = dob_filings['bin__'].astype(str)
    if not dob_now_filings.empty and 'bin' in dob_now_filings.columns:
        dob_now_filings['bin_normalized'] = dob_now_filings['bin'].astype(str)
    
    # Combine both dataframes - use all columns and fill missing with NaN
    if not dob_filings.empty and not dob_now_filings.empty:
        # Get all unique columns
        all_cols = list(set(dob_filings.columns.tolist() + dob_now_filings.columns.tolist()))
        
        # Ensure bin_normalized and source are included
        if 'bin_normalized' not in all_cols:
            all_cols.append('bin_normalized')
        if 'source' not in all_cols:
            all_cols.append('source')
        
        # Reindex both dataframes to have the same columns
        dob_filings_aligned = dob_filings.reindex(columns=all_cols)
        dob_now_filings_aligned = dob_now_filings.reindex(columns=all_cols)
        
        combined = pd.concat([dob_filings_aligned, dob_now_filings_aligned], ignore_index=True)
    elif not dob_filings.empty:
        combined = dob_filings.copy()
        if 'bin__' in combined.columns and 'bin_normalized' not in combined.columns:
            combined['bin_normalized'] = combined['bin__'].astype(str)
    elif not dob_now_filings.empty:
        combined = dob_now_filings.copy()
        if 'bin' in combined.columns and 'bin_normalized' not in combined.columns:
            combined['bin_normalized'] = combined['bin'].astype(str)
    else:
        print("\nNo filings found in either API")
        return
    
    print(f"\nTotal combined records: {len(combined)}")

    # Show unique BINs found
    if 'bin_normalized' in combined.columns:
        unique_bins = combined['bin_normalized'].dropna().unique()
        print(f"Unique BINs with filings: {len(unique_bins)}")
        try:
            bin_nums = sorted([int(b) for b in unique_bins if str(b).strip() and str(b).isdigit()])[:20]
            print(f"Sample BINs with filings: {bin_nums}...")
        except:
            print(f"Sample BINs: {list(unique_bins)[:20]}")

    # Save results
    processed_dir = Path('data/processed')
    processed_dir.mkdir(parents=True, exist_ok=True)
    input_stem = Path(search_file_path).stem

    if output_path is None:
        output_path = processed_dir / f"{input_stem}_dob_filings.csv"
    else:
        output_path = Path(output_path)

    combined.to_csv(output_path, index=False)
    print(f"\nResults saved to: {output_path}")

    # Also create a summary by BIN
    if 'bin_normalized' in combined.columns:
        summary_path = processed_dir / f"{input_stem}_dob_filings_summary.csv"
        summary = combined.groupby('bin_normalized').size().reset_index()
        summary.columns = ['BIN', 'Number_of_Filings']
        summary = summary.sort_values('Number_of_Filings', ascending=False)
        summary.to_csv(summary_path, index=False)
        print(f"Summary by BIN saved to: {summary_path}")
    
    # Show sample records
    print("\n" + "=" * 70)
    print("SAMPLE RECORDS")
    print("=" * 70)
    if len(combined) > 0:
        # Show first few columns that are likely most relevant
        sample_cols = ['bin_normalized', 'job_type', 'source']
        if 'bin__' in combined.columns:
            sample_cols.insert(0, 'bin__')
        elif 'bin' in combined.columns:
            sample_cols.insert(0, 'bin')
        
        # Add date columns if available
        date_cols = [col for col in combined.columns if 'date' in col.lower() or 'filing' in col.lower()]
        sample_cols.extend(date_cols[:2])  # Add up to 2 date columns
        
        # Add other relevant columns
        if 'job__' in combined.columns:
            sample_cols.append('job__')
        if 'job_filing_number' in combined.columns:
            sample_cols.append('job_filing_number')
        if 'street_name' in combined.columns:
            sample_cols.append('street_name')
        if 'house__' in combined.columns:
            sample_cols.append('house__')
        elif 'house_no' in combined.columns:
            sample_cols.append('house_no')
        
        available_cols = [col for col in sample_cols if col in combined.columns]
        print(combined[available_cols].head(10).to_string(index=False))
    
    return combined

if __name__ == "__main__":
    if len(sys.argv) > 1:
        bin_file = sys.argv[1]
    else:
        # Look for BIN file
        if os.path.exists('new_construction_bins.txt'):
            bin_file = 'new_construction_bins.txt'
            print(f"Using BIN file: {bin_file}\n")
        else:
            print("Please provide the path to the BIN file:")
            print("Usage: python query_dob_filings.py <path_to_bin_file>")
            sys.exit(1)
    
    if not os.path.exists(bin_file):
        print(f"Error: File '{bin_file}' not found.")
        sys.exit(1)
    
    query_dob_filings(bin_file, use_bbl_fallback=True)
