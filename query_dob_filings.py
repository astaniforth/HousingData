import pandas as pd
import requests
import time
import sys
import os
from urllib.parse import quote

# NYC Open Data API endpoints
DOB_JOB_APPLICATIONS_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
DOB_NOW_JOB_APPLICATIONS_URL = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"

def decompose_bbl(bbl):
    """Decompose BBL into borough, block, lot components"""
    if pd.isna(bbl):
        return None, None, None

    bbl_str = str(int(float(bbl)))  # Convert to string, remove .0

    if len(bbl_str) != 10:
        return None, None, None

    borough_code = bbl_str[0]
    block = int(bbl_str[1:6])  # Convert to integer
    lot = int(bbl_str[6:10])   # Convert to integer

    # Convert borough code to name (DOB APIs use names, not codes)
    borough_mapping = {
        '1': 'MANHATTAN',
        '2': 'BROOKLYN',
        '3': 'QUEENS',
        '4': 'BRONX',
        '5': 'STATEN ISLAND'
    }

    borough_name = borough_mapping.get(borough_code, borough_code)

    return borough_name, block, lot

def query_dob_api(url, search_list, job_type="NB", search_type="bin", limit=50000):
    """
    Query DOB API for job filings matching BINs or BBL components.

    Args:
        url: API endpoint URL
        search_list: List of BINs or BBL tuples to search for
        job_type: Job type to filter (default "NB" for new building)
        search_type: Type of search - "bin" or "bbl"
        limit: Maximum number of records to retrieve

    Returns:
        DataFrame with matching records
    """
    print(f"\nQuerying API: {url}")
    print(f"Looking for job type: {job_type}")
    print(f"Search type: {search_type}")
    print(f"Number of items to check: {len(search_list)}")

    all_results = []

    # Query in batches to avoid URL length limits
    batch_size = 50
    for i in range(0, len(search_list), batch_size):
        batch = search_list[i:i+batch_size]

        if search_type == "bin":
            # Original BIN-based search
            bin_column = "bin__" if "ic3t-wcy2" in url else "bin"  # DOB vs DOB NOW
            bin_filter = " OR ".join([f"{bin_column}='{bin_num}'" for bin_num in batch])
            query = f"job_type='{job_type}' AND ({bin_filter})"
        elif search_type == "bbl":
            # BBL-based search using borough, block, lot
            bbl_filters = []
            for bbl_tuple in batch:
                if bbl_tuple and len(bbl_tuple) == 3:
                    borough, block, lot = bbl_tuple
                    bbl_filters.append(f"borough='{borough}' AND block={block} AND lot={lot}")
                else:
                    continue
            if bbl_filters:
                query = f"job_type='{job_type}' AND ({' OR '.join(bbl_filters)})"
            else:
                continue
        else:
            continue

        params = {
            '$where': query,
            '$limit': limit
        }

        try:
            print(f"  Querying batch {i//batch_size + 1} (Items {i+1}-{min(i+batch_size, len(search_list))})...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data:
                all_results.extend(data)
                print(f"    Found {len(data)} records")
            else:
                print(f"    No records found")

            # Rate limiting
            time.sleep(0.5)

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
        print(f"\nNo records found")
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
    try:
        search_df = pd.read_csv(search_file_path)
        print(f"Reading search data from CSV: {search_file_path}")

        # Extract BINs and BBLs
        bins = []
        bbl_tuples = []

        if 'BIN_normalized' in search_df.columns:
            bins = [str(b).replace('.0', '') for b in search_df['BIN_normalized'].dropna() if str(b) != 'nan']
        elif 'BIN' in search_df.columns:
            bins = [str(b).replace('.0', '') for b in search_df['BIN'].dropna() if str(b) != 'nan']

        if use_bbl_fallback and 'BBL' in search_df.columns:
            bbl_values = search_df['BBL'].dropna()
            for bbl in bbl_values:
                bbl_tuple = decompose_bbl(bbl)
                if bbl_tuple[0] is not None:  # Valid BBL
                    bbl_tuples.append(bbl_tuple)

        print(f"Found {len(bins)} BINs and {len(bbl_tuples)} BBLs to search")

    except:
        # Fall back to reading as text file with BINs
        print(f"Reading BINs from text file: {search_file_path}")
        with open(search_file_path, 'r') as f:
            bins = [line.strip() for line in f if line.strip() and not line.startswith('#')]

        # Convert to integers and remove duplicates
        bins = sorted(list(set([bin_str for bin_str in bins if bin_str and bin_str != 'nan'])))
        bbl_tuples = []
        print(f"Found {len(bins)} unique BINs\n")

    # Query DOB Job Application Filings API
    print("=" * 70)
    print("QUERYING DOB JOB APPLICATION FILINGS")
    print("=" * 70)
    dob_filings = query_dob_api(DOB_JOB_APPLICATIONS_URL, bins, job_type="NB", search_type="bin")

    # Query DOB NOW Job Applications API
    print("\n" + "=" * 70)
    print("QUERYING DOB NOW JOB APPLICATIONS")
    print("=" * 70)
    dob_now_filings = query_dob_api(DOB_NOW_JOB_APPLICATIONS_URL, bins, job_type="New Building", search_type="bin")

    # If we have BBLs and want fallback, search by BBL as well
    if use_bbl_fallback and bbl_tuples:
        print("\n" + "=" * 70)
        print("QUERYING DOB APIs BY BBL (FALLBACK)")
        print("=" * 70)
        print(f"Searching {len(bbl_tuples)} BBLs that don't have BIN matches...")

        dob_filings_bbl = query_dob_api(DOB_JOB_APPLICATIONS_URL, bbl_tuples, job_type="NB", search_type="bbl")
        dob_now_filings_bbl = query_dob_api(DOB_NOW_JOB_APPLICATIONS_URL, bbl_tuples, job_type="New Building", search_type="bbl")

        # Combine BBL results with BIN results
        if not dob_filings_bbl.empty:
            dob_filings = pd.concat([dob_filings, dob_filings_bbl], ignore_index=True)
        if not dob_now_filings_bbl.empty:
            dob_now_filings = pd.concat([dob_now_filings, dob_now_filings_bbl], ignore_index=True)
    
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
    if output_path is None:
        output_path = bin_file_path.replace('.txt', '_dob_filings.csv')
    
    combined.to_csv(output_path, index=False)
    print(f"\nResults saved to: {output_path}")
    
    # Also create a summary by BIN
    if 'bin_normalized' in combined.columns:
        summary_path = bin_file_path.replace('.txt', '_dob_filings_summary.csv')
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

