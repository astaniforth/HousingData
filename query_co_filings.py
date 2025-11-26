import pandas as pd
import requests
import time
import sys
import os
from urllib.parse import quote

# NYC Open Data API endpoints for Certificate of Occupancy
DOB_NOW_CO_URL = "https://data.cityofnewyork.us/resource/pkdm-hqz6.json"
DOB_CO_URL = "https://data.cityofnewyork.us/resource/bs8b-p36w.json"

def query_co_api(url, bin_list, bin_column="bin", limit=50000):
    """
    Query CO API for certificate of occupancy data matching BINs.

    Args:
        url: API endpoint URL
        bin_list: List of BINs to search for
        bin_column: Column name for BIN (varies by API: "bin" or "bin_number")
        limit: Maximum number of records to retrieve

    Returns:
        DataFrame with matching records
    """
    print(f"\nQuerying CO API: {url}")
    print(f"BIN column: {bin_column}")
    print(f"Number of BINs to check: {len(bin_list)}")

    all_results = []

    # Query in batches to avoid URL length limits
    batch_size = 50
    for i in range(0, len(bin_list), batch_size):
        batch = bin_list[i:i+batch_size]

        # Build query: bin_column IN (list of bins)
        bin_filter = " OR ".join([f"{bin_column}='{bin_num}'" for bin_num in batch])
        query = f"({bin_filter})"

        params = {
            '$where': query,
            '$limit': limit
        }

        try:
            print(f"  Querying batch {i//batch_size + 1} (BINs {i+1}-{min(i+batch_size, len(bin_list))})...")
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

def query_co_filings(bin_file_path, output_path=None):
    """
    Query both CO APIs for certificate of occupancy data associated with BINs.

    Args:
        bin_file_path: Path to file containing BINs (one per line)
        output_path: Path to save results CSV
    """

    # Read BINs from file
    print(f"Reading BINs from: {bin_file_path}")
    with open(bin_file_path, 'r') as f:
        bins = [line.strip() for line in f if line.strip()]

    # Convert to integers and remove duplicates
    bins = sorted(list(set([int(bin_str) for bin_str in bins if bin_str.isdigit()])))
    print(f"Found {len(bins)} unique BINs\n")

    # Query DOB NOW Certificate of Occupancy API (uses bin column)
    print("=" * 70)
    print("QUERYING DOB NOW CERTIFICATE OF OCCUPANCY")
    print("=" * 70)
    dob_now_co = query_co_api(DOB_NOW_CO_URL, bins, bin_column="bin")

    # Query DOB Certificate Of Occupancy API (uses bin_number column)
    print("\n" + "=" * 70)
    print("QUERYING DOB CERTIFICATE OF OCCUPANCY")
    print("=" * 70)
    dob_co = query_co_api(DOB_CO_URL, bins, bin_column="bin_number")

    # Combine results
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    if not dob_now_co.empty:
        print(f"\nDOB NOW CO Filings: {len(dob_now_co)} records")
        print(f"Columns: {', '.join(dob_now_co.columns.tolist())}")
        # Add source column
        dob_now_co['source'] = 'DOB_NOW_CO'

    if not dob_co.empty:
        print(f"\nDOB CO Filings: {len(dob_co)} records")
        print(f"Columns: {', '.join(dob_co.columns.tolist())}")
        # Add source column
        dob_co['source'] = 'DOB_CO'

    # Normalize BIN columns before combining
    # DOB NOW CO uses "bin", DOB CO uses "bin_number"
    if not dob_now_co.empty and 'bin' in dob_now_co.columns:
        dob_now_co['bin_normalized'] = dob_now_co['bin'].astype(str)
    if not dob_co.empty and 'bin_number' in dob_co.columns:
        dob_co['bin_normalized'] = dob_co['bin_number'].astype(str)

    # Combine both dataframes - use all columns and fill missing with NaN
    if not dob_now_co.empty and not dob_co.empty:
        # Get all unique columns
        all_cols = list(set(dob_now_co.columns.tolist() + dob_co.columns.tolist()))

        # Ensure bin_normalized and source are included
        if 'bin_normalized' not in all_cols:
            all_cols.append('bin_normalized')
        if 'source' not in all_cols:
            all_cols.append('source')

        # Reindex both dataframes to have the same columns
        dob_now_co_aligned = dob_now_co.reindex(columns=all_cols)
        dob_co_aligned = dob_co.reindex(columns=all_cols)

        combined = pd.concat([dob_now_co_aligned, dob_co_aligned], ignore_index=True)
    elif not dob_now_co.empty:
        combined = dob_now_co.copy()
        if 'bin' in combined.columns and 'bin_normalized' not in combined.columns:
            combined['bin_normalized'] = combined['bin'].astype(str)
    elif not dob_co.empty:
        combined = dob_co.copy()
        if 'bin_number' in combined.columns and 'bin_normalized' not in combined.columns:
            combined['bin_normalized'] = combined['bin_number'].astype(str)
    else:
        print("\nNo CO filings found in either API")
        return

    print(f"\nTotal combined records: {len(combined)}")

    # Show unique BINs found
    if 'bin_normalized' in combined.columns:
        unique_bins = combined['bin_normalized'].dropna().unique()
        print(f"Unique BINs with CO filings: {len(unique_bins)}")
        try:
            bin_nums = sorted([int(b) for b in unique_bins if str(b).strip() and str(b).isdigit()])[:20]
            print(f"Sample BINs with CO filings: {bin_nums}...")
        except:
            print(f"Sample BINs: {list(unique_bins)[:20]}")

    # Save results
    if output_path is None:
        output_path = bin_file_path.replace('.txt', '_co_filings.csv')

    combined.to_csv(output_path, index=False)
    print(f"\nResults saved to: {output_path}")

    # Also create a summary by BIN with first CO date
    if 'bin_normalized' in combined.columns:
        summary_path = bin_file_path.replace('.txt', '_co_filings_summary.csv')

        # Function to get earliest CO date for each BIN
        def get_earliest_co_date(group):
            # Try different date column names depending on source
            date_cols = []
            if group['source'].iloc[0] == 'DOB_NOW_CO':
                date_cols = ['c_of_o_issuance_date']
            else:  # DOB_CO
                date_cols = ['c_o_issue_date']

            earliest_date = None
            for col in date_cols:
                if col in group.columns:
                    dates = pd.to_datetime(group[col], errors='coerce')
                    valid_dates = dates.dropna()
                    if len(valid_dates) > 0:
                        current_earliest = valid_dates.min()
                        if earliest_date is None or current_earliest < earliest_date:
                            earliest_date = current_earliest

            return earliest_date

        # Group by BIN and get summary
        summary_data = []
        for bin_val, group in combined.groupby('bin_normalized'):
            earliest_co = get_earliest_co_date(group)
            summary_data.append({
                'BIN': bin_val,
                'Number_of_CO_Filings': len(group),
                'First_CO_Date': earliest_co.strftime('%Y-%m-%d') if earliest_co else None,
                'CO_Sources': ', '.join(group['source'].unique())
            })

        summary = pd.DataFrame(summary_data)
        summary = summary.sort_values('Number_of_CO_Filings', ascending=False)
        summary.to_csv(summary_path, index=False)
        print(f"Summary by BIN saved to: {summary_path}")

    # Show sample records
    print("\n" + "=" * 70)
    print("SAMPLE RECORDS")
    print("=" * 70)
    if len(combined) > 0:
        # Show first few columns that are likely most relevant
        sample_cols = ['bin_normalized', 'source']
        if 'bin' in combined.columns:
            sample_cols.insert(0, 'bin')
        elif 'bin_number' in combined.columns:
            sample_cols.insert(0, 'bin_number')

        # Add date columns if available
        date_cols = [col for col in combined.columns if 'date' in col.lower() or 'issuance' in col.lower() or 'issue' in col.lower()]
        sample_cols.extend(date_cols[:2])  # Add up to 2 date columns

        # Add other relevant columns
        if 'job_filing_name' in combined.columns:
            sample_cols.append('job_filing_name')
        if 'job_number' in combined.columns:
            sample_cols.append('job_number')
        if 'c_of_o_status' in combined.columns:
            sample_cols.append('c_of_o_status')
        if 'application_status_raw' in combined.columns:
            sample_cols.append('application_status_raw')

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
            print("Usage: python query_co_filings.py <path_to_bin_file>")
            sys.exit(1)

    if not os.path.exists(bin_file):
        print(f"Error: File '{bin_file}' not found.")
        sys.exit(1)

    query_co_filings(bin_file)
