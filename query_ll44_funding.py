import pandas as pd
import requests
import time
import sys
import os
from urllib.parse import quote
from pathlib import Path
from datetime import datetime, timedelta

# NYC Open Data API endpoint for Local Law 44 funding
LL44_FUNDING_URL = "https://data.cityofnewyork.us/resource/gmi7-62cd.json"

# Local cache settings
LL44_FUNDING_CACHE_FILE = "data/raw/ll44_funding_data.csv"
LL44_FUNDING_CACHE_MAX_AGE_HOURS = 24  # Consider cache valid for 24 hours

# LL44 eligibility data (which projects are subject to LL44)
LL44_ELIGIBILITY_URL = "https://data.cityofnewyork.us/resource/ucdy-byxd.json"
LL44_ELIGIBILITY_CACHE_FILE = "data/raw/ll44_eligibility_data.csv"
LL44_ELIGIBILITY_CACHE_MAX_AGE_HOURS = 24  # Consider cache valid for 24 hours

def fetch_ll44_eligibility_data(limit=50000):
    """
    Fetch complete LL44 eligibility database from NYC Open Data API.
    This determines which projects are subject to Local Law 44.

    Args:
        limit: Maximum number of records to retrieve

    Returns:
        pandas.DataFrame: Complete LL44 eligibility data
    """
    print(f"Fetching LL44 eligibility data from NYC Open Data API...")
    print(f"Endpoint: {LL44_ELIGIBILITY_URL}")

    all_records = []
    offset = 0
    batch_size = 1000  # Socrata default limit

    while True:
        params = {
            '$limit': min(batch_size, limit - len(all_records)),
            '$offset': offset,
            '$order': 'projectid'  # Consistent ordering
        }

        try:
            print(f"Fetching records {offset + 1}-{offset + params['$limit']}...")
            response = requests.get(LL44_ELIGIBILITY_URL, params=params, timeout=30)
            response.raise_for_status()

            batch_data = response.json()
            if not batch_data:
                break

            all_records.extend(batch_data)
            offset += len(batch_data)

            print(f"  Retrieved {len(batch_data)} records (total: {len(all_records):,})")

            # Stop if we've reached the limit
            if len(all_records) >= limit:
                break

            # Rate limiting
            time.sleep(0.2)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

    print(f"\nCompleted! Retrieved {len(all_records):,} total records")

    # Convert to DataFrame
    df = pd.DataFrame(all_records)

    # Clean up column names to match our expected format
    # Using LL44 Projects dataset (ucdy-byxd.json) instead of Building dataset
    column_mapping = {
        'projectid': 'projectid',
        'projectdwid': 'projectdwid',
        'projectname': 'projectname',
        'programname': 'programname',
        'startdate': 'startdate',
        'projectedcompletiondate': 'projectedcompletiondate',
        'counted_rental_units': 'counted_rental_units',
        'counted_homeownership_units': 'counted_homeownership_units',
        'all_counted_units': 'all_counted_units',
        'totalprojectunits': 'total_units',
        'commercialsquarefootage': 'commercial_square_footage',
        'borrowerlegalentityname': 'borrower_legal_entity_name',
        'generalcontractorname': 'general_contractor_name',
        'isdavisbacon': 'is_davis_bacon',
        'issection220nyslaborlaw': 'is_section_220_nys_labor_law'
    }

    df = df.rename(columns=column_mapping)

    # Convert numeric fields
    numeric_fields = [
        'counted_rental_units', 'counted_homeownership_units', 'all_counted_units',
        'total_units', 'commercial_square_footage'
    ]

    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')

    # Convert ID fields to strings
    string_id_fields = ['projectid', 'projectdwid']
    for field in string_id_fields:
        if field in df.columns:
            df[field] = df[field].astype(str).replace('nan', '').replace('', pd.NA)

    return df

def verify_and_fetch_ll44_eligibility_data(use_existing=True):
    """
    Verify if local LL44 eligibility data matches the API, and fetch fresh data if needed.

    Args:
        use_existing: If True, use local data if it exists and is recent

    Returns:
        tuple: (pandas.DataFrame, pathlib.Path) - The LL44 eligibility data and file path used
    """
    cache_file = Path(LL44_ELIGIBILITY_CACHE_FILE)
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("VERIFYING LL44 ELIGIBILITY CACHE")
    print("=" * 70)

    # Check if local file exists
    if not cache_file.exists():
        print(f"Local LL44 eligibility cache file not found at {cache_file}")
        print("Fetching fresh data from API...")
        df = fetch_ll44_eligibility_data()
        df.to_csv(cache_file, index=False)
        print(f"Saved fresh data to: {cache_file}")
        return df, cache_file

    # Check file age
    file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
    is_recent = file_age < timedelta(hours=LL44_ELIGIBILITY_CACHE_MAX_AGE_HOURS)

    if is_recent and use_existing:
        print(f"Found recent LL44 eligibility cache file: {cache_file}")
        print(f"File age: {file_age}")
        print("Using existing cached data")
        df = pd.read_csv(cache_file, dtype={'projectid': str, 'buildingid': str, 'bin': str, 'bbl': str, 'postcode': str, 'council_district': str, 'census_tract': str})
        return df, cache_file
    else:
        print(f"LL44 eligibility cache file is stale (age: {file_age}) or use_existing=False")
        print("Fetching fresh data from API...")

        # Create backup of existing file
        backup_file = cache_file.with_suffix('.csv.backup_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
        cache_file.rename(backup_file)
        print(f"Created backup: {backup_file}")

        df = fetch_ll44_eligibility_data()
        df.to_csv(cache_file, index=False)
        print(f"Saved fresh data to: {cache_file}")
        return df, cache_file

def update_ll44_eligibility_cache():
    """Force update the local LL44 eligibility data cache."""
    print("Force updating LL44 eligibility cache...")
    df, path = verify_and_fetch_ll44_eligibility_data(use_existing=False)
    return df, path

def fetch_ll44_funding_data(limit=50000):
    """
    Fetch complete Local Law 44 funding database from NYC Open Data API.

    Args:
        limit: Maximum number of records to retrieve

    Returns:
        pandas.DataFrame: Complete LL44 funding data
    """
    print(f"Fetching Local Law 44 funding data from NYC Open Data API...")
    print(f"Endpoint: {LL44_FUNDING_URL}")

    all_records = []
    offset = 0
    batch_size = 1000  # Socrata default limit

    while True:
        params = {
            '$limit': min(batch_size, limit - len(all_records)),
            '$offset': offset,
            '$order': 'projectid'  # Consistent ordering
        }

        try:
            print(f"Fetching records {offset + 1}-{offset + params['$limit']}...")
            response = requests.get(LL44_FUNDING_URL, params=params, timeout=30)
            response.raise_for_status()

            batch_data = response.json()
            if not batch_data:
                break

            all_records.extend(batch_data)
            offset += len(batch_data)

            print(f"  Retrieved {len(batch_data)} records (total: {len(all_records):,})")

            # Stop if we've reached the limit
            if len(all_records) >= limit:
                break

            # Rate limiting
            time.sleep(0.2)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break

    print(f"\nCompleted! Retrieved {len(all_records):,} total records")

    # Convert to DataFrame
    df = pd.DataFrame(all_records)

    # Clean up column names to match our expected format
    column_mapping = {
        'projectid': 'projectid',
        'funding_source': 'funding_source',
        'funding_amount': 'funding_amount',
        'award_date': 'award_date',
        'fiscal_year': 'fiscal_year'
    }

    df = df.rename(columns=column_mapping)

    # Convert funding_amount to numeric
    if 'funding_amount' in df.columns:
        df['funding_amount'] = pd.to_numeric(df['funding_amount'], errors='coerce')

    return df

def verify_and_fetch_ll44_data(use_existing=True):
    """
    Verify if local LL44 data matches the API, and fetch fresh data if needed.

    Args:
        use_existing: If True, use local data if it exists and is recent

    Returns:
        tuple: (pandas.DataFrame, pathlib.Path) - The LL44 data and file path used
    """
    cache_file = Path(LL44_FUNDING_CACHE_FILE)
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("VERIFYING LL44 FUNDING CACHE")
    print("=" * 70)

    # Check if local file exists
    if not cache_file.exists():
        print(f"Local LL44 cache file not found at {cache_file}")
        print("Fetching fresh data from API...")
        df = fetch_ll44_funding_data()
        df.to_csv(cache_file, index=False)
        print(f"Saved fresh data to: {cache_file}")
        return df, cache_file

    # Check file age
    file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
    is_recent = file_age < timedelta(hours=LL44_FUNDING_CACHE_MAX_AGE_HOURS)

    if is_recent and use_existing:
        print(f"Found recent LL44 cache file: {cache_file}")
        print(f"File age: {file_age}")
        print("Using existing cached data")
        df = pd.read_csv(cache_file)
        return df, cache_file
    else:
        print(f"LL44 cache file is stale (age: {file_age}) or use_existing=False")
        print("Fetching fresh data from API...")

        # Create backup of existing file
        backup_file = cache_file.with_suffix('.csv.backup_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
        cache_file.rename(backup_file)
        print(f"Created backup: {backup_file}")

        df = fetch_ll44_funding_data()
        df.to_csv(cache_file, index=False)
        print(f"Saved fresh data to: {cache_file}")
        return df, cache_file

def update_ll44_cache():
    """Force update the local LL44 funding data cache."""
    print("Force updating LL44 funding cache...")
    df, path = verify_and_fetch_ll44_data(use_existing=False)
    return df, path

def query_ll44_funding(project_ids, limit=50000, use_cache=True):
    """
    Query Local Law 44 funding database for project financing information.

    Args:
        project_ids: List of project IDs to check for funding
        limit: Maximum number of records to retrieve
        use_cache: Whether to use local cache (default True)

    Returns:
        DataFrame with funding information for the requested project IDs
    """
    print(f"\nQuerying Local Law 44 funding database...")
    print(f"Number of project IDs to check: {len(project_ids)}")

    if use_cache:
        # Try to use cached data first
        try:
            print("Checking local LL44 cache...")
            ll44_df, cache_path = verify_and_fetch_ll44_data(use_existing=True)
            print(f"Loaded {len(ll44_df)} LL44 records from cache")

            # Filter for requested project IDs
            project_ids_set = set(str(pid) for pid in project_ids)
            filtered_df = ll44_df[ll44_df['projectid'].astype(str).isin(project_ids_set)]

            print(f"Found {len(filtered_df)} matching funding records in cache")
            return filtered_df

        except Exception as e:
            print(f"Cache query failed: {e}")
            print("Falling back to direct API query...")
            use_cache = False

    if not use_cache:
        # Direct API query (original implementation)
        all_results = []

        # Query in batches to avoid URL length limits
        batch_size = 50
        for i in range(0, len(project_ids), batch_size):
            batch = project_ids[i:i+batch_size]

            # Build query: projectid IN (list of project ids)
            project_filter = " OR ".join([f"projectid='{pid}'" for pid in batch])
            query = f"({project_filter})"

            params = {
                '$where': query,
                '$limit': limit
            }

            try:
                print(f"  Querying batch {i//batch_size + 1} (Project IDs {i+1}-{min(i+batch_size, len(project_ids))})...")
                response = requests.get(LL44_FUNDING_URL, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()
                if data:
                    all_results.extend(data)
                    print(f"    Found {len(data)} funding records")
                else:
                    print("    No funding records found")

                # Rate limiting
                time.sleep(0.5)

            except Exception as e:
                print(f"    Error querying batch: {str(e)}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"    Response: {e.response.text[:200]}")
                continue

        if all_results:
            df = pd.DataFrame(all_results)
            print(f"\nTotal funding records found: {len(df)}")
            return df
        else:
            print("No funding records found")
            return pd.DataFrame()

def add_financing_type(hpd_df, ll44_funding_df, output_path=None):
    """
    Add financing type column to HPD data based on Local Law 44 funding.
    HPD data should already be enriched with 'subject_to_ll44' column.

    Args:
        hpd_df: HPD DataFrame (already enriched with LL44 eligibility)
        ll44_funding_df: DataFrame with LL44 funding data
        output_path: Path to save updated CSV

    Returns:
        DataFrame with added financing type column
    """
    print(f"Adding financing types to {len(hpd_df)} HPD buildings...")

    # Get project IDs that have LL44 funding
    if not ll44_funding_df.empty:
        funded_project_ids = set(ll44_funding_df['projectid'].dropna().astype(str).unique())
        print(f"Project IDs with LL44 funding: {len(funded_project_ids)}")
    else:
        funded_project_ids = set()
        print("No LL44 funding data found")

    # Add financing type column
    def get_financing_type(row):
        project_id = row['Project ID']
        subject_to_ll44 = row['subject_to_ll44']

        if pd.isna(project_id):
            return 'Unknown'
        elif not subject_to_ll44:
            return 'Not Subject to LL44'
        elif str(project_id) in funded_project_ids:
            return 'HPD Financed'
        else:
            return 'Privately Financed'

    hpd_df = hpd_df.copy()
    hpd_df['Financing Type'] = hpd_df.apply(get_financing_type, axis=1)

    # Summary
    financing_counts = hpd_df['Financing Type'].value_counts()
    print("\nFinancing type distribution:")
    for financing_type, count in financing_counts.items():
        print(f"  {financing_type}: {count:,} projects ({count/len(hpd_df)*100:.1f}%)")

    if output_path is None:
        from pathlib import Path
        # Save to data/processed/ folder
        processed_dir = Path('data/processed')
        processed_dir.mkdir(parents=True, exist_ok=True)
        output_path = processed_dir / "Affordable_Housing_Production_by_Building_with_financing.csv"

    hpd_df.to_csv(output_path, index=False)
    print(f"\nUpdated HPD data saved to: {output_path}")

    return hpd_df

def enrich_with_ll44_eligibility(hpd_df, use_cache=True):
    """
    Enrich HPD data with LL44 eligibility information.
    Uses both eligibility and funding data to determine LL44 status.

    Args:
        hpd_df: HPD DataFrame to enrich
        use_cache: Whether to use local LL44 caches

    Returns:
        pandas.DataFrame: HPD data with added 'subject_to_ll44' column
    """
    print("\nEnriching HPD data with LL44 eligibility...")

    # Get LL44 eligibility data
    ll44_eligibility, _ = verify_and_fetch_ll44_eligibility_data(use_existing=use_cache)
    eligibility_project_ids = set(ll44_eligibility['projectid'].dropna().astype(str).unique())
    print(f"Loaded {len(ll44_eligibility)} LL44 eligible projects from eligibility dataset")

    # Also check LL44 funding data - if a project has LL44 funding, it's subject to LL44
    ll44_funding, _ = verify_and_fetch_ll44_data(use_existing=use_cache)
    funding_project_ids = set(ll44_funding['projectid'].dropna().astype(str).unique())
    print(f"Loaded {len(ll44_funding)} LL44 funding records from funding dataset")

    # Combine both sources - a project is subject to LL44 if it appears in either dataset
    all_ll44_project_ids = eligibility_project_ids.union(funding_project_ids)
    print(f"Combined: {len(all_ll44_project_ids)} unique projects subject to LL44")

    # Show breakdown
    eligibility_only = eligibility_project_ids - funding_project_ids
    funding_only = funding_project_ids - eligibility_project_ids
    both = eligibility_project_ids.intersection(funding_project_ids)

    print(f"  • In eligibility only: {len(eligibility_only)}")
    print(f"  • In funding only: {len(funding_only)} (like project 44225)")
    print(f"  • In both: {len(both)}")

    # Add subject_to_ll44 column to HPD data
    hpd_df = hpd_df.copy()
    hpd_df['subject_to_ll44'] = hpd_df['Project ID'].astype(str).isin(all_ll44_project_ids)

    # Summary
    ll44_count = hpd_df['subject_to_ll44'].sum()
    total_count = len(hpd_df)
    print(f"\nLL44 eligibility enrichment complete:")
    print(f"  Total HPD buildings: {total_count:,}")
    print(f"  Subject to LL44: {ll44_count:,} ({ll44_count/total_count*100:.1f}%)")
    print(f"  Not subject to LL44: {total_count - ll44_count:,} ({(total_count - ll44_count)/total_count*100:.1f}%)")

    return hpd_df

def query_and_add_financing(hpd_csv_path, output_path=None, use_cache=True, use_eligibility_cache=True):
    """
    Query LL44 funding and add financing type to HPD data.
    First enriches with LL44 eligibility, then queries funding only for eligible projects.

    Args:
        hpd_csv_path: Path to HPD data CSV
        output_path: Path to save updated CSV
        use_cache: Whether to use local LL44 funding cache (default True)
        use_eligibility_cache: Whether to use local LL44 eligibility cache (default True)
    """

    # Read HPD data
    print(f"Reading HPD data: {hpd_csv_path}")
    df_hpd = pd.read_csv(hpd_csv_path)
    print(f"Loaded {len(df_hpd)} HPD buildings")

    # Enrich with LL44 eligibility
    df_hpd = enrich_with_ll44_eligibility(df_hpd, use_cache=use_eligibility_cache)

    # Get project IDs that are subject to LL44
    ll44_project_ids = df_hpd[df_hpd['subject_to_ll44']]['Project ID'].dropna().astype(str).unique().tolist()
    print(f"\nQuerying funding for {len(ll44_project_ids)} LL44-eligible projects...")

    if not ll44_project_ids:
        print("No projects subject to LL44 found - skipping funding query")
        ll44_funding = pd.DataFrame()
    else:
        # Query LL44 funding database only for eligible projects (with caching)
        ll44_funding = query_ll44_funding(ll44_project_ids, use_cache=use_cache)

    # Add financing type to HPD data
    updated_hpd = add_financing_type(df_hpd, ll44_funding, output_path)

    return updated_hpd

if __name__ == "__main__":
    if len(sys.argv) > 1:
        hpd_csv = sys.argv[1]
    else:
        hpd_csv = 'data/raw/Affordable_Housing_Production_by_Building.csv'

    if not os.path.exists(hpd_csv):
        print(f"Error: HPD CSV '{hpd_csv}' not found.")
        sys.exit(1)

    query_and_add_financing(hpd_csv)
