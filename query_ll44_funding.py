import pandas as pd
import requests
import time
import sys
import os
from urllib.parse import quote

# NYC Open Data API endpoint for Local Law 44 funding
LL44_FUNDING_URL = "https://data.cityofnewyork.us/resource/gmi7-62cd.json"

def query_ll44_funding(project_ids, limit=50000):
    """
    Query Local Law 44 funding database for project financing information.

    Args:
        project_ids: List of project IDs to check for funding
        limit: Maximum number of records to retrieve

    Returns:
        DataFrame with funding information
    """
    print(f"\nQuerying Local Law 44 funding database...")
    print(f"Number of project IDs to check: {len(project_ids)}")

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

def add_financing_type(hpd_csv_path, ll44_funding_df, output_path=None):
    """
    Add financing type column to HPD data based on Local Law 44 funding.

    Args:
        hpd_csv_path: Path to HPD data CSV
        ll44_funding_df: DataFrame with LL44 funding data
        output_path: Path to save updated CSV

    Returns:
        DataFrame with added financing type column
    """

    print(f"Reading HPD data from: {hpd_csv_path}")
    df_hpd = pd.read_csv(hpd_csv_path)
    print(f"Total HPD projects: {len(df_hpd)}")

    # Get unique project IDs from HPD data
    project_ids = df_hpd['Project ID'].dropna().astype(str).unique()
    print(f"Unique project IDs in HPD data: {len(project_ids)}")

    # Get project IDs that have LL44 funding
    if not ll44_funding_df.empty:
        funded_project_ids = set(ll44_funding_df['projectid'].dropna().astype(str).unique())
        print(f"Project IDs with LL44 funding: {len(funded_project_ids)}")
    else:
        funded_project_ids = set()
        print("No LL44 funding data found")

    # Add financing type column
    def get_financing_type(project_id):
        if pd.isna(project_id):
            return 'Unknown'
        if str(project_id) in funded_project_ids:
            return 'HPD Financed'
        else:
            return 'Privately Financed'

    df_hpd['Financing Type'] = df_hpd['Project ID'].apply(get_financing_type)

    # Summary
    financing_counts = df_hpd['Financing Type'].value_counts()
    print("\nFinancing type distribution:")
    for financing_type, count in financing_counts.items():
        print(f"  {financing_type}: {count:,} projects")

    if output_path is None:
        output_path = hpd_csv_path.replace('.csv', '_with_financing.csv')

    df_hpd.to_csv(output_path, index=False)
    print(f"\nUpdated HPD data saved to: {output_path}")

    return df_hpd

def query_and_add_financing(hpd_csv_path, output_path=None):
    """
    Query LL44 funding and add financing type to HPD data.

    Args:
        hpd_csv_path: Path to HPD data CSV
        output_path: Path to save updated CSV
    """

    # Read HPD data to get project IDs
    print(f"Reading HPD data to extract project IDs: {hpd_csv_path}")
    df_hpd = pd.read_csv(hpd_csv_path)
    project_ids = df_hpd['Project ID'].dropna().astype(str).unique().tolist()
    print(f"Found {len(project_ids)} unique project IDs")

    # Query LL44 funding database
    ll44_funding = query_ll44_funding(project_ids)

    # Add financing type to HPD data
    updated_hpd = add_financing_type(hpd_csv_path, ll44_funding, output_path)

    return updated_hpd

if __name__ == "__main__":
    if len(sys.argv) > 1:
        hpd_csv = sys.argv[1]
    else:
        hpd_csv = 'Affordable_Housing_Production_by_Building.csv'

    if not os.path.exists(hpd_csv):
        print(f"Error: HPD CSV '{hpd_csv}' not found.")
        sys.exit(1)

    query_and_add_financing(hpd_csv)
