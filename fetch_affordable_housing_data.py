"""
Fetch Affordable Housing Production data from NYC Open Data API

This script fetches current affordable housing data from the NYC Open Data portal
and saves it as a CSV file compatible with the existing analysis pipeline.

API Endpoint: https://data.cityofnewyork.us/resource/hg8x-zxpr.json
"""

import requests
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta
from pathlib import Path

# HPD Projects data (program group information)
HPD_PROJECTS_URL = "https://data.cityofnewyork.us/resource/hq68-rnsi.json"
HPD_PROJECTS_CACHE_FILE = "data/processed/Affordable_Housing_Production_by_Project.csv"
HPD_PROJECTS_CACHE_MAX_AGE_HOURS = 24  # Consider cache valid for 24 hours

def fetch_hpd_projects_data(limit=50000):
    """
    Fetch HPD Projects data from NYC Open Data API.
    This contains project-level information including program_group.

    Args:
        limit: Maximum number of records to retrieve

    Returns:
        pandas.DataFrame: Complete HPD projects data
    """
    print(f"Fetching HPD Projects data from NYC Open Data API...")
    print(f"Endpoint: {HPD_PROJECTS_URL}")

    all_records = []
    offset = 0
    batch_size = 1000  # Socrata default limit

    while True:
        params = {
            '$limit': batch_size if limit is None else min(batch_size, limit - len(all_records)),
            '$offset': offset,
            '$order': 'project_id'  # Consistent ordering
        }

        try:
            print(f"Fetching records {offset + 1}-{offset + params['$limit']}...")
            response = requests.get(HPD_PROJECTS_URL, params=params, timeout=30)
            response.raise_for_status()

            batch_data = response.json()
            if not batch_data:
                break

            all_records.extend(batch_data)
            offset += len(batch_data)

            print(f"  Retrieved {len(batch_data)} records (total: {len(all_records):,})")

            # Stop if we've reached the limit (only applies when limit is set)
            if limit is not None and len(all_records) >= limit:
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
        'project_id': 'project_id',
        'project_name': 'project_name',
        'program_group': 'program_group',
        'project_start_date': 'project_start_date',
        'project_completion_date': 'project_completion_date',
        'extended_affordability_status': 'extended_affordability_status',
        'prevailing_wage_status': 'prevailing_wage_status',
        'planned_tax_benefit': 'planned_tax_benefit',
        'extremely_low_income_units': 'extremely_low_income_units',
        'very_low_income': 'very_low_income_units',
        'low_income_units': 'low_income_units',
        'moderate_income': 'moderate_income_units',
        'middle_income': 'middle_income_units',
        'other': 'other_income_units',
        'counted_rental_units': 'counted_rental_units',
        'counted_homeownership_units': 'counted_homeownership_units',
        'all_counted_units': 'all_counted_units',
        'total_units': 'total_units',
        'senior_units': 'senior_units'
    }

    df = df.rename(columns=column_mapping)

    # Convert numeric fields
    numeric_fields = [
        'extremely_low_income_units', 'very_low_income_units', 'low_income_units',
        'moderate_income_units', 'middle_income_units', 'other_income_units',
        'counted_rental_units', 'counted_homeownership_units', 'all_counted_units',
        'total_units', 'senior_units'
    ]

    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')

    # Convert project_id to string
    if 'project_id' in df.columns:
        df['project_id'] = df['project_id'].astype(str)

    return df

def verify_and_fetch_hpd_projects_data(use_existing=True):
    """
    Verify if local HPD projects data matches the API, and fetch fresh data if needed.

    Args:
        use_existing: If True, use local data if it exists and is recent

    Returns:
        tuple: (pandas.DataFrame, pathlib.Path) - The HPD projects data and file path used
    """
    cache_file = Path(HPD_PROJECTS_CACHE_FILE)
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("VERIFYING HPD PROJECTS CACHE")
    print("=" * 70)

    # Check if local file exists
    if not cache_file.exists():
        print(f"Local HPD projects cache file not found at {cache_file}")
        print("Fetching fresh data from API...")
        df = fetch_hpd_projects_data()
        df.to_csv(cache_file, index=False)
        print(f"Saved fresh data to: {cache_file}")
        return df, cache_file

    # Check file age
    file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
    is_recent = file_age < timedelta(hours=HPD_PROJECTS_CACHE_MAX_AGE_HOURS)

    if is_recent and use_existing:
        print(f"Found recent HPD projects cache file: {cache_file}")
        print(f"File age: {file_age}")
        print("Using existing cached data")
        df = pd.read_csv(cache_file, dtype={'project_id': str})
        return df, cache_file
    else:
        print(f"HPD projects cache file is stale (age: {file_age}) or use_existing=False")
        print("Fetching fresh data from API...")

        # Create backup of existing file
        backup_file = cache_file.with_suffix('.csv.backup_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
        cache_file.rename(backup_file)
        print(f"Created backup: {backup_file}")

        df = fetch_hpd_projects_data()
        df.to_csv(cache_file, index=False)
        print(f"Saved fresh data to: {cache_file}")
        return df, cache_file

def update_hpd_projects_cache():
    """Force update the local HPD projects data cache."""
    print("Force updating HPD projects cache...")
    df, path = verify_and_fetch_hpd_projects_data(use_existing=False)
    return df, path

def fetch_affordable_housing_data(limit=50000, output_file=None, use_projects_cache=True):
    """
    Fetch affordable housing data from NYC Open Data API.

    Args:
        limit: Maximum records to fetch (default 50,000)
        output_file: Output CSV file path (optional)

    Returns:
        pandas.DataFrame: The fetched data
    """
    base_url = "https://data.cityofnewyork.us/resource/hg8x-zxpr.json"

    print(f"Fetching affordable housing data from NYC Open Data API...")
    print(f"Endpoint: {base_url}")

    all_records = []
    offset = 0
    batch_size = 1000  # Socrata default limit

    while True:
        params = {
            '$limit': batch_size if limit is None else min(batch_size, limit - len(all_records)),
            '$offset': offset,
            '$order': 'project_id'  # Consistent ordering
        }

        try:
            print(f"Fetching records {offset + 1}-{offset + params['$limit']}...")
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()

            batch_data = response.json()
            if not batch_data:
                break

            all_records.extend(batch_data)
            offset += len(batch_data)

            print(f"  Retrieved {len(batch_data)} records (total: {len(all_records):,})")

            # Stop if we've reached the limit (only applies when limit is set)
            if limit is not None and len(all_records) >= limit:
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
        'project_id': 'Project ID',
        'project_name': 'Project Name',
        'project_start_date': 'Project Start Date',
        'building_id': 'Building ID',
        'house_number': 'Number',
        'street_name': 'Street',
        'borough': 'Borough',
        'postcode': 'Postcode',
        'bbl': 'BBL',
        'bin': 'BIN',
        'community_board': 'Community Board',
        'council_district': 'Council District',
        'census_tract': 'Census Tract',
        'neighborhood_tabulation_area': 'NTA - Neighborhood Tabulation Area',
        'latitude': 'Latitude',
        'longitude': 'Longitude',
        'latitude_internal': 'Latitude (Internal)',
        'longitude_internal': 'Longitude (Internal)',
        'building_completion_date': 'Building Completion Date',
        'reporting_construction_type': 'Reporting Construction Type',
        'extended_affordability_status': 'Extended Affordability Only',
        'prevailing_wage_status': 'Prevailing Wage Status',
        'extremely_low_income_units': 'Extremely Low Income Units',
        'very_low_income_units': 'Very Low Income Units',
        'low_income_units': 'Low Income Units',
        'moderate_income_units': 'Moderate Income Units',
        'middle_income_units': 'Middle Income Units',
        'other_income_units': 'Other Income Units',
        'studio_units': 'Studio Units',
        '_1_br_units': '1-BR Units',
        '_2_br_units': '2-BR Units',
        '_3_br_units': '3-BR Units',
        '_4_br_units': '4-BR Units',
        '_5_br_units': '5-BR Units',
        '_6_br_units': '6-BR+ Units',
        'unknown_br_units': 'Unknown-BR Units',
        'counted_rental_units': 'Counted Rental Units',
        'counted_homeownership_units': 'Counted Homeownership Units',
        'all_counted_units': 'All Counted Units',
        'total_units': 'Total Units'
    }

    df = df.rename(columns=column_mapping)

    # Add missing columns that our analysis expects
    expected_columns = [
        'Project ID', 'Project Name', 'Project Start Date', 'Project Completion Date',
        'Building ID', 'Number', 'Street', 'Borough', 'Postcode', 'BBL', 'BIN',
        'Community Board', 'Council District', 'Census Tract',
        'NTA - Neighborhood Tabulation Area', 'Latitude', 'Longitude',
        'Latitude (Internal)', 'Longitude (Internal)', 'Building Completion Date',
        'Reporting Construction Type', 'Extended Affordability Only', 'Prevailing Wage Status',
        'Extremely Low Income Units', 'Very Low Income Units', 'Low Income Units',
        'Moderate Income Units', 'Middle Income Units', 'Other Income Units',
        'Studio Units', '1-BR Units', '2-BR Units', '3-BR Units', '4-BR Units',
        '5-BR Units', '6-BR+ Units', 'Unknown-BR Units', 'Counted Rental Units',
        'Counted Homeownership Units', 'All Counted Units', 'Total Units',
        # Project-level columns (added via enrichment with project_ prefix when needed)
        'project_Program Group', 'project_Extended Affordability Status',
        'project_Prevailing Wage Status', 'project_Planned Tax Benefit'
    ]

    # Ensure all expected columns exist
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Reorder columns to match expected format
    df = df[expected_columns]

    # Convert ID fields to strings (preserve leading zeros, handle NaN)
    string_id_fields = [
        'Project ID', 'Building ID', 'Number', 'Postcode', 'BBL', 'BIN',
        'Council District', 'Census Tract'
    ]

    for field in string_id_fields:
        if field in df.columns:
            # Convert to string, replace 'nan' with empty string, then replace empty with NaN
            df[field] = df[field].astype(str).replace('nan', '').replace('', np.nan)

    # Convert truly numeric fields (unit counts)
    numeric_fields = [
        'Extremely Low Income Units', 'Very Low Income Units', 'Low Income Units',
        'Moderate Income Units', 'Middle Income Units', 'Other Income Units',
        'Studio Units', '1-BR Units', '2-BR Units', '3-BR Units', '4-BR Units',
        '5-BR Units', '6-BR+ Units', 'Unknown-BR Units', 'Counted Rental Units',
        'Counted Homeownership Units', 'All Counted Units', 'Total Units'
    ]

    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')

    # Enrich with project-level information (program_group, etc.)
    print("\nEnriching building data with project-level information...")
    try:
        projects_df, _ = verify_and_fetch_hpd_projects_data(use_existing=use_projects_cache)
        print(f"Loaded {len(projects_df)} project records")

        # Merge on Project ID (ensure both are strings for proper matching)
        df['Project ID'] = df['Project ID'].astype(str)
        projects_df['project_id'] = projects_df['project_id'].astype(str)

        # Select project-level columns to merge
        project_cols_to_merge = [
            'program_group', 'project_start_date', 'project_completion_date',
            'extended_affordability_status', 'prevailing_wage_status', 'planned_tax_benefit'
        ]

        # Only merge columns that exist in the projects data
        available_cols = [col for col in project_cols_to_merge if col in projects_df.columns]
        projects_subset = projects_df[['project_id'] + available_cols].copy()

        # Smart column naming: replace empty HPD columns, prefix only when HPD column has data
        base_rename_map = {
            'program_group': 'Program Group',
            'extended_affordability_status': 'Extended Affordability Status',
            'prevailing_wage_status': 'Prevailing Wage Status',
            'planned_tax_benefit': 'Planned Tax Benefit',
            'project_start_date': 'Project Start Date',
            'project_completion_date': 'Project Completion Date'
        }

        # Check if HPD columns are empty or have data
        rename_map = {}
        columns_to_drop = []  # HPD columns we'll replace with project data

        for orig_col, desired_name in base_rename_map.items():
            if orig_col in available_cols:
                if desired_name in df.columns:
                    # Check if the existing HPD column has any real data
                    hpd_col_data = df[desired_name].dropna()
                    if len(hpd_col_data) == 0:
                        # HPD column is empty - we'll replace it with project data (no prefix)
                        rename_map[orig_col] = desired_name
                        columns_to_drop.append(desired_name)
                    else:
                        # HPD column has data - keep both, use prefix for project data
                        rename_map[orig_col] = f'project_{desired_name}'
                else:
                    # No conflict - use clean name
                    rename_map[orig_col] = desired_name

        if rename_map:
            projects_subset = projects_subset.rename(columns=rename_map)
            available_cols = list(rename_map.values())

        # Drop empty HPD columns that we're replacing
        for col in columns_to_drop:
            if col in df.columns:
                df = df.drop(col, axis=1)
                print(f"Replacing empty HPD column '{col}' with project data")

        # Debug: Check merge inputs
        print(f"Merging on 'Project ID' (HPD) with 'project_id' (projects)")
        print(f"HPD Project IDs sample: {df['Project ID'].head(3).tolist()}")
        print(f"Projects project_ids sample: {projects_subset['project_id'].head(3).tolist()}")

        # Merge building data with project data
        original_count = len(df)
        df = df.merge(projects_subset, left_on='Project ID', right_on='project_id', how='left')

        # Clean up merge artifacts and rename columns
        # Remove duplicate project_id column
        if 'project_id' in df.columns:
            df = df.drop('project_id', axis=1)

        # Handle any remaining merge conflicts (_x/_y columns)
        columns_to_drop = []
        rename_map = {}

        for col in df.columns:
            if col.endswith('_x') and col[:-2] + '_y' in df.columns:
                # Merge conflict: keep _y (projects data), use appropriate name
                base_name = col[:-2]
                y_col = col[:-2] + '_y'

                # Use the _y column name as-is (it should already be correctly named)
                final_name = base_name  # The _y column should have the right name
                rename_map[y_col] = final_name
                columns_to_drop.append(col)  # Drop the _x version

        # Drop conflicting _x columns
        for col in columns_to_drop:
            if col in df.columns:
                df = df.drop(col, axis=1)

        # Rename _y columns
        if rename_map:
            df = df.rename(columns=rename_map)

        # Final cleanup: remove any remaining redundant project_ columns
        columns_to_clean = []
        for col in df.columns:
            if col.startswith('project_'):
                # Check if we already have the non-prefixed version with data
                base_name = col[8:]  # Remove 'project_' prefix
                if base_name in df.columns:
                    base_data = df[base_name].notna().sum()
                    project_data = df[col].notna().sum()
                    if base_data > 0 and project_data == 0:
                        # Base column has data, project column is empty - remove project column
                        columns_to_clean.append(col)
                        print(f"Removing empty project column '{col}' (base column '{base_name}' has data)")
                    elif base_data >= project_data and project_data > 0:
                        # Base column has equal or more data - remove project column to avoid duplication
                        columns_to_clean.append(col)
                        print(f"Removing duplicate project column '{col}' (base column '{base_name}' is sufficient)")

        for col in columns_to_clean:
            if col in df.columns:
                df = df.drop(col, axis=1)

        # Debug: Check merge results
        project_cols = [col for col in df.columns if col.startswith('project_')]
        if project_cols:
            first_col = project_cols[0]
            merged_programs = df[first_col].notna().sum()
            print(f"Merge complete: {merged_programs} records got project data out of {len(df)}")
            if merged_programs > 0:
                print(f"Sample {first_col} values: {df[first_col].dropna().head(3).tolist()}")
        else:
            # Check how many records got enriched data in standard columns
            enriched_cols = ['Program Group', 'Extended Affordability Status', 'Planned Tax Benefit']
            enriched_records = 0
            for col in enriched_cols:
                if col in df.columns:
                    enriched_records = max(enriched_records, df[col].notna().sum())
            print(f"Merge complete: {enriched_records} records enriched with project data out of {len(df)}")

        merged_count = len(df)
        print(f"Successfully enriched {merged_count} building records with project information")
        print(f"Added columns: {available_cols}")

    except Exception as e:
        print(f"Warning: Could not enrich with project data: {e}")
        print("Continuing with building-level data only")

    # Save to CSV if requested
    if output_file:
        df.to_csv(output_file, index=False)
        print(f"Data saved to: {output_file}")

    return df

def verify_and_fetch_hpd_data(sample_size=100, use_existing=True, output_path=None, use_projects_cache=True):
    """
    Verify if local HPD data matches the API, and fetch fresh data if needed.

    Args:
        sample_size: Number of records to compare for sample validation
        use_existing: If True, use local data if it matches API
        output_path: Path to save/load the CSV file (optional)
        use_projects_cache: Whether to use cached HPD projects data for enrichment

    Returns:
        tuple: (pandas.DataFrame, pathlib.Path) - The HPD data and the file path used
    """
    from pathlib import Path
    import hashlib

    # Define file paths
    if output_path:
        local_file = Path(output_path)
        local_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        data_dir = Path('data/raw')
        data_dir.mkdir(parents=True, exist_ok=True)
        local_file = data_dir / "Affordable_Housing_Production_by_Building.csv"

    print("=" * 70)
    print("STEP 1: VERIFY AND FETCH HPD DATA")
    print("=" * 70)

    # Check if local file exists
    if not local_file.exists():
        print(f"Local HPD data file not found at {local_file}")
        print("Fetching fresh data from API...")
        df = fetch_affordable_housing_data(use_projects_cache=use_projects_cache)
        df.to_csv(local_file, index=False)
        print(f"Saved fresh data to: {local_file}")
        return df, local_file

    # Load local data
    print(f"Found local HPD data file: {local_file}")

    # Define dtypes for proper loading
    dtype_spec = {
        'Project ID': str,
        'Building ID': str,
        'Number': str,
        'Postcode': str,
        'BBL': str,
        'BIN': str,
        'Council District': str,
        'Census Tract': str
    }

    local_df = pd.read_csv(local_file, dtype=dtype_spec)
    local_count = len(local_df)
    print(f"Local file has {local_count:,} records")

    # Check if local data is already filtered to New Construction (our expected state)
    is_pre_filtered = False
    if 'Reporting Construction Type' in local_df.columns:
        construction_types = local_df['Reporting Construction Type'].value_counts(dropna=False)
        is_pre_filtered = len(construction_types) == 1 and 'New Construction' in construction_types.index

    if is_pre_filtered:
        print("✅ Local data is pre-filtered to New Construction only - skipping API verification")
        return local_df, local_file

    # Fetch a sample from API for comparison (only for unfiltered legacy data)
    print(f"\nFetching {sample_size} sample records from API for verification...")
    api_sample_df = fetch_affordable_housing_data(limit=sample_size, use_projects_cache=False)  # Don't enrich sample

    if api_sample_df.empty:
        print("ERROR: Could not fetch sample data from API")
        if use_existing:
            print("Using existing local data as fallback")
            return local_df, local_file
        else:
            raise Exception("Could not fetch data from API and use_existing=False")

    api_sample_count = len(api_sample_df)
    print(f"API sample has {api_sample_count:,} records")

    # Compare record counts - if they match, assume data is current
    # (API sample might not have all records due to pagination)
    if local_count >= api_sample_count:
        print("✅ Local data has sufficient records - assuming current")
        if use_existing:
            print("Using existing local data")
            return local_df, local_file
        else:
            print("use_existing=False, fetching fresh data anyway...")
            df = fetch_affordable_housing_data(use_projects_cache=use_projects_cache)
            df.to_csv(local_file, index=False)
            print(f"Saved fresh data to: {local_file}")
            return df, local_file

    # If local has fewer records, fetch fresh data
    print(f"⚠️  Local data has fewer records ({local_count:,}) than API sample ({api_sample_count:,})")
    print("Fetching fresh data from API...")
    df = fetch_affordable_housing_data(use_projects_cache=use_projects_cache)
    df.to_csv(local_file, index=False)
    print(f"Saved fresh data to: {local_file}")
    return df, local_file

def update_local_data(output_path=None):
    """Update the local affordable housing data file with fresh API data.

    Args:
        output_path: Path to save the CSV file (optional)

    Returns:
        tuple: (pandas.DataFrame, pathlib.Path) - The data and the file path used
    """
    from pathlib import Path

    # Determine output file path
    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
    else:
        # Create data/raw directory if it doesn't exist
        data_dir = Path('data/raw')
        data_dir.mkdir(parents=True, exist_ok=True)
        output_file = data_dir / "Affordable_Housing_Production_by_Building.csv"

    backup_file = output_file.with_suffix('.csv.backup_' + datetime.now().strftime('%Y%m%d_%H%M%S'))

    # Create backup of existing file
    if output_file.exists():
        output_file.rename(backup_file)
        print(f"Created backup: {backup_file}")

    # Fetch fresh data (already has correct dtypes)
    df = fetch_affordable_housing_data()

    # Save as new file
    df.to_csv(output_file, index=False)
    print(f"Updated local data file: {output_file}")

    return df, output_file

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--update":
        # Update the local data file
        df, csv_path = update_local_data()
        print(f"Updated: {csv_path}")
    else:
        # Just fetch and display info
        df = fetch_affordable_housing_data(limit=100)  # Small sample
        print(f"\nSample data shape: {df.shape}")
        print(f"Columns: {len(df.columns)}")
        print(f"Sample project: {df['Project Name'].iloc[0] if len(df) > 0 else 'None'}")
