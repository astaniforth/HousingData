"""
Fetch Affordable Housing Production data from NYC Open Data API

This script fetches current affordable housing data from the NYC Open Data portal
and saves it as a CSV file compatible with the existing analysis pipeline.

API Endpoint: https://data.cityofnewyork.us/resource/hg8x-zxpr.json
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime

def fetch_affordable_housing_data(limit=50000, output_file=None):
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
            '$limit': min(batch_size, limit - len(all_records)),
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
        'Counted Homeownership Units', 'All Counted Units', 'Total Units'
    ]

    # Ensure all expected columns exist
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Reorder columns to match expected format
    df = df[expected_columns]

    # Convert numeric fields
    numeric_fields = [
        'Project ID', 'Building ID', 'Number', 'Postcode', 'BBL', 'BIN',
        'Extremely Low Income Units', 'Very Low Income Units', 'Low Income Units',
        'Moderate Income Units', 'Middle Income Units', 'Other Income Units',
        'Studio Units', '1-BR Units', '2-BR Units', '3-BR Units', '4-BR Units',
        '5-BR Units', '6-BR+ Units', 'Unknown-BR Units', 'Counted Rental Units',
        'Counted Homeownership Units', 'All Counted Units', 'Total Units'
    ]

    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')

    # Save to CSV if requested
    if output_file:
        df.to_csv(output_file, index=False)
        print(f"Data saved to: {output_file}")

    return df

def update_local_data():
    """Update the local affordable housing data file with fresh API data."""
    output_file = "Affordable_Housing_Production_by_Building.csv"
    backup_file = f"{output_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Create backup of existing file
    if os.path.exists(output_file):
        os.rename(output_file, backup_file)
        print(f"Created backup: {backup_file}")

    # Fetch fresh data
    df = fetch_affordable_housing_data()

    # Save as new file
    df.to_csv(output_file, index=False)
    print(f"Updated local data file: {output_file}")

    return df

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--update":
        # Update the local data file
        update_local_data()
    else:
        # Just fetch and display info
        df = fetch_affordable_housing_data(limit=100)  # Small sample
        print(f"\nSample data shape: {df.shape}")
        print(f"Columns: {len(df.columns)}")
        print(f"Sample project: {df['Project Name'].iloc[0] if len(df) > 0 else 'None'}")
