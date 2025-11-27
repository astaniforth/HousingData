import pandas as pd
import re
import sys
import os
from datetime import datetime

def extract_date(date_value):
    """Extract date string, trying to parse various formats."""
    if pd.isna(date_value):
        return None
    date_str = str(date_value).strip()
    if date_str == '' or date_str.lower() == 'nan':
        return None
    return date_str

def parse_date_for_sorting(date_str):
    """Parse date string to datetime for sorting."""
    if not date_str or pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip()
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S.%f', '%m-%d-%Y', '%Y/%m/%d']
    
    for fmt in formats:
        try:
            if 'T' in date_str:
                date_str = date_str.split('T')[0]
            return datetime.strptime(date_str.split()[0], fmt)
        except:
            continue
    return None

def normalize_bin(bin_num):
    """Convert BIN to normalized string format."""
    if pd.isna(bin_num):
        return None
    try:
        return str(int(float(bin_num)))
    except:
        return str(bin_num)

def extract_address(row, address_cols):
    """Extract address from row using specified columns."""
    address_parts = []
    for col in address_cols:
        if col in row.index and pd.notna(row[col]):
            address_parts.append(str(row[col]).strip())
    return ' '.join(address_parts) if address_parts else 'N/A'

def get_date_from_columns(row, column_priority):
    """Get first available date from a list of column names."""
    for col in column_priority:
        if col in row.index:
            date = extract_date(row[col])
            if date:
                return date
    return None

def process_dob_filings(df_filings, buildings_by_bin, source):
    """Process DOB filings and add events to buildings_by_bin."""
    if df_filings.empty:
        return

    # Determine BIN column and date columns based on source
    if source == 'DOB':
        bin_col = 'bin__' if 'bin__' in df_filings.columns else 'bin_normalized' if 'bin_normalized' in df_filings.columns else 'bin'
        filing_cols = ['pre__filing_date', 'latest_action_date', 'dobrundate']
        approval_cols = ['fully_permitted', 'approved', 'signoff_date']
        job_num_col = 'job__'
        job_type_col = 'job_type'
        default_job_type = 'NB'
        event_prefix = 'DOB'
    else:  # DOB NOW
        bin_col = 'bin' if 'bin' in df_filings.columns else 'bin_normalized'
        filing_cols = ['filing_date', 'first_permit_date']
        approval_cols = ['approved_date', 'first_permit_date']
        job_num_col = 'job_filing_number'
        job_type_col = 'job_type'
        default_job_type = 'New Building'
        event_prefix = 'DOB NOW'

    for idx, row in df_filings.iterrows():
        bin_str = normalize_bin(row.get(bin_col))
        if not bin_str:
            continue

        # Initialize BIN entry if needed
        if bin_str not in buildings_by_bin:
            buildings_by_bin[bin_str] = {
                'address': 'N/A',
                'hpd_events': [],
                'dob_events': [],
                'co_events': []
            }

        # Get dates and job info
        filing_date = get_date_from_columns(row, filing_cols)
        approval_date = get_date_from_columns(row, approval_cols)
        job_num = row.get(job_num_col, 'N/A')
        job_type = row.get(job_type_col, default_job_type)

        # Add filing event
        if filing_date:
            buildings_by_bin[bin_str]['dob_events'].append({
                'date': filing_date,
                'event': f'{event_prefix} {job_type} Application submitted',
                'source': source,
                'additional_info': f"Job: {job_num}"
            })

        # Add approval event
        if approval_date:
            buildings_by_bin[bin_str]['dob_events'].append({
                'date': approval_date,
                'event': f'{event_prefix} {job_type} Application approved',
                'source': source,
                'additional_info': f"Job: {job_num}"
            })

def process_co_filings(df_co_filings, buildings_by_bin):
    """Process CO filings and add events to buildings_by_bin."""
    if df_co_filings.empty:
        return

    for idx, row in df_co_filings.iterrows():
        bin_str = normalize_bin(row.get('bin_normalized'))
        if not bin_str:
            continue

        # Initialize BIN entry if needed
        if bin_str not in buildings_by_bin:
            buildings_by_bin[bin_str] = {
                'address': 'N/A',
                'hpd_events': [],
                'dob_events': [],
                'co_events': []
            }

        # Get CO date and type based on source
        co_date = None
        source = row.get('source', 'Unknown')
        co_type = 'Unknown'

        if source == 'DOB_NOW_CO':
            co_date = extract_date(row.get('c_of_o_issuance_date'))
            job_num = row.get('job_filing_name', 'N/A')
            co_status = row.get('c_of_o_status', 'N/A')
            filing_type = row.get('c_of_o_filing_type', 'Unknown')
            # Determine if it's first/initial or final
            if filing_type and 'Final' in str(filing_type):
                co_type = 'Final'
            elif filing_type and ('Initial' in str(filing_type) or 'Renewal' in str(filing_type)):
                co_type = 'Initial'
            else:
                co_type = 'Other'
        else:  # DOB_CO
            co_date = extract_date(row.get('c_o_issue_date'))
            job_num = row.get('job_number', 'N/A')
            co_status = row.get('application_status_raw', 'N/A')
            issue_type = row.get('issue_type', 'Unknown')
            # Determine if it's first/initial or final
            if issue_type and str(issue_type).strip() == 'Final':
                co_type = 'Final'
            else:
                co_type = 'Initial'

        # Add CO event with type
        if co_date:
            event_name = f'Certificate of Occupancy issued ({co_type})'
            buildings_by_bin[bin_str]['co_events'].append({
                'date': co_date,
                'event': event_name,
                'source': source,
                'additional_info': f"Job: {job_num}, Status: {co_status}, Type: {co_type}"
            })

def create_timeline(building_csv, filings_csv, co_filings_csv=None, output_path=None, financing_filter=None):
    """
    Create a timeline showing HPD financing dates, DOB filing/approval dates, and CO dates.

    Args:
        building_csv: Path to HPD building data CSV
        filings_csv: Path to DOB filings CSV
        co_filings_csv: Path to CO filings CSV (optional)
        output_path: Path to save output CSV (optional)
        financing_filter: Filter by financing type ('HPD Financed', 'Privately Financed', or None for all)
    """

    print(f"Reading building data (HPD) from: {building_csv}...")
    df_buildings = pd.read_csv(building_csv)
    print(f"Total buildings: {len(df_buildings):,}")

    # Apply financing filter if specified
    if financing_filter:
        print(f"Filtering to {financing_filter} projects...")
        df_buildings = df_buildings[df_buildings['Financing Type'] == financing_filter]
        print(f"Buildings after filtering: {len(df_buildings):,}\n")
    else:
        print("Including all financing types\n")

    print(f"Reading DOB filings from: {filings_csv}...")
    df_filings = pd.read_csv(filings_csv)
    print(f"Total filings: {len(df_filings):,}\n")

    if co_filings_csv:
        print(f"Reading CO filings from: {co_filings_csv}...")
        df_co_filings = pd.read_csv(co_filings_csv)
        print(f"Total CO filings: {len(df_co_filings):,}\n")
    else:
        df_co_filings = pd.DataFrame()
        print("No CO filings provided\n")
    
    # Get address columns
    address_cols = [col for col in ['Number', 'Street', 'house_no', 'street_name'] 
                     if col in df_buildings.columns]
    
    print("Creating timeline by BIN...")
    buildings_by_bin = {}
    
    # Process HPD building data
    for idx, row in df_buildings.iterrows():
        bin_str = normalize_bin(row.get('BIN'))
        if not bin_str:
            continue
        
        if bin_str not in buildings_by_bin:
            buildings_by_bin[bin_str] = {
                'address': extract_address(row, address_cols),
                'hpd_events': [],
                'dob_events': [],
                'co_events': []
            }
        
        # Add HPD financing events
        project_name = row.get('Project Name', 'N/A')
        if pd.isna(project_name):
            project_name = 'N/A'
        
        start_date = extract_date(row.get('Project Start Date'))
        if start_date:
            buildings_by_bin[bin_str]['hpd_events'].append({
                'date': start_date,
                'event': 'HPD financing submitted',
                'source': 'HPD',
                'additional_info': f"Project: {project_name}"
            })
        
        completion_date = extract_date(row.get('Project Completion Date'))
        if completion_date:
            buildings_by_bin[bin_str]['hpd_events'].append({
                'date': completion_date,
                'event': 'HPD financing completed',
                'source': 'HPD',
                'additional_info': f"Project: {project_name}"
            })
    
    # Split DOB filings by source
    if 'source' in df_filings.columns:
        dob_filings = df_filings[df_filings['source'] == 'DOB_Job_Applications'].copy()
        dob_now_filings = df_filings[df_filings['source'] == 'DOB_NOW'].copy()
    else:
        # Fallback: determine by column names
        if 'bin__' in df_filings.columns:
            dob_filings, dob_now_filings = df_filings.copy(), pd.DataFrame()
        elif 'job_filing_number' in df_filings.columns:
            dob_filings, dob_now_filings = pd.DataFrame(), df_filings.copy()
        else:
            dob_filings, dob_now_filings = df_filings.copy(), pd.DataFrame()
    
    # Process DOB filings
    process_dob_filings(dob_filings, buildings_by_bin, 'DOB')
    process_dob_filings(dob_now_filings, buildings_by_bin, 'DOB NOW')

    # Process CO filings
    process_co_filings(df_co_filings, buildings_by_bin)

    # Create timeline dataframe
    timeline_rows = []
    for bin_str, data in buildings_by_bin.items():
        for event in data['hpd_events'] + data['dob_events'] + data['co_events']:
            timeline_rows.append({
                'BIN': bin_str,
                'Address': data['address'],
                'Date': event['date'],
                'Source': event['source'],
                'Event': event['event'],
                'Additional_Info': event.get('additional_info', '')
            })
    
    df_timeline = pd.DataFrame(timeline_rows)
    df_timeline['Date_Parsed'] = df_timeline['Date'].apply(parse_date_for_sorting)
    df_timeline = df_timeline.sort_values(['BIN', 'Date_Parsed'], na_position='last')
    df_timeline_output = df_timeline.drop(columns=['Date_Parsed'])
    
    print(f"\nTotal timeline entries: {len(df_timeline):,}")
    print(f"  HPD entries: {len(df_timeline[df_timeline['Source'] == 'HPD']):,}")
    print(f"  DOB entries: {len(df_timeline[df_timeline['Source'] == 'DOB']):,}")
    print(f"  DOB NOW entries: {len(df_timeline[df_timeline['Source'] == 'DOB NOW']):,}")
    print(f"  CO entries: {len(df_timeline[df_timeline['Source'].isin(['DOB_NOW_CO', 'DOB_CO'])]):,}\n")
    
    print("Sample timeline (first 30 entries):")
    print(df_timeline_output.head(30).to_string(index=False))
    
    if output_path is None:
        output_path = building_csv.replace('.csv', '_timeline.csv')
    
    df_timeline_output.to_csv(output_path, index=False)
    print(f"\n\nTimeline saved to: {output_path}")
    
    return df_timeline_output

def create_separate_timelines(building_csv, filings_csv, co_filings_csv=None):
    """Create separate timelines for HPD financed and privately financed projects."""

    # Check if building_csv has financing type column
    df_buildings = pd.read_csv(building_csv)
    if 'Financing Type' not in df_buildings.columns:
        print("Warning: 'Financing Type' column not found in building data.")
        print("Please run query_ll44_funding.py first to add financing type information.")
        # Fall back to creating single timeline
        create_timeline(building_csv, filings_csv, co_filings_csv, financing_filter=None)
        return

    # Create HPD financed timeline
    print("=" * 80)
    print("CREATING HPD FINANCED PROJECTS TIMELINE")
    print("=" * 80)
    hpd_output = building_csv.replace('.csv', '_hpd_financed_timeline.csv')
    create_timeline(building_csv, filings_csv, co_filings_csv,
                   output_path=hpd_output, financing_filter='HPD Financed')

    # Create privately financed timeline
    print("\n" + "=" * 80)
    print("CREATING PRIVATELY FINANCED PROJECTS TIMELINE")
    print("=" * 80)
    private_output = building_csv.replace('.csv', '_privately_financed_timeline.csv')
    create_timeline(building_csv, filings_csv, co_filings_csv,
                   output_path=private_output, financing_filter='Privately Financed')

if __name__ == "__main__":
    building_csv = sys.argv[1] if len(sys.argv) > 1 else 'Affordable_Housing_Production_by_Building_with_financing.csv'
    filings_csv = sys.argv[2] if len(sys.argv) > 2 else 'new_construction_bins_dob_filings.csv'
    co_filings_csv = sys.argv[3] if len(sys.argv) > 3 else None

    if not os.path.exists(building_csv):
        print(f"Error: Building CSV '{building_csv}' not found.")
        print("Usage: python HPD_DOB_Join_On_BIN.py <building_csv> [filings_csv] [co_filings_csv]")
        sys.exit(1)

    if not os.path.exists(filings_csv):
        print(f"Error: Filings CSV '{filings_csv}' not found.")
        sys.exit(1)

    create_separate_timelines(building_csv, filings_csv, co_filings_csv)
