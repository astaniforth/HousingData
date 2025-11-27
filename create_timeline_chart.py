import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime
import sys
import os
import re
import numpy as np
from pathlib import Path

def extract_date(date_value):
    """Extract date string, trying to parse various formats."""
    if pd.isna(date_value):
        return None
    date_str = str(date_value).strip()
    if date_str == '' or date_str.lower() == 'nan':
        return None
    return date_str

def parse_date(date_str):
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

def create_timeline_chart(timeline_csv, output_path=None, global_min_date=None, global_max_date=None):
    """
    Create a timeline chart showing DOB application and HPD financing timelines for each BIN.
    """
    
    print(f"Reading timeline data from: {timeline_csv}...")
    timeline_data = pd.read_csv(timeline_csv)
    print(f"Total timeline entries: {len(timeline_data):,}\n")

    # Parse dates
    timeline_data['Date_Parsed'] = timeline_data['Date'].apply(parse_date)
    timeline_data = timeline_data[timeline_data['Date_Parsed'].notna()].copy()
    
    # Group by BIN and extract timeline data
    print("Processing timeline data by BIN...")
    building_timelines = []
    
    for building_bin in timeline_data['BIN'].unique():
        building_timeline_data = timeline_data[timeline_data['BIN'] == building_bin].copy()
        address = building_timeline_data['Address'].iloc[0] if len(building_timeline_data) > 0 else 'N/A'
        
        # Find DOB/DOB NOW application timeline
        dob_submitted = None
        dob_approved = None
        
        # Get first DOB or DOB NOW application submitted
        dob_submitted_events = building_timeline_data[
            (building_timeline_data['Source'].isin(['DOB', 'DOB NOW'])) &
            (building_timeline_data['Event'].str.contains('Application submitted', na=False))
        ]
        if len(dob_submitted_events) > 0:
            dob_submitted_events = dob_submitted_events.sort_values('Date_Parsed')
            dob_submitted = dob_submitted_events.iloc[0]['Date_Parsed']
        
        # Get first DOB or DOB NOW application approved
        dob_approved_events = building_timeline_data[
            (building_timeline_data['Source'].isin(['DOB', 'DOB NOW'])) &
            (building_timeline_data['Event'].str.contains('Application approved', na=False))
        ]
        if len(dob_approved_events) > 0:
            dob_approved_events = dob_approved_events.sort_values('Date_Parsed')
            dob_approved = dob_approved_events.iloc[0]['Date_Parsed']
        
        # Find HPD financing timeline
        hpd_submitted = None
        hpd_completed = None

        hpd_submitted_events = building_timeline_data[
            (building_timeline_data['Source'] == 'HPD') &
            (building_timeline_data['Event'] == 'HPD financing submitted')
        ]
        if len(hpd_submitted_events) > 0:
            hpd_submitted_events = hpd_submitted_events.sort_values('Date_Parsed')
            hpd_submitted = hpd_submitted_events.iloc[0]['Date_Parsed']

        hpd_completed_events = building_timeline_data[
            (building_timeline_data['Source'] == 'HPD') &
            (building_timeline_data['Event'] == 'HPD financing completed')
        ]
        if len(hpd_completed_events) > 0:
            hpd_completed_events = hpd_completed_events.sort_values('Date_Parsed')
            hpd_completed = hpd_completed_events.iloc[0]['Date_Parsed']

        # Find CO issuance timeline - both initial and final
        co_initial = None
        co_final = None

        # Find initial/first CO
        co_initial_events = building_timeline_data[
            (building_timeline_data['Source'].isin(['DOB_NOW_CO', 'DOB_CO'])) &
            (building_timeline_data['Event'].str.contains('Certificate of Occupancy issued \\(Initial\\)', na=False))
        ]
        if len(co_initial_events) > 0:
            co_initial_events = co_initial_events.sort_values('Date_Parsed')
            co_initial = co_initial_events.iloc[0]['Date_Parsed']

        # Find final CO
        co_final_events = building_timeline_data[
            (building_timeline_data['Source'].isin(['DOB_NOW_CO', 'DOB_CO'])) &
            (building_timeline_data['Event'].str.contains('Certificate of Occupancy issued \\(Final\\)', na=False))
        ]
        if len(co_final_events) > 0:
            co_final_events = co_final_events.sort_values('Date_Parsed')
            co_final = co_final_events.iloc[0]['Date_Parsed']

        # Only include BINs that have at least one complete timeline
        if (dob_submitted and dob_approved) or (hpd_submitted and hpd_completed) or co_initial or co_final:
            building_timelines.append({
                'BIN': building_bin,
                'Address': address,
                'DOB_Submitted': dob_submitted,
                'DOB_Approved': dob_approved,
                'HPD_Submitted': hpd_submitted,
                'HPD_Completed': hpd_completed,
                'CO_Initial': co_initial,
                'CO_Final': co_final
            })
    
    timelines_dataframe = pd.DataFrame(building_timelines)
    print(f"BINs with timeline data: {len(timelines_dataframe):,}\n")
    
    # Filter to BINs with complete data for both timelines
    complete_timelines = timelines_dataframe[
        (timelines_dataframe['DOB_Submitted'].notna()) &
        (timelines_dataframe['DOB_Approved'].notna()) &
        (timelines_dataframe['HPD_Submitted'].notna()) &
        (timelines_dataframe['HPD_Completed'].notna())
    ].copy()
    
    print(f"BINs with complete DOB and HPD timelines: {len(complete_timelines):,}")
    
    if len(complete_timelines) == 0:
        print("No BINs with complete timelines found. Creating chart with available data...")
        complete_timelines = timelines_dataframe.copy()
    
    # Sort by first DOB application date (DOB_Submitted)
    complete_timelines = complete_timelines.sort_values('DOB_Submitted', na_position='last')
    print(f"Sorted {len(complete_timelines)} BINs by first DOB application date\n")
    
    # Number of BINs per page
    bins_per_page = 25

    # Create multi-page PDF chart
    print("\nCreating multi-page timeline chart...")

    # Create output directory if it doesn't exist
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)

    # Determine output path
    if output_path is None:
        base_filename = Path(timeline_csv).stem
        output_path = output_dir / f"{base_filename}_timeline_chart.pdf"
    elif not output_path.endswith('.pdf'):
        output_path = output_path.replace('.png', '.pdf')
        output_path = Path(output_dir) / Path(output_path).name

    # Calculate number of pages needed
    total_bins = len(complete_timelines)
    num_pages = int(np.ceil(total_bins / bins_per_page))

    print(f"Creating {num_pages} page(s) with up to {bins_per_page} BINs per page...")

    # Determine consistent date range across all pages
    if global_min_date is not None and global_max_date is not None:
        min_date = global_min_date
        max_date = global_max_date
        print(f"Using global date range: {min_date.date()} to {max_date.date()}")
    else:
        # Fallback: calculate from current dataset
        all_dates = []
        date_columns = ['DOB_Submitted', 'DOB_Approved', 'HPD_Submitted', 'HPD_Completed', 'CO_Initial', 'CO_Final']
        for col in date_columns:
            if col in complete_timelines.columns:
                dates = complete_timelines[col].dropna()
                all_dates.extend(dates)

        if all_dates:
            min_date = min(all_dates) - pd.Timedelta(days=180)  # 6 months buffer
            max_date = max(all_dates) + pd.Timedelta(days=180)  # 6 months buffer
        else:
            # Fallback if no dates found
            min_date = pd.Timestamp('2010-01-01')
            max_date = pd.Timestamp('2030-01-01')

        print(f"Dataset date range: {min_date.date()} to {max_date.date()}")

    # Create PDF
    with PdfPages(output_path) as pdf:
        for page_num in range(num_pages):
            start_idx = page_num * bins_per_page
            end_idx = min(start_idx + bins_per_page, total_bins)
            page_data = complete_timelines.iloc[start_idx:end_idx].copy()
            
            # Create figure for this page with consistent dimensions
            # Fixed height based on 25 BINs (bins_per_page) for consistent vertical scale
            fig, ax = plt.subplots(figsize=(14, bins_per_page * 0.35))

            # Calculate positions for each BIN on this page
            y_positions = range(len(page_data))
            
            # Plot timeline segments - one line per BIN with overlapping segments
            dob_labeled = False
            hpd_labeled = False
            co_initial_labeled = False
            co_final_labeled = False

            for idx, (i, row) in enumerate(page_data.iterrows()):
                y_pos = len(page_data) - idx - 1

                # DOB timeline segment (blue)
                if pd.notna(row['DOB_Submitted']) and pd.notna(row['DOB_Approved']):
                    dob_start = row['DOB_Submitted']
                    dob_end = row['DOB_Approved']
                    dob_duration = (dob_end - dob_start).days
                    label = 'DOB Application Timeline' if not dob_labeled else ''
                    ax.barh(y_pos, dob_duration, left=dob_start, height=0.6,
                           color='#2E86AB', alpha=0.8, label=label, edgecolor='#1a5d7a', linewidth=0.5)
                    if not dob_labeled:
                        dob_labeled = True

                # HPD timeline segment (purple) - can overlap with DOB
                if pd.notna(row['HPD_Submitted']) and pd.notna(row['HPD_Completed']):
                    hpd_start = row['HPD_Submitted']
                    hpd_end = row['HPD_Completed']
                    hpd_duration = (hpd_end - hpd_start).days
                    label = 'HPD Financing Timeline' if not hpd_labeled else ''
                    ax.barh(y_pos, hpd_duration, left=hpd_start, height=0.6,
                           color='#A23B72', alpha=0.8, label=label, edgecolor='#7a2b55', linewidth=0.5)
                    if not hpd_labeled:
                        hpd_labeled = True

                # CO event markers
                # Initial CO (light green)
                if pd.notna(row['CO_Initial']):
                    co_date = row['CO_Initial']
                    label = 'Initial Certificate of Occupancy' if not co_initial_labeled else ''
                    ax.barh(y_pos + 0.15, 30, left=co_date, height=0.25,  # 30-day width for visibility
                           color='#81C784', alpha=0.9, label=label, edgecolor='#4CAF50', linewidth=1)
                    if not co_initial_labeled:
                        co_initial_labeled = True

                # Final CO (dark green)
                if pd.notna(row['CO_Final']):
                    co_date = row['CO_Final']
                    label = 'Final Certificate of Occupancy' if not co_final_labeled else ''
                    ax.barh(y_pos - 0.15, 30, left=co_date, height=0.25,  # 30-day width for visibility
                           color='#388E3C', alpha=0.9, label=label, edgecolor='#1B5E20', linewidth=1)
                    if not co_final_labeled:
                        co_final_labeled = True
            
            # Set y-axis labels with consistent positioning
            labels = []
            for idx, (i, row) in enumerate(page_data.iterrows()):
                bin_str = str(int(row['BIN'])) if pd.notna(row['BIN']) else 'N/A'
                address = str(row['Address'])[:50] if pd.notna(row['Address']) else 'N/A'
                labels.append(f"{bin_str}\n{address}")

            # Pad labels to ensure consistent y-axis height (25 slots)
            while len(labels) < bins_per_page:
                labels.append("")

            # Set consistent y-axis limits (0 to bins_per_page-1)
            ax.set_ylim(-0.5, bins_per_page - 0.5)
            ax.set_yticks(range(bins_per_page))
            ax.set_yticklabels(labels, fontsize=7)
            ax.invert_yaxis()  # Top to bottom
            
            # Set consistent x-axis limits across all pages
            ax.set_xlim(min_date, max_date)

            # Format x-axis as dates
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax.xaxis.set_major_locator(mdates.YearLocator())
            plt.xticks(rotation=45, ha='right')

            # Labels and title
            ax.set_xlabel('Date', fontsize=12, fontweight='bold')

            # Determine financing type from filename
            financing_type = ""
            if "hpd_financed" in timeline_csv.lower():
                financing_type = "HPD Financed Projects"
            elif "privately_financed" in timeline_csv.lower():
                financing_type = "Privately Financed Projects"
            else:
                financing_type = "All Projects"

            title = f'{financing_type} - Timeline Chart: DOB Application vs HPD Financing by BIN\n'
            title += f'Page {page_num + 1} of {num_pages} | BINs {start_idx + 1}-{end_idx} of {total_bins}'
            title += f'\n(Sorted by first DOB application date)'
            ax.set_title(title, fontsize=13, fontweight='bold', pad=20)
            ax.legend(loc='upper right', fontsize=9)
            ax.grid(True, alpha=0.3, axis='x')
            
            plt.tight_layout()
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            print(f"  Page {page_num + 1}/{num_pages} completed ({len(page_data)} BINs)")
    
    print(f"\nMulti-page chart saved to: {output_path}")
    
    # Also save the timeline data
    base_filename = Path(timeline_csv).stem
    data_output = output_dir / f"{base_filename}_timeline_data.csv"
    complete_timelines.to_csv(data_output, index=False)
    print(f"Timeline data saved to: {data_output}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"BINs with DOB timeline: {len(complete_timelines[complete_timelines['DOB_Submitted'].notna()]):,}")
    print(f"BINs with HPD timeline: {len(complete_timelines[complete_timelines['HPD_Submitted'].notna()]):,}")
    print(f"BINs with Initial CO: {len(complete_timelines[complete_timelines['CO_Initial'].notna()]):,}")
    print(f"BINs with Final CO: {len(complete_timelines[complete_timelines['CO_Final'].notna()]):,}")
    print(f"BINs with any CO: {len(complete_timelines[(complete_timelines['CO_Initial'].notna()) | (complete_timelines['CO_Final'].notna())]):,}")
    print(f"BINs with both DOB and HPD timelines: {len(complete_timelines[(complete_timelines['DOB_Submitted'].notna()) & (complete_timelines['HPD_Submitted'].notna())]):,}")
    print(f"BINs with DOB and any CO: {len(complete_timelines[(complete_timelines['DOB_Submitted'].notna()) & ((complete_timelines['CO_Initial'].notna()) | (complete_timelines['CO_Final'].notna()))]):,}")
    print(f"BINs with HPD and any CO: {len(complete_timelines[(complete_timelines['HPD_Submitted'].notna()) & ((complete_timelines['CO_Initial'].notna()) | (complete_timelines['CO_Final'].notna()))]):,}")
    
    return complete_timelines

def create_financing_charts():
    """Create separate charts for HPD financed and privately financed projects."""

    hpd_timeline = 'Affordable_Housing_Production_by_Building_with_financing_hpd_financed_timeline.csv'
    private_timeline = 'Affordable_Housing_Production_by_Building_with_financing_privately_financed_timeline.csv'

    # Calculate global date range across both datasets
    global_min_date = None
    global_max_date = pd.Timestamp.today()  # End date is today

    # Check HPD financed data
    if os.path.exists(hpd_timeline):
        hpd_data = pd.read_csv(hpd_timeline)
        date_columns = ['DOB_Submitted', 'DOB_Approved', 'HPD_Submitted', 'HPD_Completed', 'CO_Initial', 'CO_Final']
        for col in date_columns:
            if col in hpd_data.columns:
                dates = pd.to_datetime(hpd_data[col], errors='coerce').dropna()
                if len(dates) > 0:
                    if global_min_date is None or dates.min() < global_min_date:
                        global_min_date = dates.min()

    # Check privately financed data
    if os.path.exists(private_timeline):
        private_data = pd.read_csv(private_timeline)
        date_columns = ['DOB_Submitted', 'DOB_Approved', 'HPD_Submitted', 'HPD_Completed', 'CO_Initial', 'CO_Final']
        for col in date_columns:
            if col in private_data.columns:
                dates = pd.to_datetime(private_data[col], errors='coerce').dropna()
                if len(dates) > 0:
                    if global_min_date is None or dates.min() < global_min_date:
                        global_min_date = dates.min()

    # Set global date range
    if global_min_date is None:
        global_min_date = pd.Timestamp('2010-01-01')  # Fallback

    # Add buffer
    global_min_date = global_min_date - pd.Timedelta(days=180)  # 6 months buffer
    global_max_date = global_max_date + pd.Timedelta(days=30)   # 1 month buffer

    print(f"Global date range for all charts: {global_min_date.date()} to {global_max_date.date()}")

    # Create HPD financed chart
    if os.path.exists(hpd_timeline):
        print("=" * 80)
        print("CREATING HPD FINANCED PROJECTS CHART")
        print("=" * 80)
        create_timeline_chart(hpd_timeline, global_min_date=global_min_date, global_max_date=global_max_date)
    else:
        print(f"HPD financed timeline not found: {hpd_timeline}")

    # Create privately financed chart
    if os.path.exists(private_timeline):
        print("\n" + "=" * 80)
        print("CREATING PRIVATELY FINANCED PROJECTS CHART")
        print("=" * 80)
        create_timeline_chart(private_timeline, global_min_date=global_min_date, global_max_date=global_max_date)
    else:
        print(f"Privately financed timeline not found: {private_timeline}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # If specific timeline file provided, create single chart
        timeline_csv = sys.argv[1]
        if not os.path.exists(timeline_csv):
            print(f"Error: File '{timeline_csv}' not found.")
            sys.exit(1)
        create_timeline_chart(timeline_csv)
    else:
        # Create both financing type charts
        create_financing_charts()

