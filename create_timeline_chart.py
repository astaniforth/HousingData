import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import sys
import os
import re

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

def create_timeline_chart(timeline_csv, output_path=None):
    """
    Create a timeline chart showing DOB application and HPD financing timelines for each BIN.
    """
    
    print(f"Reading timeline data from: {timeline_csv}...")
    df = pd.read_csv(timeline_csv)
    print(f"Total timeline entries: {len(df):,}\n")
    
    # Parse dates
    df['Date_Parsed'] = df['Date'].apply(parse_date)
    df = df[df['Date_Parsed'].notna()].copy()
    
    # Group by BIN and extract timeline data
    print("Processing timeline data by BIN...")
    bin_timelines = []
    
    for bin_val in df['BIN'].unique():
        bin_data = df[df['BIN'] == bin_val].copy()
        address = bin_data['Address'].iloc[0] if len(bin_data) > 0 else 'N/A'
        
        # Find DOB/DOB NOW application timeline
        dob_submitted = None
        dob_approved = None
        
        # Get first DOB or DOB NOW application submitted
        dob_submitted_events = bin_data[
            (bin_data['Source'].isin(['DOB', 'DOB NOW'])) & 
            (bin_data['Event'].str.contains('Application submitted', na=False))
        ]
        if len(dob_submitted_events) > 0:
            dob_submitted_events = dob_submitted_events.sort_values('Date_Parsed')
            dob_submitted = dob_submitted_events.iloc[0]['Date_Parsed']
        
        # Get first DOB or DOB NOW application approved
        dob_approved_events = bin_data[
            (bin_data['Source'].isin(['DOB', 'DOB NOW'])) & 
            (bin_data['Event'].str.contains('Application approved', na=False))
        ]
        if len(dob_approved_events) > 0:
            dob_approved_events = dob_approved_events.sort_values('Date_Parsed')
            dob_approved = dob_approved_events.iloc[0]['Date_Parsed']
        
        # Find HPD financing timeline
        hpd_submitted = None
        hpd_completed = None
        
        hpd_submitted_events = bin_data[
            (bin_data['Source'] == 'HPD') & 
            (bin_data['Event'] == 'HPD financing submitted')
        ]
        if len(hpd_submitted_events) > 0:
            hpd_submitted_events = hpd_submitted_events.sort_values('Date_Parsed')
            hpd_submitted = hpd_submitted_events.iloc[0]['Date_Parsed']
        
        hpd_completed_events = bin_data[
            (bin_data['Source'] == 'HPD') & 
            (bin_data['Event'] == 'HPD financing completed')
        ]
        if len(hpd_completed_events) > 0:
            hpd_completed_events = hpd_completed_events.sort_values('Date_Parsed')
            hpd_completed = hpd_completed_events.iloc[0]['Date_Parsed']
        
        # Only include BINs that have at least one complete timeline
        if (dob_submitted and dob_approved) or (hpd_submitted and hpd_completed):
            bin_timelines.append({
                'BIN': bin_val,
                'Address': address,
                'DOB_Submitted': dob_submitted,
                'DOB_Approved': dob_approved,
                'HPD_Submitted': hpd_submitted,
                'HPD_Completed': hpd_completed
            })
    
    df_timelines = pd.DataFrame(bin_timelines)
    print(f"BINs with timeline data: {len(df_timelines):,}\n")
    
    # Filter to BINs with complete data for both timelines
    complete_timelines = df_timelines[
        (df_timelines['DOB_Submitted'].notna()) & 
        (df_timelines['DOB_Approved'].notna()) &
        (df_timelines['HPD_Submitted'].notna()) & 
        (df_timelines['HPD_Completed'].notna())
    ].copy()
    
    print(f"BINs with complete DOB and HPD timelines: {len(complete_timelines):,}")
    
    if len(complete_timelines) == 0:
        print("No BINs with complete timelines found. Creating chart with available data...")
        complete_timelines = df_timelines.copy()
    
    # Sort by BIN for consistent ordering
    complete_timelines = complete_timelines.sort_values('BIN')
    
    # Limit to reasonable number for visualization (too many will be unreadable)
    max_bins = 50
    if len(complete_timelines) > max_bins:
        print(f"Limiting to first {max_bins} BINs for readability...")
        complete_timelines = complete_timelines.head(max_bins)
    
    # Create the chart
    print("\nCreating timeline chart...")
    fig, ax = plt.subplots(figsize=(14, max(8, len(complete_timelines) * 0.3)))
    
    # Calculate positions for each BIN
    y_positions = range(len(complete_timelines))
    
    # Plot DOB timeline bars
    dob_bars = []
    hpd_bars = []
    
    for idx, (i, row) in enumerate(complete_timelines.iterrows()):
        y_pos = len(complete_timelines) - idx - 1
        
        # DOB timeline bar
        if pd.notna(row['DOB_Submitted']) and pd.notna(row['DOB_Approved']):
            dob_start = row['DOB_Submitted']
            dob_end = row['DOB_Approved']
            dob_duration = (dob_end - dob_start).days
            dob_bars.append((y_pos, dob_start, dob_end, dob_duration))
        
        # HPD timeline bar
        if pd.notna(row['HPD_Submitted']) and pd.notna(row['HPD_Completed']):
            hpd_start = row['HPD_Submitted']
            hpd_end = row['HPD_Completed']
            hpd_duration = (hpd_end - hpd_start).days
            hpd_bars.append((y_pos, hpd_start, hpd_end, hpd_duration))
    
    # Plot DOB bars (blue) - offset slightly up
    dob_labeled = False
    for y_pos, start, end, duration in dob_bars:
        label = 'DOB Application Timeline' if not dob_labeled else ''
        ax.barh(y_pos + 0.2, duration, left=start, height=0.35, color='#2E86AB', alpha=0.7, label=label)
        if not dob_labeled:
            dob_labeled = True
    
    # Plot HPD bars (green) - offset slightly down
    hpd_labeled = False
    for y_pos, start, end, duration in hpd_bars:
        label = 'HPD Financing Timeline' if not hpd_labeled else ''
        ax.barh(y_pos - 0.2, duration, left=start, height=0.35, color='#A23B72', alpha=0.7, label=label)
        if not hpd_labeled:
            hpd_labeled = True
    
    # Set y-axis labels
    labels = []
    for idx, (i, row) in enumerate(complete_timelines.iterrows()):
        bin_str = str(int(row['BIN'])) if pd.notna(row['BIN']) else 'N/A'
        address = str(row['Address'])[:40] if pd.notna(row['Address']) else 'N/A'
        labels.append(f"{bin_str}\n{address}")
    
    ax.set_yticks(y_positions)
    ax.set_yticklabels(labels, fontsize=8)
    ax.invert_yaxis()  # Top to bottom
    
    # Format x-axis as dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    plt.xticks(rotation=45, ha='right')
    
    # Labels and title
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_title('Timeline Chart: DOB Application vs HPD Financing\nby BIN', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='upper right')
    ax.grid(True, alpha=0.3, axis='x')
    
    plt.tight_layout()
    
    # Save the chart
    if output_path is None:
        output_path = timeline_csv.replace('.csv', '_timeline_chart.png')
    
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nChart saved to: {output_path}")
    
    # Also save the timeline data
    data_output = timeline_csv.replace('.csv', '_timeline_data.csv')
    complete_timelines.to_csv(data_output, index=False)
    print(f"Timeline data saved to: {data_output}")
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"BINs with DOB timeline: {len(complete_timelines[complete_timelines['DOB_Submitted'].notna()]):,}")
    print(f"BINs with HPD timeline: {len(complete_timelines[complete_timelines['HPD_Submitted'].notna()]):,}")
    print(f"BINs with both timelines: {len(complete_timelines[(complete_timelines['DOB_Submitted'].notna()) & (complete_timelines['HPD_Submitted'].notna())]):,}")
    
    return complete_timelines

if __name__ == "__main__":
    if len(sys.argv) > 1:
        timeline_csv = sys.argv[1]
    else:
        timeline_csv = 'Affordable_Housing_Production_by_Building_timeline.csv'
        if not os.path.exists(timeline_csv):
            print("Please provide the path to the timeline CSV file:")
            print("Usage: python create_timeline_chart.py <timeline_csv>")
            sys.exit(1)
    
    if not os.path.exists(timeline_csv):
        print(f"Error: File '{timeline_csv}' not found.")
        sys.exit(1)
    
    create_timeline_chart(timeline_csv)

