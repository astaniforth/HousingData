#!/usr/bin/env python3
"""
Check if we have any DOB NOW data in the output
and if application_number contains any DOB NOW jobs (which start with letters like S, X, etc.)
"""

import pandas as pd

output_file = "/Users/andrewstaniforth/Documents/Programming/HousingData/output/hpd_multifamily_finance_new_construction_with_all_dates.csv"

print("=" * 80)
print("CHECKING FOR DOB NOW DATA IN OUTPUT")
print("=" * 80)

df = pd.read_csv(output_file)

print(f"\nTotal buildings: {len(df)}")
print(f"Buildings with earliest_dob_date: {df['earliest_dob_date'].notna().sum()}")

# Check application_number format
# DOB NOW jobs start with letters: S, X, etc. (e.g., S00587462-I1, X00969702-I1)
# BISWEB jobs are all numeric (e.g., 220412541, 321593101)

if 'application_number' in df.columns:
    apps_with_data = df[df['application_number'].notna()]
    print(f"\nBuildings with application_number: {len(apps_with_data)}")
    
    # Check how many start with a letter (DOB NOW) vs all numeric (BISWEB)
    def is_dobnow_format(app_num):
        if pd.isna(app_num):
            return False
        app_str = str(app_num)
        # DOB NOW format: starts with letter
        return app_str[0].isalpha() if app_str else False
    
    def is_bisweb_format(app_num):
        if pd.isna(app_num):
            return False
        app_str = str(app_num).strip()
        # BISWEB format: all numeric
        return app_str.replace('.0', '').replace('.', '').isdigit() if app_str else False
    
    dobnow_count = apps_with_data['application_number'].apply(is_dobnow_format).sum()
    bisweb_count = apps_with_data['application_number'].apply(is_bisweb_format).sum()
    
    print(f"\n📊 Application Number Breakdown:")
    print(f"   DOB NOW format (starts with letter): {dobnow_count}")
    print(f"   BISWEB format (all numeric): {bisweb_count}")
    
    if dobnow_count > 0:
        print(f"\n✅ We DO have DOB NOW data in the output!")
        print(f"\nSample DOB NOW applications:")
        dobnow_apps = apps_with_data[apps_with_data['application_number'].apply(is_dobnow_format)]
        for idx, row in dobnow_apps.head(5).iterrows():
            print(f"   Building {row['Project ID']}: {row['application_number']} - Date: {row['earliest_dob_date']}")
    else:
        print(f"\n❌ NO DOB NOW data found in output!")
        print(f"   All application numbers are BISWEB format (numeric only)")
        
        # Show some examples
        print(f"\nSample BISWEB applications:")
        for idx, row in apps_with_data.head(5).iterrows():
            print(f"   Building {row['Project ID']}: {row['application_number']} - Date: {row['earliest_dob_date']}")

else:
    print("\n⚠️  No application_number column in output")

# Also check earliest_dob_date_source to see what date columns are being used
if 'earliest_dob_date_source' in df.columns:
    source_counts = df['earliest_dob_date_source'].value_counts()
    print(f"\n📋 Date Source Distribution:")
    for source, count in source_counts.items():
        print(f"   {source}: {count} buildings")

