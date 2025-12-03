#!/usr/bin/env python3
"""
Find buildings without DOB dates and analyze why
"""

import pandas as pd

output_file = "/Users/andrewstaniforth/Documents/Programming/HousingData/output/hpd_multifamily_finance_new_construction_with_all_dates.csv"

df = pd.read_csv(output_file)

# Find buildings without DOB dates
no_dob = df[df['earliest_dob_date'].isna()].copy()

print("=" * 80)
print(f"BUILDINGS WITHOUT DOB DATES: {len(no_dob)} total")
print("=" * 80)

# Get a diverse sample
print("\n📊 Sample of 10 buildings without DOB dates:")
sample = no_dob.head(10)

for idx, row in sample.iterrows():
    print(f"\n{'-'*80}")
    print(f"Building ID: {row['Project ID']}")
    print(f"Project: {row['Project Name']}")
    print(f"Address: {row['Number']} {row['Street']}, {row['Borough']}")
    print(f"BIN: {row['BIN']}")
    print(f"BBL: {row['BBL']}")
    print(f"has_valid_bin: {row.get('has_valid_bin', 'N/A')}")
    
# Analyze patterns
print("\n" + "=" * 80)
print("ANALYSIS")
print("=" * 80)

# Check BIN patterns
placeholder_bins = ['1000000', '2000000', '3000000', '4000000', '5000000']
no_dob['BIN_str'] = no_dob['BIN'].astype(str).str.replace('.0', '')
placeholder_count = no_dob[no_dob['BIN_str'].isin(placeholder_bins)].shape[0]
null_bin_count = no_dob['BIN'].isna().sum()

print(f"\n🔍 BIN Analysis:")
print(f"   Placeholder BINs (1000000, 2000000, etc.): {placeholder_count}")
print(f"   Null/NaN BINs: {null_bin_count}")
print(f"   Valid-looking BINs: {len(no_dob) - placeholder_count - null_bin_count}")

# Check BBL
null_bbl_count = no_dob['BBL'].isna().sum()
print(f"\n🔍 BBL Analysis:")
print(f"   Null/NaN BBLs: {null_bbl_count}")
print(f"   Has BBL: {len(no_dob) - null_bbl_count}")

# Both null
both_null = no_dob[(no_dob['BIN'].isna() | no_dob['BIN_str'].isin(placeholder_bins)) & no_dob['BBL'].isna()]
print(f"\n❌ Both BIN and BBL are null/placeholder: {len(both_null)} buildings")
print(f"   These can't be queried at all!")

# Has valid identifiers but no DOB data
has_identifiers = no_dob[~((no_dob['BIN'].isna() | no_dob['BIN_str'].isin(placeholder_bins)) & no_dob['BBL'].isna())]
print(f"\n🤔 Has valid BIN or BBL but no DOB data: {len(has_identifiers)} buildings")
print(f"   These should be investigated - why didn't they match?")

# Export sample for detailed investigation
if len(has_identifiers) > 0:
    investigate_sample = has_identifiers.head(5)
    print(f"\n📋 Top 5 buildings to investigate:")
    for idx, row in investigate_sample.iterrows():
        print(f"   Building {row['Project ID']}: BIN={row['BIN']}, BBL={row['BBL']}, Address={row['Number']} {row['Street']}")
    
    # Save to file for API queries
    with open('/Users/andrewstaniforth/Documents/Programming/HousingData/testing_debugging/no_dob_sample.txt', 'w') as f:
        f.write("Buildings without DOB dates to investigate:\n\n")
        for idx, row in investigate_sample.iterrows():
            f.write(f"Building ID: {row['Project ID']}\n")
            f.write(f"BIN: {row['BIN']}\n")
            f.write(f"BBL: {row['BBL']}\n")
            f.write(f"Address: {row['Number']} {row['Street']}, {row['Borough']}\n")
            f.write("-" * 80 + "\n")
    print("\n✅ Sample saved to testing_debugging/no_dob_sample.txt")

# Borough distribution
print(f"\n🗺️  Borough Distribution (buildings without DOB dates):")
borough_counts = no_dob['Borough'].value_counts()
for borough, count in borough_counts.items():
    print(f"   {borough}: {count}")

