#!/usr/bin/env python3
"""
Test address matching for building 50497 specifically
"""

import sys
sys.path.append("/Users/andrewstaniforth/Documents/Programming/HousingData")

from query_dob_filings import query_dob_by_address

print("=" * 80)
print("TESTING ADDRESS VARIATIONS FOR BUILDING 50497")
print("=" * 80)

print("\nBuilding 50497: 655 Morris Avenue")
print("HPD has: 635 MORRIS AVENUE")
print("DOB has: 655 MORRIS AVENUE")
print("\nThis demonstrates the challenge: address discrepancies between HPD and DOB\n")

# Test both addresses
addresses = [
    ("BRONX", "635", "MORRIS AVENUE"),  # What HPD says
    ("BRONX", "655", "MORRIS AVENUE"),  # What DOB says
]

for borough, house, street in addresses:
    print(f"\n{'='*80}")
    print(f"Testing: {house} {street}, {borough}")
    print(f"{'='*80}")
    
    result = query_dob_by_address([(borough, house, street)])
    
    if not result.empty:
        print(f"✅ Found {len(result)} records")
        display_cols = ['job__', 'bin__', 'house__', 'street_name', 'pre__filing_date']
        existing_cols = [col for col in display_cols if col in result.columns]
        print(result[existing_cols].to_string(index=False))
    else:
        print(f"❌ No records found")

print(f"\n{'='*80}")
print("CONCLUSION")
print(f"{'='*80}")
print("""
Address-based fallback CAN find DOB data, but only if:
1. The address in HPD matches the address in DOB
2. Or we implement fuzzy matching (e.g., try nearby house numbers)

For the 67 buildings without DOB data:
- Some have matching addresses and will benefit from address fallback
- Some have mismatched addresses (like 635 vs 655) and won't match
- Some legitimately don't have NB filings in DOB

RECOMMENDATION:
- Implement address fallback as-is (will help some cases)
- Accept that some buildings won't match due to address discrepancies
- Could enhance later with fuzzy matching if needed
""")

