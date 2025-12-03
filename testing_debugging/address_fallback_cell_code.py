# TIER 3: ADDRESS-BASED FALLBACK
# For buildings without DOB data after BIN and BBL queries, try address-based lookup

print("=" * 70)
print("STEP 3C: ADDRESS-BASED FALLBACK (TIER 3)")
print("=" * 70)

# Identify buildings that still don't have DOB matches
if 'projects_with_no_dob' in globals() and len(projects_with_no_dob) > 0:
    print(f"\nProjects without DOB data after BIN/BBL queries: {len(projects_with_no_dob)}")
    
    # Get the buildings that need address fallback
    buildings_needing_address_fallback_df = hpd_multifamily_finance_new_construction_with_normalized_ids_df[
        hpd_multifamily_finance_new_construction_with_normalized_ids_df['Project ID'].isin(projects_with_no_dob)
    ].copy()
    
    print(f"Total building records needing address fallback: {len(buildings_needing_address_fallback_df)}")
    
    # Extract addresses - need to get from original HPD dataframe
    # The normalized IDs dataframe may not have address columns
    # Merge with original HPD data to get addresses
    hpd_with_addresses = pd.merge(
        buildings_needing_address_fallback_df[['Project ID', 'Building ID', 'bin_normalized', 'bbl_normalized']],
        hpd_multifamily_finance_new_construction_df[['Project ID', 'Building ID', 'Borough', 'Number', 'Street']],
        on=['Project ID', 'Building ID'],
        how='left'
    )
    
    addresses = []
    address_to_building_map = {}
    
    for idx, row in hpd_with_addresses.iterrows():
        borough = str(row.get('Borough', '')).upper()
        house_no = str(row.get('Number', '')).strip()
        street = str(row.get('Street', '')).strip().upper()
        
        # Skip if any required field is missing
        if not borough or borough == 'NAN' or not house_no or house_no == 'NAN' or not street or street == 'NAN':
            continue
        
        address_tuple = (borough, house_no, street)
        
        # Add to unique addresses list
        if address_tuple not in address_to_building_map:
            addresses.append(address_tuple)
            address_to_building_map[address_tuple] = []
        
        # Track which building IDs correspond to this address
        building_id = row.get('Building ID')
        if pd.notna(building_id):
            address_to_building_map[address_tuple].append(str(building_id))
    
    print(f"Unique addresses to query: {len(addresses)}")
    
    if len(addresses) > 0:
        # Query DOB by address
        print(f"\nQuerying DOB APIs by address (Tier 3 fallback)...")
        dob_address_results_df = query_dob_by_address(addresses)
        
        if not dob_address_results_df.empty:
            print(f"✅ Found {len(dob_address_results_df)} DOB records by address")
            
            # Add source tag
            dob_address_results_df['source'] = 'ADDRESS_FALLBACK'
            
            # Normalize BIN column
            if 'bin__' in dob_address_results_df.columns:
                dob_address_results_df['bin_normalized'] = dob_address_results_df['bin__'].astype(str).str.replace('.0', '', regex=False)
            elif 'bin' in dob_address_results_df.columns:
                dob_address_results_df['bin_normalized'] = dob_address_results_df['bin'].astype(str).str.replace('.0', '', regex=False)
            
            # Reconstruct BBL if not present (same logic as earlier)
            if 'bbl' not in dob_address_results_df.columns:
                dob_address_results_df['bbl_reconstructed'] = dob_address_results_df.apply(reconstruct_bbl, axis=1)
                dob_address_results_df['bbl_normalized'] = dob_address_results_df['bbl_reconstructed']
            else:
                dob_address_results_df['bbl_normalized'] = dob_address_results_df['bbl'].apply(
                    lambda x: str(int(float(x))).zfill(10) if pd.notna(x) else None
                )
            
            # Append to combined DOB data
            all_cols = list(set(list(combined_dob_with_normalized_bbl_df.columns) + list(dob_address_results_df.columns)))
            combined_dob_aligned = combined_dob_with_normalized_bbl_df.reindex(columns=all_cols)
            address_aligned = dob_address_results_df.reindex(columns=all_cols)
            combined_dob_with_normalized_bbl_df = pd.concat([combined_dob_aligned, address_aligned], ignore_index=True)
            
            print(f"✅ Appended address fallback results")
            print(f"   Total DOB records now: {len(combined_dob_with_normalized_bbl_df)}")
            
            # Check how many projects now have DOB data
            hpd_matched_on_address_df = pd.merge(
                buildings_needing_address_fallback_df,
                dob_address_results_df[['bin_normalized']].drop_duplicates(),
                on='bin_normalized',
                how='inner'
            )
            matched_project_ids_address = set(hpd_matched_on_address_df['Project ID'].unique())
            print(f"   Projects matched via address: {len(matched_project_ids_address)}")
        else:
            print(f"❌ No DOB records found by address")
    else:
        print(f"⚠️  No valid addresses to query")
else:
    print(f"\n✅ All projects already have DOB data")

print(f"\n✅ Step 3C complete")

