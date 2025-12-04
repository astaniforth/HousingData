# Bugs and Fixes Log

This document tracks bugs discovered and fixes applied during development.

## Undefined Variable Bug in Notebook

**Status: Fixed**
**Date: Dec 2, 2025**
**Commit SHA: 6e2115a**

**Bug Description:**
NameError in run_workflow.ipynb cell: `name 'bad_bins' is not defined`

**Symptoms:**
- Cell execution failed with NameError when trying to filter duplicated BINs
- Code referenced `bad_bins` variable that was never defined in the notebook scope

**Root Cause:**
Code snippet was copied from elsewhere or left incomplete, referencing a variable that should have been defined but wasn't.

**Fix:**
Simplified the filtering logic to remove the undefined `bad_bins` reference. The cell now simply shows all duplicated BINs without trying to exclude "bad" ones.

**Code Change:**
```python
# Before (broken):
hpd_multifamily_finance_new_construction_df[
    (hpd_multifamily_finance_new_construction_df["BIN"].duplicated(keep=False)) &
    (~hpd_multifamily_finance_new_construction_df["BIN"].astype(str).isin(bad_bins))
]

# After (fixed):
hpd_multifamily_finance_new_construction_df[
    hpd_multifamily_finance_new_construction_df["BIN"].duplicated(keep=False)
]
```

**Testing:**
- Cell now executes without errors
- No linter errors remain

## BBL Matching Not Working in HPD-DOB Join

**Status: Fixed**
**Date: Dec 2, 2025**

**Bug Description:**
BBL matching was not working in the HPD-DOB join workflow. The code was only matching on BIN, and the BBL fallback matching was failing.

**Symptoms:**
- BBL matching was not finding matches even when BBLs existed in the DOB data
- Many HPD rows showed NaT (Not a Time) for `earliest_dob_date` even when DOB records existed with matching BBLs
- The code was creating `bbl_str` from only the `bbl` column, ignoring reconstructed BBLs from borough/block/lot

**Root Cause:**
The code in the join cell was creating a new `bbl_str` column from only the `bbl` column using `dob_df['bbl'].apply(...)`. However, `combined_dob_with_normalized_bbl_df` already has a `bbl_normalized` column that includes both:
1. Original `bbl` values (when available)
2. Reconstructed BBLs from borough/block/lot (for BISWEB data that doesn't have a `bbl` column)

By creating `bbl_str` from only the `bbl` column, the code was missing all the reconstructed BBLs, causing BBL matching to fail for records that only had borough/block/lot information.

**Fix:**
Changed the code to use the existing `bbl_normalized` column instead of creating a new `bbl_str` column. Added proper formatting logic to ensure `bbl_normalized` values are always 10-digit zero-padded strings. Also filtered out None/NaN BBLs before grouping to avoid grouping on None values.

**Code Changes:**
1. Replaced `dob_df['bbl_str'] = dob_df['bbl'].apply(...)` with logic that uses `bbl_normalized` if it exists, or creates it from `bbl` as a fallback
2. Added `format_bbl()` function to properly format BBL values (handles int, float, and string types)
3. Changed `dob_bbl_min` grouping from `groupby('bbl_str')` to `groupby('bbl_normalized')` with filtering for non-null values
4. Updated merge statement to rename `bbl_normalized` to `BBL_str` instead of `bbl_str`

**Testing:**
- Code now uses `bbl_normalized` which includes both original and reconstructed BBLs
- BBL matching should now work for records that only have borough/block/lot information
- Proper formatting ensures all BBLs are 10-digit zero-padded strings for consistent matching

## DOB Date Filtering - Type Mismatch in doc__ Comparison

**Status: Fixed**
**Date: Dec 2, 2025**

**Bug Description:**
The doc__ comparison was using integer comparison (`doc__ == 1`) but the column may contain values that need zero-padded string comparison (`'01'`).

**Symptoms:**
- All earliest_dob_date values were NaT
- doc__ = 1 filter might not match records with different type representation

**Root Cause:**
The doc__ column value type varies. Need to compare as zero-padded string for consistency.

**Fix:**
Changed comparison from:
```python
dob_df['doc__'] == 1
```
To:
```python
dob_df['doc__'].astype(str).str.zfill(2) == '01'
```

This converts the value to a zero-padded string (1 -> '01') for consistent matching.

**Testing:**
- Debug script in testing_debugging/debug_doc01_bins.py can investigate data

---

## Earliest DOB Date Not Correctly Extracted Across All Applications

**Status: Open**
**Date: Dec 2, 2025**

**Bug Description:**
When multiple DOB applications exist for the same BIN, the code selects the most recent application (to avoid withdrawn/abandoned ones), but then only finds the earliest date from that selected application. This misses earlier dates that exist in older applications for the same BIN.

**Symptoms:**
- For Building ID 927748, BIN 2129098, the `earliest_dob_date_source` correctly shows `pre__filing_date`
- But `earliest_dob_date` shows `2013-12-31` (from the `paid` column) instead of `2011-06-14` (from `pre__filing_date`)
- The actual `pre__filing_date` for job 220124381 is `2011-06-14`, which should be the earliest date

**Root Cause:**
The current logic:
1. Filters to most recent application per BIN (based on `application_date`)
2. Finds the earliest date FROM THAT APPLICATION only
3. If the most recent application doesn't have the earliest `pre__filing_date` (because it's in an older application), we miss it

**Example Scenario:**
- Application 1 (older): `pre__filing_date = 2011-06-14`, `paid = 2013-12-31`
- Application 2 (newer): `pre__filing_date = None`, `paid = 2014-01-15`
- Current logic selects Application 2 (most recent), then finds earliest date from Application 2 = `2014-01-15`
- But the actual earliest date across ALL applications is `2011-06-14` from Application 1

**Fix:**
After selecting the most recent application per BIN (to avoid withdrawn/abandoned applications), we should find the earliest date across ALL applications for that BIN, not just from the selected application. This ensures we get the true earliest milestone date while still filtering out withdrawn applications.

**Code Changes Needed:**
1. After `groupby('bin_normalized').first()` to get the most recent application, store the BIN values
2. For each BIN, filter the original dataframe to all applications for that BIN (not just the most recent one)
3. Find the earliest date across ALL applications for that BIN
4. Use that earliest date (and its source column) as the final result

**Testing:**
- For BIN 2129098, should correctly identify `2011-06-14` as the earliest date from `pre__filing_date`
- Should work correctly when the most recent application has the earliest date
- Should work correctly when an older application has the earliest date

---

## C of O Dates Missing Initial CO from DOB NOW API

**Status: Fixed**
**Date: Dec 2, 2025**
**Commit SHA: 62c28c4**

**Bug Description:**
The earliest C of O date is not being correctly extracted when records exist in both DOB NOW CO API (pkdm-hqz6) and DOB CO API (bs8b-p36w). The workflow is only showing the Final CO date from the legacy DOB CO API, missing the earlier Initial CO date from the DOB NOW API.

**Symptoms:**
- Building ID 64608 (BIN 3427387, "142-150 SOUTH PORTLAND") shows `earliest_co_date` as 2025-10-30
- However, DOB NOW CO API has CO-000051396 dated 02/28/2024 with `c_of_o_filing_type = "Initial"`
- The 2025-10-30 date is from job 321593101 in the legacy DOB CO API with `issue_type = "Final"`
- The actual first/initial CO should be 02/28/2024, not 2025-10-30

**Root Cause:**
The CO date column detection code in the notebook was checking for columns containing both 'date' AND 'issue':
```python
if 'date' in col.lower() and 'issue' in col.lower():
```

This matched `c_o_issue_date` (DOB CO API - legacy) but not `c_of_o_issuance_date` (DOB NOW API), because 'issuance' ≠ 'issue'. All DOB NOW CO dates were being completely ignored.

**API Data Found:**
DOB NOW CO API (pkdm-hqz6) for BIN 3427387:
- 9 records total
- **Earliest**: CO-000051396, dated 02/28/2024, type "Initial"
- Others: various renewal and final COs dated 2024-2025

DOB CO API (bs8b-p36w) for BIN 3427387:
- 1 record: job 321593101, dated 2025-10-30, type "Final"

**Fix:**
Changed the column detection in cell 21 of run_workflow.ipynb to also check for 'issuance':

```python
# BEFORE (buggy):
for col in co_filings_df.columns:
    if 'date' in col.lower() and 'issue' in col.lower():
        co_date_cols.append(col)

# AFTER (fixed):
for col in co_filings_df.columns:
    col_lower = col.lower()
    if 'date' in col_lower and ('issue' in col_lower or 'issuance' in col_lower):
        co_date_cols.append(col)
```

This now correctly finds both:
- `c_o_issue_date` (DOB CO API - legacy)  
- `c_of_o_issuance_date` (DOB NOW CO API)

**Testing:**
- Debug script created: `testing_debugging/debug_co_64608.py` confirms both APIs have data and the column names
- For BIN 3427387, should now show `earliest_co_date` as 2024-02-28 (Initial CO), not 2025-10-30 (Final CO)
- The fix ensures all C of O dates from both APIs are considered when finding the earliest date

---

## DOB Query Excludes Shared BINs

**Status: Fixed**
**Date: Dec 2, 2025**
**Commit SHA: 36794c5**

**Bug Description:**
Buildings that share a BIN with another building are not getting DOB data. The DOB query step only queries BINs that appear exactly once in the dataset, excluding any BIN that's used by multiple buildings.

**Symptoms:**
- Building 44409 (Crotona Terrace II) - BIN 2124684 - No DOB data ❌
- Building 50104 (Crotona Terrace I) - **SAME BIN 2124684** - No DOB data ❌
- Both buildings also share BBL 2029847503
- Job 220412541 exists in DOB BISWEB API with correct data (doc__='01', job_type='NB', pre__filing_date='10/28/2014')
- When queried directly, the job is returned and passes all filters
- But the notebook doesn't query this BIN at all because it's shared

**Root Cause:**
In the "Step 3A: BIN/BBL Prep and Filtering" cell (cell 11), the code filters BINs to only include unique ones:

```python
bin_counts = hpd_multifamily_finance_new_construction_df['BIN'].value_counts()
unique_bins = bin_counts[bin_counts == 1].index.tolist()  # Only BINs that appear ONCE
...
if not is_bad_bin(b_clean) and b_clean in unique_bins:  # BUG: excludes duplicated BINs!
    bins.append(b_clean)
```

This was likely intended to deduplicate the BIN list for the API query, but it accidentally **excludes** BINs that are shared by multiple buildings in the dataset.

**Example:**
- BIN 2124684 is used by 2 buildings (44409 and 50104) - they're phases of the same development
- `bin_counts[2124684] = 2`
- `unique_bins` doesn't include 2124684 (because count ≠ 1)
- DOB query never queries BIN 2124684
- Both buildings get no DOB data

This affects any multi-phase development where HPD tracks phases as separate projects but DOB tracks them under one BIN/BBL.

**Fix:**
Changed the logic in cell 11 to use a set for deduplication (so we only query each BIN once) but don't exclude BINs just because they're shared:

```python
# BEFORE (buggy):
bin_counts = hpd_multifamily_finance_new_construction_df['BIN'].value_counts()
unique_bins = bin_counts[bin_counts == 1].index.tolist()
bins = []
for b in hpd_multifamily_finance_new_construction_df['BIN'].dropna():
    b_clean = ...
    if not is_bad_bin(b_clean) and b_clean in unique_bins:  # BUG!
        bins.append(b_clean)

# AFTER (fixed):
bins_set = set()  # Use a set to automatically deduplicate
for b in hpd_multifamily_finance_new_construction_df['BIN'].dropna():
    b_clean = ...
    if not is_bad_bin(b_clean):
        bins_set.add(b_clean)
bins = sorted(list(bins_set))
```

**Testing:**
- For BIN 2124684, should now query DOB and get job 220412541
- Both buildings 44409 and 50104 should get `earliest_dob_date = 2014-10-28`
- Both buildings share the same BIN/BBL, so both will get the same DOB dates
- Verify other shared BINs also get DOB data


---

## Address-Based Fallback Implementation (Tier 3)

**Status: Implemented**
**Date: Dec 3, 2025**

**Enhancement Description:**
Added address-based querying as a third-tier fallback for buildings that don't have DOB data after BIN and BBL queries. This handles cases where lot splits, BIN/BBL mismatches, or other identifier discrepancies prevent matching.

**Motivation:**
Analysis showed 67 buildings without DOB dates after BIN/BBL queries. Investigation revealed several causes:
1. **Lot splits/mergers**: Property BBL changed during development (e.g., lot 1 split into lot 200)
2. **BIN mismatches**: HPD and DOB databases have different BINs for the same building
3. **Address discrepancies**: HPD and DOB have slightly different addresses (e.g., "635" vs "655")
4. **Legitimate missing data**: Some HPD "New Construction" projects are filed as Alterations in DOB

**Example Case: Building 50497 (655 Morris Avenue)**
- HPD has: BIN 2002441, BBL 2024410001, address "635 MORRIS AVENUE"
- DOB has: BIN 2127027, BBL (different), address "655 MORRIS AVENUE"
- BIN query fails: BIN mismatch
- BBL query fails: Block 2441 has no NB filings
- Address query succeeds (if corrected): Job 220211205 at "655 MORRIS AVENUE" found

**Implementation:**
Added new Step 3C to `run_workflow.ipynb` after Step 3B (BBL fallback):

1. Identifies buildings without DOB matches after BIN and BBL queries
2. Extracts addresses (Borough, Number, Street) from HPD data
3. Queries DOB APIs using existing `query_dob_by_address()` function
4. Normalizes BIN/BBL columns and appends results to combined DOB data
5. Tracks how many additional projects now have DOB data

**Files Modified:**
- `run_workflow.ipynb`: Added 2 cells (markdown header + code) after cell 13
- Created backup: `run_workflow.ipynb.backup_20251202_191328`

**Code Structure:**
```python
# Step 3C: Address-Based Fallback
# 1. Get buildings without DOB data (from projects_with_no_dob)
# 2. Merge with original HPD data to get addresses
# 3. Extract unique addresses (Borough, Number, Street)
# 4. Call query_dob_by_address()
# 5. Normalize and append results to combined_dob_with_normalized_bbl_df
```

**Limitations:**
- Only matches when addresses are **exactly identical** between HPD and DOB
- Does not implement fuzzy matching or "nearby house number" logic
- Will not find DOB data for buildings with significant address discrepancies
- HPD "New Construction" projects filed as "Alteration" in DOB still won't match (by design)

**Benefits:**
- Handles lot splits/mergers automatically (address-based matching ignores lot numbers)
- Catches BIN/BBL mismatches where address data is correct
- No false positives (exact matching only)
- Uses existing, tested `query_dob_by_address()` function

**Testing:**
- Tested with building 50497: Address "655 MORRIS AVENUE" finds job 220211205 ✅
- Tested with sample of 5 buildings without DOB data: 0 matched (due to address discrepancies)
- Address fallback will help some cases but not all

**Future Enhancements:**
- Could add fuzzy address matching (e.g., try ±10 house numbers)
- Could parse DOB job descriptions for lot split notes (e.g., "NEW LOT 200")
- Could query entire block when individual lot fails (aggressive fallback)

---

## Missing DOB Date Extraction Cell

**Status: Fixed**
**Date: Dec 4, 2025**

**Bug Description:**
Cell 19 (COUNTING MATCHED DOB APPLICATIONS) threw `NameError: name 'hpd_multifamily_finance_new_construction_with_dob_date_df' is not defined` because the cell that creates this variable was missing (Cell 18 was an empty markdown cell).

**Symptoms:**
- Running the notebook in order caused NameError in Cell 19
- The dataframe `hpd_multifamily_finance_new_construction_with_dob_date_df` was never created

**Root Cause:**
Cell 18 was an empty markdown cell that should have contained the code to:
1. Filter DOB data to doc__='01' (BISWEB) and I1 (DOB NOW) filings
2. Extract earliest DOB date from date columns
3. Group by BIN to get earliest date per building
4. Create `hpd_multifamily_finance_new_construction_with_dob_date_df` by joining HPD data with DOB dates

This cell was likely lost during a notebook edit or was accidentally cleared.

**Fix:**
Added the DOB date extraction code to Cell 18:
- Filters `combined_dob_with_normalized_bbl_df` to relevant records (doc__='01' or I1 suffix)
- Identifies and parses date columns (pre__filing_date, paid, fully_paid, etc.)
- Extracts earliest DOB date per record using `get_earliest_date()` function
- Groups by BIN to get minimum earliest_dob_date per BIN
- Joins with HPD data using BIN matching (with BBL fallback)
- Creates `hpd_multifamily_finance_new_construction_with_dob_date_df` with columns:
  - All original HPD columns
  - `earliest_dob_date`: Earliest DOB milestone date
  - `earliest_dob_date_source`: Which column the date came from
  - `fully_permitted_date`: The fully_permitted date specifically

**Testing:**
- Notebook should now run through Cell 19 without NameError
- `hpd_multifamily_finance_new_construction_with_dob_date_df` should have 581 rows with DOB dates populated where available

---

## Condo BBL Fallback Not Implemented

**Status: Fixed**
**Date: Dec 4, 2025**

**Bug Description:**
Building 995045 at 45 Commercial Street (Greenpoint Landing H1H2) with BBL `3024727504` showed no NB filings, even though DOB has 8 NB filings for this property. The issue was that HPD has the **billing BBL** (lot 7504) but DOB permits are filed on the **base BBL** (lot 70 → BBL `3024720070`).

**Symptoms:**
- Building 995045 shows 0 NB filings in data quality report
- Address fallback also failed because it was searching the wrong BBL
- The `query_condo_lots_for_bbl` function existed but was never called in the workflow
- The function only searched base→billing direction, not billing→base

**Root Cause:**
1. **Condo fallback step was never implemented** - The workflow mentioned "BBL → Condo → Address" but the condo step was completely missing
2. **Existing function was incomplete** - `query_condo_lots_for_bbl` only looked up billing BBL from base BBL, but many HPD records have the billing BBL and need to find the base BBL
3. **No bidirectional lookup** - For condos, we need to:
   - Search input BBL in `condo_billing_bbl` → find base BBL
   - Search input BBL in `condo_base_bbl` → it IS the base BBL
   - Then search base BBL to get ALL related billing BBLs
   - Query DOB with all related BBLs

**Example:**
- HPD BBL: `3024727504` (billing BBL, lot 7504)
- Digital Tax Map shows: base BBL = `3024720070` (lot 70)
- DOB BISWEB has 8 NB filings on BBL `3024720070`
- Without condo fallback, we query lot 7504 which has no NB filings

**Fix:**
Added two new functions to `query_dob_filings.py`:
1. `get_all_condo_related_bbls(bbl)` - Bidirectional lookup that finds all related BBLs:
   - Searches `condo_billing_bbl` for input → gets base BBL
   - Searches `condo_base_bbl` for input → confirms it's a base BBL
   - Then gets ALL billing BBLs for that base BBL
   - Returns set of all related BBLs

2. `query_dob_for_condo_bbls(bbl_list)` - Queries DOB for NB filings on all condo-related BBLs:
   - For each input BBL, finds all related condo BBLs
   - Queries BISWEB and DOB NOW with padded block/lot values
   - Returns combined DataFrame with source='CONDO_FALLBACK_*'

Added "Tier 2.5: Condo Fallback" step in `run_workflow.ipynb`:
- Runs after BBL fallback (Tier 2) and before Address fallback (Tier 3)
- Checks unmatched projects for condo relationships
- Queries DOB for all related BBLs
- Includes results in combined DOB data

**API Details:**
- Uses NYC Digital Tax Map: Condominiums dataset (`p8u6-a6it.json`)
- Columns: `condo_base_bbl`, `condo_billing_bbl`, `condo_name`, etc.
- BISWEB requires **padded** block (5 digits) and lot (5 digits) values

**Testing:**
- Building 995045 (BBL `3024727504`) now finds 8 NB filings via condo fallback
- Job 321589704 with pre-filing date 06/01/2020 is correctly identified
- Test script: `testing_debugging/test_condo_fallback.py`

---

## DOB NOW Data Missing bbl_normalized Column

**Status: Fixed**
**Date: Dec 4, 2025**
**Commit SHA: 8ebfe17**

**Bug Description:**
DOB NOW data (from `query_dobnow_bin` and `query_dobnow_bbl`) had the `bbl` column normalized to 10 digits, but this was stored in `bbl`, not `bbl_normalized`. The counting logic and matching logic check for `bbl_normalized`, so BBL matching was failing for ALL DOB NOW records.

**Symptoms:**
- `DOBNOW I1 applications: 89` (records exist)
- `Buildings with DOBNOW I1 matches: 0` (but none matched!)
- Building 1004735 (GLENMORE MANOR) with BBL `3036920001` had DOB NOW New Building permits but showed 0 matches
- The building has a placeholder BIN (`3000000`) so BIN matching fails, and BBL matching was broken

**Root Cause:**
In cell 17, when DOB NOW data is processed:
```python
# This normalized bbl but saved to 'bbl', not 'bbl_normalized':
dob_now_bbl_df['bbl'] = dob_now_bbl_df['bbl'].apply(...)
# Missing: dob_now_bbl_df['bbl_normalized'] = dob_now_bbl_df['bbl']
```

The counting logic then checks:
```python
if hpd_bbl and 'bbl_normalized' in dobnow_i1_df.columns:
    bbl_matches = dobnow_i1_df[dobnow_i1_df['bbl_normalized'] == hpd_bbl]
```

Since `bbl_normalized` didn't exist in DOB NOW data, no BBL matches were ever found.

**Fix:**
Added `bbl_normalized = bbl` for both `dob_now_bin_df` and `dob_now_bbl_df`:
```python
if 'bbl' in dob_now_bbl_df.columns:
    dob_now_bbl_df['bbl'] = dob_now_bbl_df['bbl'].apply(...)
    dob_now_bbl_df['bbl_normalized'] = dob_now_bbl_df['bbl']  # CRITICAL: Set for matching
```

**Testing:**
- Building 1004735 (GLENMORE MANOR, BBL 3036920001) should now show DOB NOW matches
- Overall `Buildings with DOBNOW I1 matches` should be > 0
- Test script: `testing_debugging/investigate_glenmore_manor.py`

---

## String 'nan' Matching Causes Incorrect Date Assignment

**Status: Fixed**
**Date: Dec 4, 2025**

**Bug Description:**
Buildings with no BIN or BBL were showing `earliest_dob_date = 2003-09-11` and `fully_permitted_date = 2014-08-12` even though they had no DOB match. These specific dates were being incorrectly assigned to 29 buildings.

**Symptoms:**
- 29 buildings in data_quality_report.csv showed `earliest_dob_date = 2003-09-11` and `fully_permitted_date = 2014-08-12`
- Most of these buildings had `BIN = NaN` and `BBL = NaN` (no identifiers)
- Some showed `count_nb_bisweb_01_matched = 1` even though they had no BIN/BBL to match on
- The dates 2003-09-11 and 2014-08-12 appeared to come from nowhere

**Root Cause:**
When pandas converts NaN to string, it produces the literal string `'nan'`. The code was:

```python
# HPD side:
hpd_df['BIN_str'] = hpd_df['BIN'].astype(str).str.replace('.0', '')
# If BIN is NaN: BIN_str = 'nan'

# DOB side:
dob_df['bin_normalized'] = dob_df['bin__'].astype(str).str.replace('.0', '')
# If bin__ is NaN: bin_normalized = 'nan'

# Merge:
pd.merge(hpd_df, dob_df, on='BIN_str')  # 'nan' == 'nan' MATCHES!
```

The `.notna()` filter only checks for actual null values, NOT the string `'nan'`. So when grouping DOB records:
```python
dob_bin_min = dob_filtered_df[dob_filtered_df['bin_normalized'].notna()].groupby('bin_normalized')
```

This included a group where `bin_normalized = 'nan'` containing the earliest dates across ALL DOB records with missing BINs.

When merged with HPD data, ALL HPD buildings with missing BINs (which also have `BIN_str = 'nan'`) matched this group and received those dates.

**Affected Building IDs:**
927561, 965150, 983133, 986549, 988878, 989393, 1004426, 1008645, 974229, 994674, 977177, 955101, 996855, 1000227, 967338, 1017632, 1004751, 1013384, 1014223, 1014053, 1013778, 399127, 1004583, 1011389, 1015498, 965899, 1008199, 1017566

**Fix:**
1. Added filters to exclude string `'nan'` and empty strings when grouping by BIN/BBL:
```python
dob_bin_min = dob_filtered_df[
    dob_filtered_df['bin_normalized'].notna() & 
    (dob_filtered_df['bin_normalized'] != 'nan') &
    (dob_filtered_df['bin_normalized'] != '')
].groupby('bin_normalized', as_index=False).agg({...})
```

2. Created a helper function to properly normalize BIN strings:
```python
def normalize_bin_str(bin_val):
    if pd.isna(bin_val):
        return None
    s = str(bin_val).replace('.0', '')
    if s.lower() == 'nan' or s == '':
        return None
    return s

hpd_df['BIN_str'] = hpd_df['BIN'].apply(normalize_bin_str)
```

3. Applied the same fix to:
   - DOB date extraction BIN groupby (Cell 18)
   - DOB date extraction BBL groupby (Cell 18)
   - CO date extraction BIN groupby (Cell 23)
   - CO date extraction BBL groupby (Cell 23)

**Files Modified:**
- `run_workflow.ipynb`: Cells 18 and 23

**Testing:**
- Buildings without BIN/BBL should now have `earliest_dob_date = NaN` (no match)
- Buildings with valid BIN/BBL should still get correct dates
- The suspicious dates 2003-09-11 and 2014-08-12 should no longer appear for unmatched buildings

---

## Fully Permitted Date Missing DOB NOW Values

**Status: Fixed**
**Date: Dec 4, 2025**

**Bug Description:**
The `fully_permitted_date` column was only being populated from BISWEB's `fully_permitted` column, missing the equivalent data from DOB NOW which uses `first_permit_date` for the same milestone.

**Symptoms:**
- `fully_permitted_date` only had 806 non-null values (from BISWEB)
- 81 DOB NOW records with `first_permit_date` were being ignored
- Buildings with only DOB NOW data had no `fully_permitted_date` even when the data existed

**Root Cause:**
The code in Cell 18 only extracted from one column:
```python
if 'fully_permitted' in dob_filtered_df.columns:
    dob_filtered_df['fully_permitted_date'] = dob_filtered_df['fully_permitted']
```

DOB NOW uses `first_permit_date` instead of `fully_permitted` for the same milestone, but this was not being captured.

**Fix:**
Updated the code to combine both columns:
1. Start with BISWEB's `fully_permitted` column
2. Fill missing values with DOB NOW's `first_permit_date` 
3. For rows with both values (edge case), use the earlier date

```python
# Use fully_permitted from BISWEB
if 'fully_permitted' in dob_filtered_df.columns:
    dob_filtered_df['fully_permitted_date'] = pd.to_datetime(dob_filtered_df['fully_permitted'], errors='coerce')

# Fill in with first_permit_date from DOB NOW where fully_permitted is missing
if 'first_permit_date' in dob_filtered_df.columns:
    first_permit = pd.to_datetime(dob_filtered_df['first_permit_date'], errors='coerce')
    mask = dob_filtered_df['fully_permitted_date'].isna()
    dob_filtered_df.loc[mask, 'fully_permitted_date'] = first_permit[mask]
```

**Files Modified:**
- `run_workflow.ipynb`: Cell 18

**Testing:**
- `fully_permitted_date` should now have ~887 non-null values (806 + ~81 from DOB NOW)
- Buildings with DOB NOW data should now show permit dates

---

## DOB NOW I1 Counting Always Returns 0 Matches

**Status: Fixed**
**Date: Dec 4, 2025**

**Bug Description:**
Buildings with DOB NOW I1 filings showed correct dates but `count_new_building_dobnow_i1_matched = 0`. The counting logic found 90 DOB NOW I1 applications but matched 0 buildings.

**Symptoms:**
- Data quality report showed DOB NOW dates (e.g., 2021-04-07) for buildings
- But `count_new_building_dobnow_i1_matched` = 0 for all buildings
- Debug output showed: "DOBNOW I1 applications: 90" but "Buildings with DOBNOW I1 matches: 0"
- Debug confirmed BIN '3428961' existed in DOB NOW I1 with 2 records

**Root Cause:**
When BISWEB and DOB NOW data are concatenated, the combined dataframe has BOTH:
- `job__` column (from BISWEB, NaN for DOB NOW records)
- `job_filing_number` column (from DOB NOW, NaN for BISWEB records)

The counting code used an `if/elif/elif` chain:
```python
if 'job__' in bin_matches.columns:  # True! (column exists from BISWEB)
    matched_app_nums_i1.update(bin_matches['job__'].dropna().astype(str))  # Empty! (NaN for DOB NOW)
elif 'job_filing_number' in bin_matches.columns:  # Never reached!
    ...
```

Since `job__` exists in the combined df (from BISWEB data), the check `'job__' in bin_matches.columns` was True. But for DOB NOW records, `job__` is always NaN, so `.dropna()` returned empty, and `job_filing_number` was never checked.

**Fix:**
Changed from `if/elif/elif` to `if/if/if` and added check for non-null values:
```python
if 'job__' in bin_matches.columns and bin_matches['job__'].notna().any():
    matched_app_nums_i1.update(bin_matches['job__'].dropna().astype(str))
if 'application_number' in bin_matches.columns and bin_matches['application_number'].notna().any():
    matched_app_nums_i1.update(bin_matches['application_number'].dropna().astype(str))
if 'job_filing_number' in bin_matches.columns and bin_matches['job_filing_number'].notna().any():
    matched_app_nums_i1.update(bin_matches['job_filing_number'].dropna().astype(str))
```

Applied fix to all 6 matching sections (BIN/BBL/Address for both BISWEB and DOB NOW).

**Files Modified:**
- `run_workflow.ipynb`: Cell 19 (counting cell)

**Testing:**
- Building 998619 (BIN 3428961) should now show `count_new_building_dobnow_i1_matched = 2`
- Overall "Buildings with DOBNOW I1 matches" should be > 0

