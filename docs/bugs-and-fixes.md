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

