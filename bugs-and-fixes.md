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
Updated the filtering logic to use the existing `is_bad_bin()` function instead of the undefined `bad_bins` variable. The cell now properly excludes placeholder/bad BINs while showing duplicated ones.

**Code Change:**
```python
# Before (broken):
hpd_multifamily_finance_new_construction_df[
    (hpd_multifamily_finance_new_construction_df["BIN"].duplicated(keep=False)) &
    (~hpd_multifamily_finance_new_construction_df["BIN"].astype(str).isin(bad_bins))
]

# After (fixed):
hpd_multifamily_finance_new_construction_df[
    (hpd_multifamily_finance_new_construction_df["BIN"].duplicated(keep=False)) &
    (~hpd_multifamily_finance_new_construction_df["BIN"].astype(str).apply(is_bad_bin))
]
```

**What is_bad_bin does:**
The `is_bad_bin()` function identifies placeholder BINs by checking for:
- Null/NaN values
- Borough placeholder BINs: '1000000', '2000000', '3000000', '4000000', '5000000'

**Testing:**
- Cell now executes without errors
- No linter errors remain

## BBL Matching Not Working in HPD-DOB Join

**Status: Fixed**
**Date: Dec 2, 2025**
**Commit SHA: 0e75fac**

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
2. Added `format_bbl()` function to properly format BBL values (handles int, float, and string types, ensures 10-digit zero-padding)
3. Changed `dob_bbl_min` grouping from `groupby('bbl_str')` to `groupby('bbl_normalized')` with filtering for non-null values: `dob_bin_dates[dob_bin_dates['bbl_normalized'].notna()].groupby(...)`
4. Updated merge statement to rename `bbl_normalized` to `BBL_str` instead of `bbl_str`

**Testing:**
- Code now uses `bbl_normalized` which includes both original and reconstructed BBLs
- BBL matching should now work for records that only have borough/block/lot information
- Proper formatting ensures all BBLs are 10-digit zero-padded strings for consistent matching

## doc__ Column Type Mismatch Causing Filter Failure

**Status: Fixed**
**Date: Dec 2, 2025**

**Bug Description:**
The `doc__` column in the DOB BISWEB data is stored as integers (1, 2, 3) but the filter was comparing against the string `'01'`. This caused the filter `doc__ == '01'` to return 0 records, meaning ALL doc__ values were included instead of just doc__ = 01.

**Symptoms:**
- For Building ID 927748, BIN 2129098, the `earliest_dob_date` showed 2013-12-31 instead of 2011-06-14
- The `earliest_dob_date_source` correctly showed `pre__filing_date`, but the date value was from doc__ = 02 (2013-12-31) instead of doc__ = 01 (2011-06-14)
- The API returns multiple records for job 220124381 with different doc__ values:
  - doc__ = 01: pre__filing_date = 06/14/2011 (correct)
  - doc__ = 02: pre__filing_date = 12/31/2013 (wrong one being selected)
  - doc__ = 03: pre__filing_date = 12/31/2013 (wrong one being selected)

**Root Cause:**
The CSV file stores `doc__` as integers (1, 2, 3) but the code compared against string `'01'`:
```python
dob_df['doc__'] == '01'  # Returns False because 1 != '01'
```

**Fix:**
Changed the comparison to convert the integer to a zero-padded string:
```python
dob_df['doc__'].astype(str).str.zfill(2) == '01'  # 1 -> '01' -> True
```

**Testing:**
- For BIN 2129098, should now correctly get pre__filing_date = 2011-06-14 from doc__ = 01
- Filter now correctly excludes doc__ = 02 and 03 records

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

## DOB NOW Date Columns Not Being Detected

**Status: Fixed**
**Date: Dec 2, 2025**

**Bug Description:**
DOB NOW data was not being picked up in the HPD-DOB join workflow. The date column detection logic was only looking for space-separated column names like 'filing date' and 'first approved date', but DOB NOW API returns columns with underscores like 'filing_date', 'first_permit_date', and 'approved_date'.

**Symptoms:**
- DOB NOW records were present in the combined dataframe but their dates were not being extracted
- `earliest_dob_date` was NaT for rows that should have matched DOB NOW records
- Date column detection was only finding BISWEB date columns, not DOB NOW columns

**Root Cause:**
The date column detection logic in the join cell was checking for exact matches with space-separated names:
- `'filing date'`
- `'first approved date'`

But DOB NOW API actually returns columns with underscores:
- `filing_date`
- `first_permit_date`
- `approved_date`
- `first_approved_date`

**Fix:**
Updated the date column detection to check for both space-separated and underscore-separated versions of DOB NOW date columns. Also added partial matching to catch variations in column names.

**Code Changes:**
1. Expanded DOB NOW date column detection to check for both space and underscore versions
2. Added checks for: `filing_date`, `first_permit_date`, `first_approved_date`, `approved_date`
3. Added partial matching using `startswith()` to catch column name variations
4. Added debug output to verify DOB NOW data presence and show which date columns are found

**Testing:**
- Date column detection now finds DOB NOW columns with underscores
- Debug output helps verify DOB NOW data is included in the combined dataframe
- Both BISWEB and DOB NOW date columns are now properly detected and used

