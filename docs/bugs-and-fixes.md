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

## C of O Date Not Showing Earliest Date - Missing Join Logic

**Status: Open**
**Date: Dec 2, 2025**

**Bug Description:**
The C of O dates in the output CSV are not showing the earliest dates. For example, Project 64608 (142-150 SOUTH PORTLAND, BIN 3427387) shows October 30, 2025 as the earliest CO date, but the actual earliest CO (Initial CO) was issued on February 28, 2024.

**Symptoms:**
- The `earliest_co_date` column in `output/hpd_multifamily_finance_new_construction_with_all_dates.csv` shows incorrect (later) dates
- For BIN 3427387: showing `2025-10-30` instead of `2024-02-28`
- The output CSV file exists but appears to be from an old run

**Root Cause:**
The current `run_workflow.ipynb` notebook is **missing the CO data join logic** that existed in a previous version (archive/run_workflow.ipynb.bak2). The notebook:
1. Queries CO data in Step 3B (creates `co_filings_df`)
2. Creates timeline files in Step 4 using `create_separate_timelines()`
3. BUT never joins the CO data with the HPD dataframe to add the `earliest_co_date` column
4. The output CSV files are from a previous run and are now stale

Additionally, there's a date parsing issue:
- DOB NOW CO API (`pkdm-hqz6`) returns dates like `02/28/2024 14:34:05` in the `c_of_o_issuance_date` column
- DOB CO API (`bs8b-p36w`) returns dates like `2025-10-30T00:00:00.000` in the `c_o_issue_date` column
- Both formats need to be parsed correctly and compared to find the earliest date

**Investigation Results:**
Querying the APIs directly for BIN 3427387 shows:
- DOB NOW CO API: 9 records including Initial CO on `02/28/2024 14:34:05`
- DOB CO API: 1 record with Final CO on `2025-10-30T00:00:00.000`
- The correct earliest CO date should be `2024-02-28`

**Fix:**
Need to add a new cell to the notebook (after Step 3B, before Step 4) that:
1. Identifies all CO date columns in `co_filings_df` (look for columns with 'date' and 'issue' or 'issuance')
2. Converts all date columns to datetime, handling both MM/DD/YYYY HH:MM:SS and ISO formats
3. For each row in `co_filings_df`, finds the earliest date among all CO date columns
4. Groups by `bin_normalized` to get the minimum `earliest_co_date` per BIN
5. Merges this with the HPD dataframe on BIN (with BBL fallback for rows without BIN matches)
6. Exports to `output/hpd_multifamily_finance_new_construction_with_all_dates.csv`

**Code Changes Needed:**
Add CO join cell based on the logic from `archive/run_workflow.ipynb.bak2` lines 6547-6650, with proper date parsing to handle both date formats.

**Testing:**
- For BIN 3427387, should correctly identify `2024-02-28` as the earliest CO date (from Initial CO)
- Should work correctly for BINs with only one CO record
- Should handle both date formats correctly

