# Fix for Earliest DOB Date Extraction

## Problem
When multiple DOB applications exist for the same BIN, the code selects the most recent application (to avoid withdrawn/abandoned ones), but then only finds the earliest date from that selected application. This misses earlier dates that exist in older applications for the same BIN.

## Solution
After selecting the most recent application per BIN, we should find the earliest date across **ALL** applications for that BIN, not just from the selected application.

## Corrected Code Logic

```python
# After filtering to doc__ = '01' (BISWEB) or ending with 'I1' (DOB NOW)
# and sorting by application_date descending...

# Step 1: Get the most recent application per BIN (to avoid withdrawn ones)
dob_bin_most_recent = dob_bin_dates_sorted.groupby('bin_normalized', as_index=False).first()

# Step 2: For each BIN, find the earliest date across ALL applications (not just most recent)
# This ensures we get the true earliest milestone date
dob_bin_min = pd.DataFrame(columns=['bin_normalized', 'earliest_dob_date', 'earliest_dob_date_source', 'application_number'])

for bin_val in dob_bin_most_recent['bin_normalized'].unique():
    # Get ALL applications for this BIN (not just the most recent one)
    bin_applications = dob_bin_dates_sorted[dob_bin_dates_sorted['bin_normalized'] == bin_val].copy()
    
    # Find the earliest date across ALL applications for this BIN
    earliest_date = pd.NaT
    earliest_source = None
    earliest_application_number = None
    
    for idx, row in bin_applications.iterrows():
        result = _get_earliest_date(row, date_cols)
        if isinstance(result, tuple) and len(result) >= 2:
            date_val, source_col = result
            if pd.notna(date_val) and (pd.isna(earliest_date) or date_val < earliest_date):
                earliest_date = date_val
                earliest_source = source_col
                # Get application number from the row that has the earliest date
                earliest_application_number = get_application_number(row)
    
    # Add to result dataframe
    if pd.notna(earliest_date):
        dob_bin_min = pd.concat([
            dob_bin_min,
            pd.DataFrame([{
                'bin_normalized': bin_val,
                'earliest_dob_date': earliest_date,
                'earliest_dob_date_source': earliest_source,
                'application_number': earliest_application_number
            }])
        ], ignore_index=True)

# Similar logic for BBL grouping
```

## Key Changes
1. **After selecting most recent application**: Store the BIN values from `dob_bin_most_recent`
2. **For each BIN**: Filter the original `dob_bin_dates_sorted` dataframe to get ALL applications for that BIN
3. **Find earliest across all**: Iterate through all applications for the BIN and find the truly earliest date
4. **Preserve application info**: Keep track of which application number had the earliest date

This ensures we:
- Still filter out withdrawn/abandoned applications (by selecting most recent first)
- But find the earliest date across all valid applications for that BIN
- Get the correct earliest milestone date (e.g., `2011-06-14` instead of `2013-12-31`)

