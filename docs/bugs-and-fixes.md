# Bugs and Fixes Log

## Open Bugs

## Fixed Bugs

### Variable Name Error in Visualization Cell
**Bug title**: NameError: name 'full_hpd_df' is not defined in run_workflow.ipynb visualization cell

**Status**: Fixed

**Fix commit SHA**: afc7cb3

**One-line summary**: Corrected variable name from `full_hpd_df` to `hpd_df` to match the variable defined earlier in the notebook workflow.

### Missing Program Group Hatching in Bar Chart
**Bug title**: Program group hatches not visible in stacked bar chart visualization

**Status**: Fixed

**Fix commit SHA**: b247895

**One-line summary**: Added hatch pattern ('////') to Multifamily Finance Program bars to visually distinguish them from Multifamily Incentives Program bars in the stacked bar chart.

### Enhanced Chart Readability with Gridlines
**Bug title**: Chart lacks horizontal gridlines making it difficult to read values

**Status**: Fixed

**Fix commit SHA**: fd58b34

**One-line summary**: Added horizontal gridlines (`ax.grid(True, which='major', axis='y', alpha=0.3)`) to the bar chart for improved readability and value estimation.

### DOB API BBL Formatting Issue
**Bug title**: DOB API queries by BBL returning no results due to incorrect BBL component formatting

**Status**: Fixed

**Fix commit SHA**: 993f350

**One-line summary**: Fixed BBL decomposition to use 5-digit zero-padded blocks and 5-digit zero-padded lots for DOB API queries (e.g., BROOKLYN/01556/00003). **VERIFIED**: Test query returned 5 records, confirming the fix works.

### DOB API Query Order Fix
**Bug title**: DOB API queries not following proper fallback order

**Status**: Fixed

**Fix commit SHA**: a2fc23b

**One-line summary**: Updated notebook to implement proper API-specific fallback: BISWEB BIN → BISWEB BBL for failed BINs, DOB NOW BIN → DOB NOW BBL for failed BINs. Fixed BBL extraction using decompose_bbl function and added detailed logging.

### Notebook Syntax Errors
**Bug title**: Syntax errors in run_workflow.ipynb cell 10 preventing execution

**Status**: Fixed

**Fix commit SHA**: c305147

**One-line summary**: Fixed unterminated string literals from split print statements, removed duplicate if statements, and resolved emoji encoding issues. Cell now compiles successfully.

### Missing Function Imports
**Bug title**: NameError for DOB query functions not imported in notebook

**Status**: Fixed

**Fix commit SHA**: a959916

**One-line summary**: Added missing imports for query_dob_bisweb_bin, query_dob_bisweb_bbl, query_dobnow_bin, query_dobnow_bbl functions used by the updated DOB workflow.

### Incorrect Column References
**Bug title**: KeyError for BIN_normalized column that doesn't exist in dataframe

**Status**: Fixed

**Fix commit SHA**: c86e638

**One-line summary**: Replaced references to non-existent BIN_normalized column with BIN column, which is the actual column name in the HPD dataframe.

### DOB Workflow Order Fix
**Bug title**: DOB API queries doing BBL fallbacks after each individual BIN search instead of after all BIN searches

**Status**: Fixed

**Fix commit SHA**: dbcbc19

**One-line summary**: Restructured DOB workflow to: 1) BISWEB BIN for all, 2) DOB NOW BIN for all, 3) Combined BBL fallbacks only for buildings that failed BOTH BIN searches. More efficient than individual API fallbacks.

### Duplicate BBL Queries
**Bug title**: Same BBL being queried multiple times when multiple BINs map to the same BBL

**Status**: Fixed

**Fix commit SHA**: f70561c

**One-line summary**: Added deduplication of BBL tuples using `set()` before querying APIs, preventing duplicate queries for the same borough/block/lot combination.

### DOB NOW BBL Query Returns Zero Records
**Bug title**: DOB NOW BBL queries returning 0 records due to padded block/lot values

**Status**: Fixed

**Fix commit SHA**: 73f773c

**One-line summary**: DOB NOW API requires unpadded block/lot values (unlike BISWEB which needs padded). Updated query_dobnow_bbl to strip leading zeros before querying. Tested: now returns 6 records vs 0 before.

### Incorrect Variable and Column Names
**Bug title**: NameError for df_hpd_projects and incorrect column name 'Construction Type'

**Status**: Fixed

**Fix commit SHA**: 8c637d6

**One-line summary**: Fixed variable name from df_hpd_projects to hpd_df and column name from 'Construction Type' to 'Reporting Construction Type' to match actual notebook variables and HPD dataframe structure.

### Incorrect DOB Matching Logic
**Bug title**: All projects showing as unmatched because code tried to match on Project ID which doesn't exist in DOB data

**Status**: Fixed

**Fix commit SHA**: c01e834

**One-line summary**: Replaced incorrect Project ID matching with proper BIN/BBL joins. DOB data doesn't have HPD Project IDs, so matching must be done on BIN first, then BBL as fallback. Added debugging code to analyze unmatched projects.

### BBL Reconstruction Not Running for BISWEB Data
**Bug title**: BBL matching failing because BISWEB BBL queries don't return bbl column, only borough+block+lot, and reconstruction wasn't running

**Status**: Fixed

**Fix commit SHA**: c1ae3ea, dfa78f4, 7507624, f7e6e8c

**One-line summary**: Fixed BBL reconstruction to always run (not just when bbl column is missing). BISWEB BBL data needs reconstruction from borough+block+lot. Simplified bbl_normalized creation to use bbl if available, otherwise use reconstructed. Added debugging to track BBL matching. Fixed IndentationError from duplicate else statements. Fixed NameError by initializing matched_project_ids_bbl before use. **CRITICAL FIX**: Moved BBL merge outside else block - merge was only running when neither bbl nor bbl_reconstructed existed, preventing BISWEB BBL matches (e.g., BBL 3015560003) from being found. Now merge runs after normalization regardless of which path was taken.

### Permits on Different Lots in Same Block Not Found
**Bug title**: Permits filed on different lots in the same block (e.g., lot 00200 instead of base lot 00001) not being found by BBL queries

**Status**: Fixed

**Fix commit SHA**: c58ca50

**One-line summary**: Added `query_other_lots_in_block` function as a fallback after condo billing BBL fallback. When a base lot doesn't match, this searches all NB permits in the same block and filters by address hint from HPD data. This catches cases like project 50497 (655 Morris Avenue) where the permit is on lot 00200 instead of the base lot 00001. The fallback only runs for BBLs that still didn't match after condo fallback, ensuring we catch edge cases without excessive API calls.

## Known Issues
- None currently documented

