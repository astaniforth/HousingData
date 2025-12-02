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

