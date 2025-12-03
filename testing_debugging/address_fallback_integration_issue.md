# SOLUTION: Address Fallback Integration Issue

## Problem Identified

The address fallback (Step 3C) runs AFTER the HPD-DOB matching logic (cell 14), so the new DOB records found by address fallback are never matched to HPD buildings.

**Current flow:**
1. Cell 11-12: Query DOB by BIN
2. Cell 13: Query DOB by BBL (fallback) → creates `combined_dob_with_normalized_bbl_df`
3. Cell 14: **MATCH HPD to DOB** → creates `mfp_projects_without_dob` (27 projects)
4. Cell 15 (Step 3C): Address fallback → appends to `combined_dob_with_normalized_bbl_df` (finds 5 matches)
5. Cell 16+: Continue with old matching results

**Result:** The 5 projects found by address fallback are added to DOB data but never matched back to HPD!

## Solutions

### Option 1: Move Address Fallback Before Matching (RECOMMENDED)
Move Step 3C to run at the END of cell 13, so all DOB data (BIN + BBL + ADDRESS) is collected before matching happens.

**New flow:**
1. Cell 11-12: Query DOB by BIN
2. Cell 13: 
   - Query DOB by BBL (fallback)
   - **ADDRESS FALLBACK (use mfp_projects_without_dob from earlier)**
   - Combined result → `combined_dob_with_normalized_bbl_df`
3. Cell 14: MATCH HPD to complete DOB dataset

### Option 2: Re-run Matching After Address Fallback
Add matching logic at the end of Step 3C to update the results.

### Option 3: Create Step 3C as its own mini-workflow
Have Step 3C not only query but also match and update the final output.

## Implementation Plan

The cleanest solution is **Option 1**: Integrate address fallback into cell 13 so all DOB querying happens in one place before matching.

However, there's a catch: Cell 13 runs BBL fallback for `unmatched_projects_df` (from cell 11), but we don't know which projects are STILL unmatched after cell 13's BBL queries until cell 14 calculates `mfp_projects_without_dob`.

**Better approach**: Add address fallback at the END of cell 13, querying addresses for ALL projects that don't have matches yet (check against `combined_dob_with_normalized_bbl_df`).


