# The Real Problem with Address Fallback

## Issue

Address fallback is finding DOB records (987 records!), but they're not being matched to HPD buildings because:

1. **Address fallback finds DOB records with DIFFERENT BINs** than HPD
   - Example: HPD has BIN "4000000" (placeholder)
   - DOB has BIN "4624528" (real BIN)
   
2. **Current matching logic only matches by BIN and BBL**
   - Tier 1: Match HPD BIN to DOB BIN
   - Tier 2: Match HPD BBL to DOB BBL
   - No Tier 3: Match HPD address to DOB address!

3. **Result**: Address fallback data is added to DOB dataset, but never matched to HPD

## Example

**Project 70082 (ROCKAWAY VILLAGE PHASE 4)**:
- HPD BIN: 4000000 (placeholder)
- HPD BBL: 4155377501
- HPD Address: 1605 VILLAGE LANE, QUEENS

**DOB Record (found by address fallback)**:
- DOB BIN: 4624528 (real BIN, different!)
- DOB Address: 1605 VILLAGE LANE, QUEENS

**Matching:**
- BIN match? NO (4000000 ≠ 4624528)
- BBL match? Maybe not (could be condo lot)
- **Address match? YES! But we don't check this!**

## Solution

Add address-based matching as Tier 3 in cell 13 (the matching cell).

After BIN and BBL matching, for remaining unmatched projects:
1. Extract their addresses
2. Match to DOB records by address (borough + house + street)
3. Create a mapping of Project IDs that match via address

This is similar to what we did in the standalone test script!

## Implementation

The matching cell needs to add:
```python
# Tier 3: Match by address for projects still unmatched
unmatched_on_bin_bbl = # projects not matched by BIN or BBL
for each unmatched project:
    get HPD address (borough, house, street)
    find DOB records with matching address
    if found:
        mark project as matched
```

The key is that we already have the DOB data (from address fallback), we just need to MATCH it differently!


