# API Limit Analysis

## Summary
Several NYC Open Data APIs have more than 50,000 records. We need to ensure we're fetching all data, not just the first 50k.

## APIs with >50k Records

| API | Total Records | Status | Impact |
|-----|---------------|--------|--------|
| **ZAP BBL** | 131,165 | ✅ **FIXED** | Was only getting 50k, now uses pagination |
| **DOB BISWEB** | 3,983,519 | ⚠️ **POTENTIAL ISSUE** | Query functions use limit=50000 per batch |
| **DOB NOW CO** | 69,349 | ⚠️ **POTENTIAL ISSUE** | Query function uses limit=50000 per batch |
| **DOB CO** | 142,314 | ⚠️ **POTENTIAL ISSUE** | Query function uses limit=50000 per batch |
| ZAP Project | 32,836 | ✅ OK | Under 50k |
| Affordable Housing | 8,604 | ✅ OK | Under 50k |
| HPD Projects | 5,252 | ✅ OK | Under 50k |

## Current Implementation Status

### ✅ Fixed: ZAP Data Integration (`zap_data_integration.ipynb`)
- **Issue**: Was fetching only 50k records with `$limit: 50000`
- **Fix**: Added pagination with `$offset` to fetch all 131k+ records
- **Status**: Fixed in commit dc12ec3

### ⚠️ Potential Issues

#### 1. DOB Query Functions (`query_dob_filings.py`)
**Functions affected:**
- `query_dob_bisweb_bin(limit=50000)`
- `query_dob_bisweb_bbl(limit=50000)`
- `query_dobnow_bin(limit=50000)`
- `query_dobnow_bbl(limit=50000)`
- `query_dob_by_address(limit=50000)`

**Current behavior:**
- These functions query specific BINs/BBLs in batches
- Each batch query has `$limit: 50000`
- If a single batch of BINs/BBLs returns more than 50k records, we'd miss data

**Risk assessment:**
- **Low risk** for typical use cases (querying specific BINs/BBLs from HPD data)
- **Higher risk** if querying large batches or entire blocks
- DOB BISWEB has 3.9M records total, but we're filtering by BIN/BBL

**Recommendation:**
- Add pagination to handle cases where a batch query returns exactly 50k records (might be truncated)
- Add warning/logging when a query returns exactly 50k records (might indicate truncation)

#### 2. CO Query Functions (`query_co_filings.py`)
**Function affected:**
- `query_co_api(url, bin_list, bin_column="bin", limit=50000)`

**Current behavior:**
- Queries specific BINs in batches of 50
- Each batch query has `$limit: 50000`
- If a single batch of 50 BINs returns more than 50k CO records, we'd miss data

**Risk assessment:**
- **Low risk** - unlikely that 50 BINs would have >50k CO records
- DOB NOW CO has 69k records total, DOB CO has 142k records total
- But we're filtering by specific BINs, so per-batch results should be much smaller

**Recommendation:**
- Add pagination to handle cases where a batch query returns exactly 50k records
- Add warning when a query returns exactly 50k records

#### 3. Affordable Housing Data (`fetch_affordable_housing_data.py`)
**Status:** ✅ **OK** - Has pagination implemented, but defaults to `limit=50000`
- Total records: 8,604 (well under 50k)
- Function has proper pagination with `$offset`
- If called with `limit=None`, will fetch all records
- If called with default `limit=50000`, will stop at 50k (but dataset is only 8.6k, so OK)

**Recommendation:**
- Consider changing default to `limit=None` to always fetch all records

## Recommendations

### High Priority
1. ✅ **DONE**: Fix ZAP BBL data fetching (completed)
2. Add pagination to DOB query functions when a batch returns exactly 50k records
3. Add pagination to CO query functions when a batch returns exactly 50k records

### Medium Priority
4. Add warning/logging when queries return exactly 50k records (might indicate truncation)
5. Consider changing `fetch_affordable_housing_data` default to `limit=None`

### Low Priority
6. Document the 50k limit in function docstrings
7. Add unit tests to verify pagination works correctly

## Implementation Pattern for Pagination

When a query might return >50k records, use this pattern:

```python
all_results = []
limit = 50000
offset = 0

while True:
    params = {
        '$where': query,
        '$limit': limit,
        '$offset': offset
    }
    
    response = requests.get(url, params=params, timeout=30)
    data = response.json()
    
    if not data:
        break
    
    all_results.extend(data)
    
    # If we got fewer than the limit, we've reached the end
    if len(data) < limit:
        break
    
    offset += limit
    
    # Warn if we got exactly the limit (might be truncated)
    if len(data) == limit:
        print(f"Warning: Query returned exactly {limit} records. Results might be truncated.")
```

## Testing

To verify no data is being missed:
1. Check if any queries return exactly 50,000 records
2. Compare record counts with API total counts
3. Test with known large datasets (e.g., entire blocks)

