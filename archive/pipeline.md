# Pipeline Overview
The notebook pulls the HPD affordable housing building dataset (optionally refreshing it from NYC Open Data), enriches it with project-level metadata, and filters to new construction projects—specifically the Multifamily Finance Program subset. It then queries DOB BISWEB and DOB NOW for New Building filings by BIN with a BBL fallback, normalizes DOB identifiers, and matches the results back to HPD projects. Certificate of Occupancy filings are queried by BIN, and timelines can be assembled to show HPD financing, DOB permitting, and CO milestones per building.

## Source Datasets
- Affordable_Housing_Production_by_Building (NYC Open Data `hg8x-zxpr`), keyed by `Project ID`, `Building ID`, `BIN`, `BBL`, plus address fields.
- Affordable_Housing_Production_by_Project (NYC Open Data `hq68-rnsi`), project-level attributes such as `program_group`, `planned_tax_benefit`, `project_start_date`, `project_completion_date` keyed by `project_id`.
- DOB BISWEB Job Applications (`ic3t-wcy2`), New Building (`job_type='NB'`) filings keyed by `bin__` and/or (`borough`, `block`, `lot`).
- DOB NOW Job Applications (`w9ak-ipjd`), New Building filings keyed by `bin` and/or (`borough`, `block`, `lot`).
- DOB NOW Certificate of Occupancy (`pkdm-hqz6`), keyed by `bin`.
- DOB Certificate of Occupancy (`bs8b-p36w`), keyed by `bin_number`.
- Optional condo billing lookup (`p8u6-a6it`) is available in helpers for condo BBL translation but is not exercised in the notebook run.

## Transformation Steps (Pseudocode)
FALLBACK A ON key means: start with the current dataset and, for rows missing on `key`, pull them from A (without duplicating keys).

### HPD preparation
```sql
hpd_enriched =
  hpd_buildings_raw
    LEFT JOIN hpd_projects
      ON hpd_buildings_raw.Project_ID = hpd_projects.project_id
    SELECT
      hpd_buildings_raw.*,
      COALESCE(hpd_buildings_raw.Program_Group, hpd_projects.program_group) AS Program_Group,
      COALESCE(hpd_buildings_raw.Extended_Affordability_Status, hpd_projects.extended_affordability_status) AS Extended_Affordability_Status,
      COALESCE(hpd_buildings_raw.Prevailing_Wage_Status, hpd_projects.prevailing_wage_status) AS Prevailing_Wage_Status,
      COALESCE(hpd_buildings_raw.Planned_Tax_Benefit, hpd_projects.planned_tax_benefit) AS Planned_Tax_Benefit,
      COALESCE(hpd_buildings_raw.Project_Start_Date, hpd_projects.project_start_date) AS Project_Start_Date,
      COALESCE(hpd_buildings_raw.Project_Completion_Date, hpd_projects.project_completion_date) AS Project_Completion_Date
```
Explanation: Enrich building-level HPD data with project attributes, filling empty HPD columns from the projects table when present.

```sql
hpd_new_construction =
  hpd_enriched
    WHERE Reporting_Construction_Type = 'New Construction'
```
Explanation: Keep only new construction records from HPD.

```sql
hpd_mfp_new_construction =
  hpd_new_construction
    WHERE Program_Group = 'Multifamily Finance Program'
```
Explanation: Focus the workflow on Multifamily Finance Program new construction projects.

### DOB New Building filings retrieval
```sql
dob_search_bins_bbl =
  hpd_mfp_new_construction
    WHERE BIN NOT IN ('1000000','2000000','3000000','4000000','5000000')
    SELECT DISTINCT
      BIN AS bin_normalized,
      BBL AS bbl_normalized,
      borough_from_bbl AS borough,
      block_padded AS block,
      lot_padded AS lot
```
Explanation: Prepare unique BINs (excluding placeholder BINs) and decomposed BBL components for DOB queries.

```sql
dob_bisweb_bin_filings =
  DOB_BISWEB_Job_Applications
    WHERE job_type = 'NB'
      AND bin__ IN dob_search_bins_bbl.bin_normalized
    SELECT *, bin__ AS bin_normalized
```
Explanation: Pull BISWEB New Building filings by BIN.

```sql
dob_now_bin_filings =
  DOB_NOW_Job_Applications
    WHERE job_type = 'New Building'
      AND bin IN dob_search_bins_bbl.bin_normalized
    SELECT *, bin AS bin_normalized
```
Explanation: Pull DOB NOW New Building filings by BIN.

```sql
unmatched_projects_by_bin =
  hpd_mfp_new_construction
    WHERE BIN NOT IN (dob_bisweb_bin_filings.bin_normalized UNION dob_now_bin_filings.bin_normalized)
    SELECT DISTINCT Project_ID, BBL, borough_from_bbl AS borough, block_padded AS block, lot_padded AS lot
```
Explanation: Identify HPD projects whose BINs had no DOB matches and route them to BBL fallback.

```sql
dob_bisweb_bbl_filings =
  DOB_BISWEB_Job_Applications
    WHERE job_type = 'NB'
      AND (borough, block, lot) IN unmatched_projects_by_bin.(borough, block, lot)
    SELECT *, bin__ AS bin_normalized
```
Explanation: BISWEB fallback search by BBL for projects missed by BIN lookup.

```sql
dob_now_bbl_filings =
  DOB_NOW_Job_Applications
    WHERE job_type = 'New Building'
      AND (borough, block, lot) IN unmatched_projects_by_bin.(borough, block, lot)
    SELECT *, bin AS bin_normalized
```
Explanation: DOB NOW fallback search by BBL for projects missed by BIN lookup (uses unpadded block/lot per API).

```sql
dob_bin_filings =
  dob_bisweb_bin_filings
    UNION ALL dob_now_bin_filings

dob_bbl_filings =
  dob_bisweb_bbl_filings
    UNION ALL dob_now_bbl_filings

combined_dob_filings =
  dob_bin_filings
    FALLBACK dob_bbl_filings
      ON bin_normalized
    SELECT
      *,
      COALESCE(bbl, CONCAT(borough_code, block_padded, lot_padded)) AS bbl_normalized
```
Explanation: Prefer BIN-based DOB filings, then FALLBACK to BBL-derived filings where BINs had no hits; normalize BIN/BBL identifiers and reconstruct missing BBLs from borough/block/lot.

```sql
hpd_matched_on_bin =
  hpd_mfp_new_construction
    JOIN combined_dob_filings
      ON hpd_mfp_new_construction.BIN_normalized = combined_dob_filings.bin_normalized

hpd_matched_on_bbl =
  hpd_mfp_new_construction
    WHERE Project_ID NOT IN hpd_matched_on_bin.Project_ID
    JOIN combined_dob_filings
      ON hpd_mfp_new_construction.BBL_normalized = combined_dob_filings.bbl_normalized

dob_matched_projects =
  hpd_matched_on_bin
    FALLBACK hpd_matched_on_bbl
      ON Project_ID
```
Explanation: First match HPD projects to DOB filings by BIN; FALLBACK to BBL matches only for projects still unmatched, keeping a single record per project.

### Certificate of Occupancy retrieval
```sql
co_filings_combined =
  DOB_NOW_CO
    WHERE bin IN dob_search_bins_bbl.bin_normalized
    SELECT *, bin AS bin_normalized
    UNION ALL DOB_CO
    WHERE bin_number IN dob_search_bins_bbl.bin_normalized
    SELECT *, bin_number AS bin_normalized
```
Explanation: Query both CO datasets by BIN and union them with a normalized BIN identifier.

### Timeline assembly (when run)
```sql
timeline_events =
  hpd_mfp_new_construction
    SELECT BIN AS bin_normalized, Project_Start_Date AS event_date, 'HPD financing submitted' AS event
    UNION ALL hpd_mfp_new_construction
    SELECT BIN AS bin_normalized, Project_Completion_Date AS event_date, 'HPD financing completed' AS event
    UNION ALL combined_dob_filings
    SELECT bin_normalized, filing_or_approval_date AS event_date, 'DOB NB filing/approval' AS event
    UNION ALL co_filings_combined
    SELECT bin_normalized, co_issue_date AS event_date, 'Certificate of Occupancy issued' AS event
    ORDER BY bin_normalized, event_date
```
Explanation: Build per-BIN timelines that combine HPD financing dates, DOB filing/approval milestones, and CO issuance events; split into HPD-financed vs privately financed timelines when a `Financing Type` column exists.

## Fallback Logic
- Column-level: Program group, affordability status, prevailing wage status, planned tax benefit, and start/completion dates use `COALESCE(project_data, building_data)` from HPD projects when building-level fields are empty (see `hpd_enriched`).
- DOB search priority: primary search by BIN in both BISWEB and DOB NOW; FALLBACK to BBL queries for BINs with no hits.
- Identifier completion: For DOB records missing `bbl`, reconstruct `bbl_normalized` from `borough`/`block`/`lot`.
- Timeline generation: if `Financing Type` is absent, `create_separate_timelines` falls back to a single combined timeline instead of split HPD/private views.

## Output Datasets
- `hpd_new_construction_df` (in memory) / `data/raw/Affordable_Housing_Production_by_Building.csv`: HPD buildings filtered to New Construction with project-level enrichment.
- `hpd_multifamily_finance_new_construction_df` (in memory): subset for Multifamily Finance Program buildings used for all downstream matching.
- `combined_dob_filings` (in memory): stacked BISWEB/DOB NOW New Building filings with normalized BIN/BBL for matching.
- `co_filings_df` (in memory): union of DOB NOW CO and DOB CO filings keyed by BIN.
- `hpd_timeline_df` and `private_timeline_df` (temp CSVs when timeline code runs): BIN-level event sequences for HPD-financed and privately financed projects (or a single combined timeline if financing type is unavailable). Produced ad hoc when the notebook executes Step 4.

## Assumptions & Caveats
- Only HPD records with `Reporting Construction Type = 'New Construction'` and `Program Group = 'Multifamily Finance Program'` are analyzed; other program groups are excluded from DOB/CO searches.
- BIN placeholders (`1000000`–`5000000`) and BINs shared by multiple projects are excluded from DOB API calls to avoid noise.
- DOB BBL fallback relies on accurate `BBL` values in HPD data; reconstructed BBLs from borough/block/lot may be imperfect for BISWEB rows.
- DOB and CO unions do not deduplicate overlapping filings beyond BIN/BBL normalization; downstream consumers may need additional de-duping.
- Timeline outputs depend on DOB/CO query results being present; in the notebook they are created via temporary files and may not persist unless saved explicitly.
