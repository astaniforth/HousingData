# NYC Housing Data Analysis

Interactive Jupyter workflow (`run_workflow.ipynb`) that links HPD Affordable Housing Production data to DOB permit filings and Certificates of Occupancy for Multifamily Finance Program new construction projects.

## Quick Start

```bash
pip install pandas matplotlib requests beautifulsoup4 plotly
jupyter notebook run_workflow.ipynb
```

## Notebook Flow (matches `run_workflow.ipynb`)

- **Setup**: Import helpers from `fetch_affordable_housing_data.py`, `query_dob_filings.py`, and `query_co_filings.py`.
- **Step 1: Fetch HPD data**
  - Options: `refresh_data`, `refresh_hpd_projects`, `hpd_output_path`
  - Downloads/refreshes HPD Buildings data, filters to **New Construction**, prints counts and sample rows.
- **HPD exploration**
  - Program group counts (rows vs. unique Project IDs) and unit totals.
  - Planned Tax Benefit breakdowns and a stacked bar of units by year for Multifamily Finance vs. Multifamily Incentives (colored by tax benefit).
  - 421a + 2025 start date sample.
- **Filter to Multifamily Finance Program (New Construction)**
  - Produces in-memory `hpd_multifamily_finance_new_construction_df` for DOB matching.
- **Step 3A: DOB filings (BIN/BBL)**
  - Cleans BINs (drops placeholders) and builds BIN/BBL lists.
  - Queries **BISWEB BIN**, **DOB NOW BIN**, then fallback **BISWEB BBL** and **DOB NOW BBL**.
  - Normalizes BIN/BBL, finds earliest DOB milestone per BIN/BBL, and joins back to HPD.
  - Exports `output/hpd_multifamily_finance_new_construction_with_dob_date.csv`.
- **Step 3B: Certificate of Occupancy**
  - Queries CO APIs by BIN, then BBL fallback for missing/placeholder BINs.
  - Joins earliest CO date into the DOB-joined table.
  - Exports `output/hpd_multifamily_finance_new_construction_with_all_dates.csv`.

## Configuration (in-notebook)

| Variable | Purpose |
|----------|---------|
| `refresh_data` | Fetch fresh HPD buildings data |
| `refresh_hpd_projects` | Refresh HPD projects cache before buildings fetch/verify |
| `hpd_output_path` | Local path for HPD buildings CSV |

## Data Sources

| Dataset | API Endpoint |
|---------|--------------|
| HPD Affordable Housing Production (Buildings) | [hg8x-zxpr](https://data.cityofnewyork.us/resource/hg8x-zxpr.json) |
| HPD Affordable Housing Production (Projects) | [hq68-rnsi](https://data.cityofnewyork.us/resource/hq68-rnsi.json) |
| DOB Job Application Filings (BISWEB) | [ic3t-wcy2](https://data.cityofnewyork.us/resource/ic3t-wcy2.json) |
| DOB NOW Job Applications | [w9ak-ipjd](https://data.cityofnewyork.us/resource/w9ak-ipjd.json) |
| DOB NOW Certificate of Occupancy | [pkdm-hqz6](https://data.cityofnewyork.us/resource/pkdm-hqz6.json) |
| DOB Certificate of Occupancy | [bs8b-p36w](https://data.cityofnewyork.us/resource/bs8b-p36w.json) |

## Outputs

- `output/hpd_multifamily_finance_new_construction_with_dob_date.csv` — HPD MFP new construction joined to earliest DOB milestone.
- `output/hpd_multifamily_finance_new_construction_with_all_dates.csv` — Adds earliest CO date to the above.

## Project Structure

```
├── run_workflow.ipynb           # Main analysis notebook
├── fetch_affordable_housing_data.py  # HPD data fetching/verification
├── query_dob_filings.py         # DOB permit queries (BISWEB/DOB NOW)
├── query_co_filings.py          # Certificate of Occupancy queries
├── data/
│   └── raw/                     # HPD source data cache
├── output/                      # Generated outputs
├── docs/
│   ├── project_planning.md
│   └── bugs-and-fixes.md
└── testing_debugging/
```

Notes:
- Current flow uses BIN/BBL fallbacks only; condo/address fallbacks are not executed in the notebook.
- Workflow writes CSVs for downstream use rather than remaining purely in-memory.
