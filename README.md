# Housing Data Analysis

Pipeline for connecting HPD affordable housing production data to DOB filings and Certificates of Occupancy (CO), and visualizing timelines by BIN.

## Quick Start (orchestrated run)
- Install deps: `pip install pandas matplotlib requests beautifulsoup4 plotly`
- Run everything with defaults: `python run_workflow.py`
  - Automatically verifies/fetches latest HPD data, adds financing classification, enriches with DOB/CO data, and generates timeline charts
- Useful flags:
  - `--refresh-hpd` force fetch fresh HPD data from NYC Open Data
  - `--bin-file all_construction_bins.txt` supply your own BIN list for CO queries
  - `--skip-dob` / `--skip-co` / `--skip-join` / `--skip-charts` reuse existing outputs
  - `--dob-search-source my_bins.csv` run DOB search against a custom CSV (enables BBL fallback if it has BIN + BBL columns)

Outputs land in `data/raw/`, `data/processed/`, `data_quality_reports/`, and `output/`.

## Workflow Steps
1. **Data Verification & Fetching**: Verify local HPD data matches NYC Open Data API (record count + sample comparison); fetch fresh data if needed. Raw data saved to `data/raw/`.
2. **Financing Classification**: Query LL44 funding database to classify projects as HPD-financed vs privately-financed. Processed data saved to `data/processed/`.
3. **DOB/CO Data Enrichment**: Query DOB APIs for New Building filings and Certificate of Occupancy data using BINs/BBLs. Enriched data saved to `data/processed/`.
4. **Timeline Charts**: Generate multi-page PDF timeline charts showing HPD production dates, DOB filings, and CO events by financing type. Charts saved to `output/`.
5. **Data Quality Reporting**: `data_quality.py` tracks completeness, validation metrics, and generates Sankey diagrams showing data flow through the pipeline. Reports saved to `data_quality_reports/`.

## Key Scripts (manual usage)
- `fetch_affordable_housing_data.py` — fetch HPD data; `--verify` option compares local data with API
- `query_ll44_funding.py <hpd_csv>` — adds `Financing Type` based on LL44 funding (outputs to `data/processed/`)
- `query_dob_filings.py <bins_or_csv>` — DOB NB/New Building filings with BBL fallback (outputs to `data/processed/`)
- `query_co_filings.py <bin_list.txt>` — CO filings for BINs (outputs to `data/processed/`)
- `HPD_DOB_Join_On_BIN.py <hpd_csv> <dob_filings_csv> [co_filings_csv]` — joins timelines
- `create_timeline_chart.py <timeline_csv>` — PDF timeline chart for the provided timeline
- `run_workflow.py` — orchestrates the complete 4-step pipeline with data quality tracking

## Inputs and Outputs
- **Raw Data**: `data/raw/Affordable_Housing_Production_by_Building.csv` (verified against NYC Open Data API)
- **Processed Data** (`data/processed/`):
  - `*with_financing.csv` (HPD + LL44 financing classification)
  - `*_dob_filings.csv` and `*_dob_filings_summary.csv` (DOB New Building filings)
  - `*_co_filings.csv` and `*_co_filings_summary.csv` (Certificate of Occupancy data)
  - `*_timeline.csv` (HPD financed and privately financed timelines)
- **Outputs**:
  - `output/*_timeline_chart.pdf` and `output/*_timeline_data.csv` (timeline visualizations)
  - `data_quality_reports/*` (data quality reports and Sankey diagrams showing data flow)

## Data Sources
- HPD Affordable Housing Production
- LL44 Funding: [NYC Open Data](https://data.cityofnewyork.us/resource/gmi7-62cd.json)
- DOB Job Application Filings: [ic3t-wcy2](https://data.cityofnewyork.us/Housing-Development/DOB-Job-Application-Filings/ic3t-wcy2)
- DOB NOW Job Applications: [w9ak-ipjd](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Job-Application-Filings/w9ak-ipjd)
- DOB NOW Certificate of Occupancy: [pkdm-hqz6](https://data.cityofnewyork.us/resource/pkdm-hqz6.json)
- DOB Certificate Of Occupancy: [bs8b-p36w](https://data.cityofnewyork.us/resource/bs8b-p36w.json)

## Notes
- The workflow automatically verifies local HPD data against the NYC Open Data API using record count and sample comparison before proceeding.
- BBL fallback in `query_dob_filings.py` requires a CSV with BIN and BBL columns (e.g., the HPD dataset).
- API calls hit NYC Open Data and may take several minutes; using `--skip-dob`/`--skip-co` flags reuses existing processed data.
- Data quality reports include Sankey diagrams showing how the dataset is filtered and enriched through each pipeline stage.
- The pipeline is designed to be extensible - additional enrichment steps can be added before chart generation.
