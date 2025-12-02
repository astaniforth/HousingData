# NYC Housing Data Analysis

Interactive Jupyter notebook for analyzing NYC affordable housing production—correlating HPD financing data with DOB permit filings and Certificates of Occupancy.

## Quick Start

```bash
pip install pandas matplotlib requests beautifulsoup4 plotly
jupyter notebook run_workflow.ipynb
```

## What the Notebook Does

The notebook (`run_workflow.ipynb`) is a modular workflow where you can run individual steps independently. Each cell shows dataframe views and statistics for inspection.

### Step 1: Fetch HPD Data
- Fetches HPD Affordable Housing Production data from NYC Open Data API
- Verifies local data against API (record count + sample comparison)
- Filters to **New Construction** projects only
- Shows program group breakdowns and unit counts

### Step 2: Analyze and Filter HPD Data
- Visualizes units financed by year with stacked bar charts
- Breaks down by Program Group (Multifamily Finance vs Multifamily Incentives)
- Shows Planned Tax Benefit distribution (421a, etc.)
- Filters to **Multifamily Finance Program** for DOB matching

### Step 3A: Query DOB Filings
Queries NYC Department of Buildings for new building permits using a multi-step fallback strategy:
1. **BISWEB BIN** — Query by Building Identification Number
2. **DOB NOW BIN** — Query DOB NOW system by BIN
3. **BISWEB BBL** — Fallback by Borough-Block-Lot for unmatched BINs
4. **DOB NOW BBL** — Fallback BBL query in DOB NOW
5. **Condo Billing BBL** — Handles condo billing BBL variations
6. **Address Search** — Final fallback using street address

Shows matching statistics: how many projects matched via each method.

### Step 3B: Query Certificate of Occupancy
- Queries DOB NOW CO and DOB CO APIs
- Extracts initial and final CO dates
- Shows CO filing statistics

### Step 4: Generate Timelines
- Joins HPD data with DOB filings by BIN/BBL
- Creates separate timelines for HPD-financed vs privately-financed projects
- Identifies projects without DOB matches

### Step 5: Summary
- Final workflow summary with record counts
- Lists unmatched projects for further investigation

## Data Sources

| Dataset | API Endpoint |
|---------|--------------|
| HPD Affordable Housing Production (Buildings) | [hg8x-zxpr](https://data.cityofnewyork.us/resource/hg8x-zxpr.json) |
| HPD Affordable Housing Production (Projects) | [hq68-rnsi](https://data.cityofnewyork.us/resource/hq68-rnsi.json) |
| DOB Job Application Filings (BISWEB) | [ic3t-wcy2](https://data.cityofnewyork.us/resource/ic3t-wcy2.json) |
| DOB NOW Job Applications | [w9ak-ipjd](https://data.cityofnewyork.us/resource/w9ak-ipjd.json) |
| DOB NOW Certificate of Occupancy | [pkdm-hqz6](https://data.cityofnewyork.us/resource/pkdm-hqz6.json) |
| DOB Certificate of Occupancy | [bs8b-p36w](https://data.cityofnewyork.us/resource/bs8b-p36w.json) |

## Project Structure

```
├── run_workflow.ipynb           # Main analysis notebook
├── fetch_affordable_housing_data.py  # HPD data fetching
├── query_dob_filings.py         # DOB permit queries
├── query_co_filings.py          # Certificate of Occupancy queries
├── data/
│   └── raw/                     # Raw HPD data files
├── output/                      # Generated outputs
├── docs/
│   ├── project_planning.md      # Task tracking
│   └── bugs-and-fixes.md        # Bug log
└── testing_debugging/           # Debug scripts
```

## Notebook Configuration

Each step has configuration options at the top of its cell:

| Variable | Purpose |
|----------|---------|
| `refresh_data` | Force fetch fresh HPD data from API |
| `skip_co` | Use existing CO data instead of querying |
| `skip_join` | Skip timeline creation |
| `skip_charts` | Skip chart generation |

## Key Features

- **In-memory workflow**: DataFrames pass between cells without file I/O
- **BBL fallback matching**: When BIN lookup fails, automatically tries BBL variations
- **Condo support**: Handles NYC condo billing BBL complexities
- **Data quality tracking**: Shows match rates and unmatched records
