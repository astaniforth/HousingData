# Housing Data Analysis - Project Planning

## Overview
This project analyzes affordable housing production in NYC, correlating HPD financing data with DOB permit filings and Certificate of Occupancy data.

## Current Workflow
1. Extract BINs from Affordable_Housing_Production_by_Building.csv
2. Query DOB APIs for NB/New Building filings using query_dob_filings.py
3. Join HPD and DOB data by BIN using HPD_DOB_Join_On_BIN.py
4. Visualize timelines using create_timeline_chart.py

## Tasks

### Completed Tasks
- [x] Set up project structure with HPD and DOB data integration
- [x] Create DOB filing queries for new construction projects
- [x] Separate DOB query types into 4 distinct functions (BISWEB BIN, BISWEB BBL, DOBNOW BIN, DOBNOW BBL)
- [x] Implement specific query execution order: BISWEB BIN → DOB NOW BIN → BISWEB BBL (fallback) → DOB NOW BBL (fallback)
- [x] Ensure proper padding of block and lot values in BBL decomposition for API compatibility
- [x] Join HPD financing data with DOB application data
- [x] Generate timeline visualizations showing DOB and HPD timelines

### Completed Tasks
- [x] Research CO data sources and API structure
- [x] Create CO data query script
- [x] Integrate CO events into timeline data structure
- [x] Update timeline chart to include CO events
- [x] Test CO data integration with existing workflow
- [x] Update documentation and README
- [x] Add final CO events in addition to initial CO events
- [x] Differentiate between initial and final CO based on filing_type/issue_type columns
- [x] Add Local Law 44 funding classification (HPD vs Private financing)
- [x] Create separate timeline charts for HPD and privately financed projects

### Current Task: Add Certificate of Occupancy (CO) Data
- [x] Research CO data sources and API structure
- [x] Create CO data query script
- [x] Integrate CO events into timeline data structure
- [x] Update timeline chart to include CO events
- [x] Test CO data integration with existing workflow
- [x] Update documentation and README

### Completed Tasks
- [x] Create Jupyter notebook version of workflow with dataframe views
- [x] Refactor notebook to use in-memory DataFrames instead of file I/O between cells

### Repository Organization
- [x] Clean up repository structure
- [x] Move testing/debugging files to `testing_debugging/` folder
- [x] Move archived/obsolete files to `archive/` folder
- [x] Remove data_quality.py dependencies from active workflow files
- [x] Consolidate all imports to top of notebook instead of scattered throughout cells

### Documentation
- [x] Revise README to focus on notebook workflow (commit 5e1d7ac)

### DOB Query Enhancements
- [x] Implement address-based fallback (Tier 3) for buildings without DOB data after BIN/BBL queries
- [x] Add Step 3C to notebook for address-based fallback to handle lot splits, BIN/BBL mismatches

### ZAP Data Integration
- [x] Create notebook to integrate ZAP (Zoning Application Portal) BBL and Project data with HPD dataset
- [x] Join HPD data with ZAP BBL data to get Zoning Project IDs
- [x] Join with ZAP Project data to get full project details
- [x] Handle multiple matches by prioritizing ULURP projects

### Current Task: Add DOB Application Match Counts
- [x] Add count columns for NB (BISWEB) 01 applications matched via BIN, BBL, or Address
- [x] Add count columns for New Building (DOBNOW) I1 applications matched via BIN, BBL, or Address
- [x] Include these counts in exported CSV data

### Data Quality Scoring
- [x] Implement data quality scoring system with five criteria:
  - CO date vs Building Completion Date alignment (0-10 points)
  - Number of NB/New Building filings (0-10 points)
  - BIN match with DOB NB application (0-10 points)
  - CO date vs Project Start Date timeline (0-10 points)
  - Fully Permitted date vs Project Start Date timing (0-10 points)
- [x] Create data quality report CSV with scores for each building (0-50 total points)
- [x] Add score visualization and statistics with Poor/Fair/Good/Excellent categories

### Future Enhancements
- [ ] Add permit issuance dates
- [ ] Include building inspection data
- [ ] Add geospatial analysis
- [ ] Create interactive dashboard
