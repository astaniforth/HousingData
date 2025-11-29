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

### Future Enhancements
- [ ] Add permit issuance dates
- [ ] Include building inspection data
- [ ] Add geospatial analysis
- [ ] Create interactive dashboard
