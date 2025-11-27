# Housing Data Analysis

This repository contains scripts and data for analyzing affordable housing production in New York City, with a focus on identifying new construction projects and correlating HPD (Housing Preservation and Development) financing data with DOB (Department of Buildings) permit filings.

## Files

### Data Files
- `data/raw/Affordable_Housing_Production_by_Building.csv` - HPD data on affordable housing buildings
- `Affordable_Housing_Production_by_Building_with_financing.csv` - HPD data with financing type classification (HPD vs Private)
- `new_construction_bins_dob_filings.csv` - DOB job application filings for new construction (NB/New Building type)
- `all_construction_bins_co_filings.csv` - Certificate of Occupancy data from DOB APIs

### Scripts

#### `query_dob_filings.py`
Queries NYC Open Data APIs to find DOB job filings for new construction projects.
- Searches DOB Job Application Filings API for job type "NB"
- Searches DOB NOW Job Applications API for job type "New Building"
- Matches filings to BINs from the housing data
- **BBL Fallback**: When BINs are missing, decomposes BBL into borough/block/lot for alternative search
- **Multiple BIN Handling**: Recognizes that one BBL can contain multiple BINs

**Usage:**
```bash
# Search by BIN file
python query_dob_filings.py bins_file.txt

# Search by CSV with BIN/BBL columns (enables BBL fallback)
python query_dob_filings.py data_with_bbl.csv
```

#### `query_co_filings.py`
Queries NYC Open Data APIs to find Certificate of Occupancy data for buildings.
- Searches DOB NOW Certificate of Occupancy API
- Searches DOB Certificate Of Occupancy API
- Extracts first CO issuance date for each BIN

**Usage:**
```bash
python query_co_filings.py all_construction_bins.txt
```

#### `query_ll44_funding.py`
Queries Local Law 44 funding database to classify projects by financing type.
- Checks if project IDs exist in LL44 funding database
- Classifies projects as "HPD Financed" or "Privately Financed"
- Adds financing type column to HPD data

**Usage:**
```bash
python query_ll44_funding.py data/raw/Affordable_Housing_Production_by_Building.csv
```

#### `HPD_DOB_Join_On_BIN.py`
Creates a timeline combining HPD financing data with DOB filing/approval dates and Certificate of Occupancy data.
- Extracts HPD financing start and completion dates
- Extracts DOB/DOB NOW application submission and approval dates
- Extracts first Certificate of Occupancy issuance date
- Creates a unified timeline CSV sorted by BIN and date

**Usage:**
```bash
python HPD_DOB_Join_On_BIN.py data/raw/Affordable_Housing_Production_by_Building.csv new_construction_bins_dob_filings.csv [co_filings.csv]
```

**Output:**
- `Affordable_Housing_Production_by_Building_timeline.csv` - Timeline of all events by BIN

#### `create_timeline_chart.py`
Generates a Gantt-style timeline chart showing DOB application, HPD financing, and Certificate of Occupancy timelines for each BIN.

**Usage:**
```bash
python create_timeline_chart.py Affordable_Housing_Production_by_Building_timeline.csv
```

**Output:**
- `*_hpd_financed_timeline_timeline_chart.pdf` - Chart for HPD financed projects
- `*_privately_financed_timeline_timeline_chart.pdf` - Chart for privately financed projects
- `*_timeline_timeline_data.csv` - Extracted timeline data for each financing type

## Data Sources

- **HPD Data**: Affordable Housing Production data from NYC HPD
- **LL44 Funding**: [NYC Open Data - Local Law 44 Funding](https://data.cityofnewyork.us/resource/gmi7-62cd.json) - Used to classify projects as HPD or privately financed
- **DOB Job Applications**: [NYC Open Data - DOB Job Application Filings](https://data.cityofnewyork.us/Housing-Development/DOB-Job-Application-Filings/ic3t-wcy2)
- **DOB NOW Job Applications**: [NYC Open Data - DOB NOW Job Applications](https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Job-Application-Filings/w9ak-ipjd)
- **DOB NOW Certificate of Occupancy**: [NYC Open Data - DOB NOW Certificate of Occupancy](https://data.cityofnewyork.us/resource/pkdm-hqz6.json)
- **DOB Certificate Of Occupancy**: [NYC Open Data - DOB Certificate Of Occupancy](https://data.cityofnewyork.us/resource/bs8b-p36w.json)

## Requirements

```bash
pip install pandas matplotlib requests beautifulsoup4
```

## Workflow

1. Extract BINs for new construction projects from `data/raw/Affordable_Housing_Production_by_Building.csv`
2. Query LL44 funding database to classify projects by financing type using `query_ll44_funding.py`
3. Query DOB APIs for NB/New Building filings using `query_dob_filings.py` (includes BBL fallback for missing BINs)
4. Query CO APIs for Certificate of Occupancy data using `query_co_filings.py`
5. Join HPD, DOB, and CO data by BIN using `HPD_DOB_Join_On_BIN.py` (creates separate timelines for HPD and private projects)
6. Visualize timelines using `create_timeline_chart.py` (creates separate charts for HPD and private projects)

## Timeline Event Types

- **HPD financing submitted** - Project start date from HPD
- **HPD financing completed** - Project completion date from HPD
- **DOB NB Application submitted** - First DOB filing for new building
- **DOB NB Application approved** - First DOB approval for new building
- **DOB NOW New Building Application submitted** - First DOB NOW filing
- **DOB NOW New Building Application approved** - First DOB NOW approval
- **Certificate of Occupancy issued (Initial)** - Initial/Temporary CO issuance date
- **Certificate of Occupancy issued (Final)** - Final CO issuance date

