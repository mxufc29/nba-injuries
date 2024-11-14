# NBA Injury Data Extraction Pipeline - QC Instructions

Help me test my NBA injury data extraction pipeline!

## Setup Tasks

Download everything in the “testingfiles” folder – three `.py` files, one `.csv` file.

- **NBALinkGen.py**
- **InjRepPDFScrape_async.py**
- **ETLTesting_QC.py**
- **nbainjrep_keydts.csv**

Open the three `.py` files in your IDE.

The extraction uses Python’s `tabula` library, which requires a preexisting Java environment. Instructions here to install the Java runtime if you don’t have it already:

[https://tabula-py.readthedocs.io/en/latest/getting_started.html#requirements](https://tabula-py.readthedocs.io/en/latest/getting_started.html#requirements)

Modify the following filepaths at the top of `ETLTesting_QC.py` to a convenient location of your choosing on the local disk:

- `DOWNLOAD_DATADIR` – location for PDF file downloads
- `EXPORT_DATADIR` – location for data exports
- `DATA_DIR` – make sure the `nbainjrep_keydts.csv` file is saved to this same location

## Run the Following Test Blocks Separately in “main” of ETLTesting_QC.py

### Test 1 - (Bulk Download Raw Data from URLs)

Verify downloads successfully completed, and that the files are correctly placed into the `DOWNLOAD_DATADIR` folder specified.

### Test 2 – (QC Local ETL)

There should be a random sample of 10 CSV datasets exported to the `EXPORT_DATADIR` folder, sourced from the group of downloaded reports above. These datasets are fully extracted and cleaned.

Conduct a QC – repeat for each CSV dataset:

1. Open the CSV dataset in `EXPORT_DATADIR` [extracted data].
2. Navigate to the URL specified in “Report Link” (last column of the dataset) [original source].
3. Validate that the CSV dataset is identical in content to the URL source, paying attention to the following fields:
   - **Player name**
   - **Current Status**
   - **Reason** (particularly text that extends across multiple lines)

If you have time, run the code again a few more times and do a few more rounds of QC.

Submit any inconsistencies or errors here - [https://github.com/mxufc29/nba-injuries/issues](https://github.com/mxufc29/nba-injuries/issues). Include the URL data source, the Player name/Reason so that the record can be located, and describe the inconsistency between the original and cleaned data.

### Test 3 - (QC URL ETL)

This test is exactly the same as Test 2, except the reports are accessed and data extracted directly from the URL in real time, instead of on the local drive. This will take considerably longer as the sleep delay must be extended to prevent the server from rejecting the HTML requests.

There should be a random sample of 10 CSV datasets exported to the `EXPORT_DATADIR`. These datasets are fully extracted and cleaned.

Conduct a QC – repeat for each CSV dataset:

1. Open the CSV dataset in `EXPORT_DATADIR` [extracted data].
2. Navigate to the URL specified in “Report Link” (last column of the dataset) [original source].
3. Validate that the CSV dataset is identical in content to the URL source, paying attention to the following fields:
   - **Player name**
   - **Current Status**
   - **Reason** (particularly text that extends across multiple lines)

If you have time, run the code again a few more times and do a few more rounds of QC.

Submit any inconsistencies or errors here - [https://github.com/mxufc29/nba-injuries/issues](https://github.com/mxufc29/nba-injuries/issues). Include the URL data source, the Player name/Reason so that the record can be located, and describe the inconsistency between the original and cleaned data.

### Test 4 - (Total ETL from URL)

This should perform a total extraction and aggregation for an entire set of reports from the playoffs during the 21-22 season. The reports are accessed and data extracted directly from the URL in real time.

There should be a single aggregated dataset (`nba_allinjreps_playoffs2122test.csv`) exported to the `EXPORT_DATADIR` folder. Do a quick spot-check of the overall data structure/alignment (no detailed QC needed).
