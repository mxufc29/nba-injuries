# NBA Injury Data Extraction Pipeline - QC Instructions

## Setup Tasks

1. **Download Required Files**  
   Download everything in the “Required Files” folder – three `.py` files and one `.csv` file.

2. **Open Files**  
   Open the three `.py` files in your IDE.

3. **Install Java for Tabula**  
   The extraction uses Python’s `tabula` library, which requires a preexisting Java environment. Instructions for installing Java can be found [here](https://tabula-py.readthedocs.io/en/latest/getting_started.html#requirements).

4. **Modify Filepaths**  
   Update the following filepaths at the top of `ETLTesting_QC.py` to convenient locations on your local disk:
   - `DOWNLOAD_DATADIR` – location for PDF file downloads.
   - `EXPORT_DATADIR` – location for data exports.
   - `DATA_DIR` – ensure the `nbainjrep_keydts.csv` file is saved here.

## Testing Instructions
Run the following tests blocks separately in “main” of ETLTesting_QC.py

### Test 1 - Bulk Download Raw Data from URLs
- Verify that downloads complete successfully and files are correctly placed into the `DOWNLOAD_DATADIR` folder specified.

### Test 2 – QC Local ETL
1. Expect a random sample of 10 CSV datasets in `EXPORT_DATADIR`, sourced from the downloaded reports.
2. **QC Procedure**: For each CSV dataset:
   - Open the CSV dataset in `EXPORT_DATADIR` [extracted data].
   - Navigate to the URL in the "Report Link" column [original source].
   - Validate that the CSV dataset matches the content at the URL source, paying attention to:
     - Player name
     - Current Status
     - Reason (especially if it extends across multiple lines).
3. If time allows, run the code multiple times and perform additional QC rounds.
4. Log any inconsistencies or errors in the `ERRORLOG` file on GitHub.

### Test 3 - QC URL ETL
- This test mirrors Test 2, but accesses and extracts data directly from the URL in real-time, rather than from local storage.
- **Note**: This test will take longer due to extended sleep delays required to avoid server rejections.

### Test 4 - Total ETL from URL
1. Perform a complete extraction and aggregation of reports from the playoffs during the 21-22 season.
2. Confirm that a dataset (`nba_allinjreps_playoffs2122test.csv`) is exported to `EXPORT_DATADIR`.
3. Conduct a brief spot check of this file (no detailed QC required).

