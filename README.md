# nba-injuries

Initial Testing

(a) Run NBALinkGen.py, InjRepPDFScrape.py, Testing.py in console 

(b) In Testing.py, dict_dfinjtest will contain a dict of dataframes from 10 randomly selected dates in 2023, with the index being the url of the injury report from which the dataframe data is extracted. Do a quality check and validation of the dataframe's data with the raw data in the url pdf. With emphasis on the following:

- When the 'Reason' column (last column) is split into multiple lines (line break occurs) in the url pdf, make sure the data is complete in the dataframe
- When 'NOT YET SUBMITTED' is present in the url report, make sure the entry is present in the dataframe
- Make sure the first and last records of each page in the url pdf are preserved in the dataframe
- Count the number of entries (player records, e.g. "one row" in the url pdf) and make sure it is equal to the total number of records in the dataframe

Write your results/any errors/inconsistencies you notice below. Thanks so much!
