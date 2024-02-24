# nba-injuries

Initial Testing

(a) Run NBALinkGen.py, InjRepPDFScrape.py, Testing.py in console 

(b) In Testing.py, dict_dfinjtest will contain a dict of 10 dataframes selected at random, with the index being the url of the report from which the dataframe data is extracted. Do a quality check and validation of the dataframe's data with the data in the url. With emphasis on 

- When 'Reason' field is split into multiple lines (line break occurs) in the url report
- When 'NOT YET SUBMITTED' is present in the url report, make sure the entry is present in the dataframe
- The first and last records of each page in the url report are preserved in the dataframe
- Count the number of 'Player records' and make sure it is equal to the number of records in the dataframe

Write your results/any errors/inconsistencies you notice below.
