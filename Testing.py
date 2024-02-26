import random
import pandas as pd
from datetime import datetime, timedelta
from os import path
import InjRepPDFScrape
import NBALinkGen

# Directories
DOWNLOAD_DATADIR = 'C:/Users/Michael Xu/Desktop/Sports Analytics/Projects/Data/Downloads/NBAOfficialInjReports'
# Set the Dimensions for Area and Columns for Extraction, derived from the Tabula app
# format of area params is [top (y1), left(x1), bottom (y1+height), right (x1+width)]
area_params2023 = [76.30171985626221, 18.31240634193411, 534.1120389938354, 820.2697929691313]
columns_params2023 = [76.30171985626221, 183.545096237564, 255.11084727516163, 371.93141146888723, 543.4787264560698,
                      655.0371030735015]  # number of column params is (number of columns - 1)
area_params2024 = []
columns_params2024 = []


def round_prev30min(timestamp: datetime) -> datetime:
    if timestamp.minute >= 30:
        redr = timestamp.minute % 30
        tsdelta = timestamp - timedelta(minutes=redr)
        return tsdelta.replace(second=0, microsecond=0)
    else:
        tsdelta = timestamp - timedelta(hours=1) + timedelta(minutes=(30 - timestamp.minute))
        return tsdelta.replace(second=0, microsecond=0)


seasonstart_2023 = datetime(year=2023, month=10, day=24, hour=17, minute=30)
start_dltest = datetime(year=2024, month=2, day=1, hour=00, minute=30)
end_dltest = round_prev30min(datetime.now())

# InjRepPDFScrape.download_injreps(ts_start=start_dltest, ts_end=end_dltest)

start_urltest = seasonstart_2023
end_urltest = datetime(year=2023, month=12, day=31, hour=23, minute=30)
list_testrepdts = []
hourscount = 0
while (start_urltest + timedelta(hours=hourscount)) <= end_urltest:
    list_testrepdts.append(start_urltest + timedelta(hours=hourscount))
    hourscount += 1

dict_dfinjtest = {}
for reptime_x in random.sample(list_testrepdts, k=10):
    injrepurl_x = NBALinkGen.gen_injrepnbacom(timestamp=reptime_x)
    df_x = InjRepPDFScrape.extract_injrepdata(filepath=injrepurl_x, filestorage='url', area_headpg=area_params2023,
                                              cols_headpg=columns_params2023)
    df_x['ReportTime'] = reptime_x.strftime('%Y-%m-%d/%H:%M')
    dict_dfinjtest[injrepurl_x] = df_x

# def extract(listrepdts: list)
