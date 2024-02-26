import pandas as pd
from datetime import datetime, timedelta
from os import path

# URL STEMS (Constants to change as needed)

URL_pl_NBAcomprof = 'https://www.nba.com/player/*/profile'  # default is 'https://www.nba.com/player/*/[player-slug]/profile'
                                                            # but no slug redirects correctly
URL_pl_Rotowireprof = 'https://www.rotowire.com/basketball/*'
URL_injrep_nbacom = 'https://ak-static.cms.nba.com/referee/injury/Injury-Report_*.pdf'


def genpl_NBAcomprof(nbaid: str) -> str:
    return URL_pl_NBAcomprof.replace('*', nbaid)

def genpl_Rotowireprof(rotoid: str, plslug: str) -> str:
    return URL_pl_Rotowireprof.replace('*', rotoid + '-' + plslug)

def gen_injrepnbacom(timestamp: datetime) -> str:
    """
    :param timestamp: date and time of injury report as displayed on file title (time ranges from 00:30 to 23:30 in
    one hr increments), and time as reflected on report is 30 minutes behind
    :return: link to PDF file of injury report
    """
    URLstem_date = timestamp.date().strftime('%Y-%m-%d')
    URLstem_time = (timestamp - timedelta(minutes=30)).time().strftime('%I%p')
    return URL_injrep_nbacom.replace('*', URLstem_date + '_' + URLstem_time)


def gen_injrep_dlpath(timestamp: datetime, directorypath: str) -> str:
    URLstem_date = timestamp.date().strftime('%Y-%m-%d')
    URLstem_time = (timestamp - timedelta(minutes=30)).time().strftime('%I%p')
    filename = 'Injury-Report_' + URLstem_date + '_' + URLstem_time + '.pdf'
    injrep_dlpath = path.join(directorypath, filename)
    return injrep_dlpath

