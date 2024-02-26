import numpy as np
from NBALinkGen import gen_injrepnbacom
from datetime import datetime, timedelta
from os import path, PathLike
import pandas as pd
import requests
import tabula
import PyPDF2
import logging
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

# Directories
DOWNLOAD_DATADIR = 'C:/Users/Michael Xu/Desktop/Sports Analytics/Projects/Data/Downloads/NBAOfficialInjReports'
requestheaders = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/120.0.0.0 Safari/537.36'}

# Logging Setup
logging.basicConfig(filename=path.join(DOWNLOAD_DATADIR, 'dl_errors.log'), filemode='w+',
                    format='%(levelname)s - %(message)s [%(asctime)s]')


def download_file(rep_timestamp: datetime):
    rep_URL = gen_injrepnbacom(rep_timestamp)
    filename = rep_URL.split('/')[-1]
    try:
        resp = requests.get(rep_URL, params={'downloadformat': 'pdf'}, headers=requestheaders)
        resp.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
        with open(path.join(DOWNLOAD_DATADIR, filename), mode='wb') as file:
            file.write(resp.content)
    except Exception as e:
        logging.warning('Download of %s failed b/c %s', filename, e)
        print(f'Download of {filename} failed b/c {e}')


def download_injreps(ts_start: datetime, ts_end: datetime):
    list_reptimesdl = []
    hourscount = 0
    while (ts_start + timedelta(hours=hourscount)) <= ts_end:
        list_reptimesdl.append(ts_start + timedelta(hours=hourscount))
        hourscount += 1
    with ThreadPoolExecutor() as tpexec:
        tpexec.map(download_file, list_reptimesdl)


def extract_injrepdata(filepath: str | PathLike, filestorage: str, area_headpg: list, cols_headpg: list,
                       area_otherpgs: list = None, cols_otherpgs: list = None) -> pd.DataFrame():
    """
    :param filestorage: specify 'local' file or 'url'
    :param filepath: filepath or URL of the report
    :return:
    """
    if (filestorage == 'local'):
        try:
            with open(filepath, mode='rb') as injrepfile:
                pdf_reader = PyPDF2.PdfReader(injrepfile)
                pdf_numpgs = len(pdf_reader.pages)
        except Exception as e_gen:
            print(f'Could not open {str(filepath)} due to {e_gen}')
    elif (filestorage == 'url'):
        try:
            resp = requests.get(filepath, headers=requestheaders)
            resp.raise_for_status()
            pdf_reader = PyPDF2.PdfReader(BytesIO(resp.content))
            pdf_numpgs = len(pdf_reader.pages)
        except Exception as e_gen:
            print(f'Could not retrieve {str(filepath)} due to {e_gen}')
    else:
        raise ValueError("Invalid input for filestorage - use 'local'' or 'url'.")

    if area_otherpgs is None:
        area_otherpgs = area_headpg
    if cols_otherpgs is None:
        cols_otherpgs = cols_headpg
    dfs_headpg = tabula.read_pdf(filepath, stream=True, area=area_headpg, columns=cols_headpg, pages=1)
    dfs_otherpgs = []  # default to empty list if only single pg
    if pdf_numpgs >= 2:
        dfs_otherpgs = tabula.read_pdf(filepath, stream=True, area=area_otherpgs, columns=cols_otherpgs,
                                       pages='2-' + str(pdf_numpgs),
                                       pandas_options={'header': None})
    df_rawdata = _concat_injreppgs(dflist_headpg=dfs_headpg, dflist_otherpgs=dfs_otherpgs)
    df_cleandata = _clean_injrep(df_rawdata)
    return df_cleandata


def _concat_injreppgs(dflist_headpg: list, dflist_otherpgs: list) -> pd.DataFrame():
    list_dfparts = [dflist_headpg[0]]
    for appenddf_x in dflist_otherpgs:
        appenddf_x.columns = dflist_headpg[0].columns
        list_dfparts.append(appenddf_x)
    for df_x in list_dfparts:
        df_x['LastonPg'] = False
        df_x.at[(df_x.shape[0] - 1), 'LastonPg'] = True
    df_injrepconcat = pd.concat(list_dfparts, ignore_index=True)
    df_injrepconcat['LastonReport'] = False
    df_injrepconcat.at[(df_injrepconcat.shape[0] - 1), 'LastonReport'] = True
    return df_injrepconcat


def _clean_injrep(dfinjrep_x: pd.DataFrame) -> pd.DataFrame():
    dfcleaning_x = dfinjrep_x.copy()

    # Forward fill nested columns
    ffill_cols = ['Game Date', 'Game Time', 'Matchup', 'Team']  # CONSTANT - modify as needed
    for colname, seriesx in dfcleaning_x.items():
        if (colname in ffill_cols):
            seriesx.ffill(inplace=True)

    # Remove 'Not Yet Submitted' Condition
    dfcleaning_x['unsubmitted'] = dfcleaning_x['Reason'].apply(lambda x: str(x).casefold()) == 'NOT YET SUBMITTED'.casefold()
    df_unsubmitted = dfcleaning_x.loc[dfcleaning_x['unsubmitted'], :]
    dfcleaning_x = dfcleaning_x.loc[~(dfcleaning_x['unsubmitted']), :]

    # Previous and next values of relevant columns
    dfcleaning_x['NextReas'] = dfcleaning_x['Reason'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['NextPlname'] = dfcleaning_x['Player Name'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['NextCstatus'] = dfcleaning_x['Current Status'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['PrevReas'] = dfcleaning_x['Reason'].shift(periods=1, fill_value='N/A')
    dfcleaning_x['PrevPlname'] = dfcleaning_x['Player Name'].shift(periods=1, fill_value='N/A')
    dfcleaning_x['PrevCstatus'] = dfcleaning_x['Current Status'].shift(periods=1, fill_value='N/A')

    # Flags
    dfcleaning_x['GLeague'] = dfcleaning_x['Reason'].str.contains('G League', case=False).astype(pd.BooleanDtype())
    dfcleaning_x['likely_inj1line_nosplit'] = (
            (dfcleaning_x['Reason'].str.contains('-', case=False)) &
            (dfcleaning_x['Reason'].str.contains(';', case=False)) &
            (dfcleaning_x['Player Name'].notna()) &
            (dfcleaning_x['Current Status'].notna())
    )
    dfcleaning_x['reas_multilinesplit'] = (
            (dfcleaning_x['NextPlname'].isna()) &
            (dfcleaning_x['NextCstatus'].isna()) &
            (dfcleaning_x['PrevPlname'].isna()) &
            (dfcleaning_x['PrevCstatus'].isna()) &
            ~(dfcleaning_x['likely_inj1line_nosplit'])
    )
    dfcleaning_x.loc[dfcleaning_x['GLeague'], 'reas_multilinesplit'] = False

    # (a) Handle multiline text in 'Reason' split onto preceding and succeeding line
    ## Reason has existing value but previous and next line are "empty" - three line split
    dfcleaning_x.loc[((dfcleaning_x['reas_multilinesplit']) & (dfcleaning_x['Reason'].notna())), 'Reason'] = (
            dfcleaning_x['PrevReas'] + ' ' + dfcleaning_x['Reason'] + ' ' + dfcleaning_x['NextReas'])
    ## Reason has nan values - two line split
    dfcleaning_x.fillna(value={'Reason': dfcleaning_x['Reason'].ffill() + ' ' + dfcleaning_x['Reason'].bfill()},
                        inplace=True)
    ## Identify and delete rows that have been addressed
    dfcleaning_x['next_multiline'] = dfcleaning_x['reas_multilinesplit'].shift(periods=-1, fill_value=False).astype(bool)
    dfcleaning_x['prev_multiline'] = dfcleaning_x['reas_multilinesplit'].shift(periods=1, fill_value=False).astype(bool)
    dfcleaning_x['del_multiline'] = (
            (dfcleaning_x['next_multiline']) |
            (dfcleaning_x['prev_multiline'])
    )
    dfcleaning_x = dfcleaning_x.loc[~(dfcleaning_x['del_multiline']), :]

    # (b) Handle a 'Reason' description which is split by a page break
    ## Refresh Previous/Next Values
    dfcleaning_x['NextReas'] = dfcleaning_x['Reason'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['NextPlname'] = dfcleaning_x['Player Name'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['NextCstatus'] = dfcleaning_x['Current Status'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['PrevReas'] = dfcleaning_x['Reason'].shift(periods=1, fill_value='N/A')
    dfcleaning_x['PrevPlname'] = dfcleaning_x['Player Name'].shift(periods=1, fill_value='N/A')
    dfcleaning_x['PrevCstatus'] = dfcleaning_x['Current Status'].shift(periods=1, fill_value='N/A')

    dfcleaning_x['reas_pgbksplit'] = (
            (dfcleaning_x['LastonPg']) &
            ~(dfcleaning_x['LastonReport']) &
            (dfcleaning_x['NextPlname'].isna()) &
            (dfcleaning_x['NextCstatus'].isna())
    )
    dfcleaning_x.loc[dfcleaning_x['reas_pgbksplit'], 'Reason'] = (
            dfcleaning_x['Reason'] + ' ' + dfcleaning_x['NextReas'])
    ## Identify and delete rows that have been addressed
    dfcleaning_x['prev_pgbksplit'] = dfcleaning_x['reas_pgbksplit'].shift(periods=1, fill_value=False).astype(bool)
    dfcleaning_x = dfcleaning_x.loc[~(dfcleaning_x['prev_pgbksplit']), :]

    # Drop variables used for cleaning (keep first seven cols), add back unsubmitted cols
    dfcleaning_xfinal = pd.concat([dfcleaning_x[dfcleaning_x.columns[:7]], df_unsubmitted[df_unsubmitted.columns[:7]]])

    dfcleaning_xfinal.sort_index(inplace=True)
    dfcleaning_xfinal.reset_index(inplace=True, drop=True)
    return dfcleaning_xfinal

