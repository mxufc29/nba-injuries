from datetime import datetime, timedelta
import re
from os import path, PathLike
import pandas as pd
import tabula
import PyPDF2
import logging
from io import BytesIO
import asyncio
from aiohttp import ClientSession
import NBALinkGen


# Directories
# TODO - modify directory for local downloads
DOWNLOAD_DATADIR = 'C:/Users/Michael Xu/Desktop/Sports Analytics/Projects/Data/Downloads/NBAOfficialInjReports'
requestheaders = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/120.0.0.0 Safari/537.36'}

# Logging Setup
LOGGING_DATADIR = DOWNLOAD_DATADIR
logging.basicConfig(filename=path.join(LOGGING_DATADIR, 'injreperrors.log'), filemode='w+',
                    format='%(levelname)s - %(message)s [%(asctime)s]')


async def lmtd_download_file(rep_timestamp: datetime, session: ClientSession, datadir: str | PathLike, dl_ceiling: int,
                             sleep: float, **kwargs):
    async with asyncio.Semaphore(dl_ceiling):
        await download_file(rep_timestamp, session, datadir, **kwargs)
        await asyncio.sleep(sleep)


async def download_file(rep_timestamp: datetime, session: ClientSession, datadir: str | PathLike, **kwargs):
    rep_URL = NBALinkGen.gen_injrepnbacom(rep_timestamp)
    filename = rep_URL.split('/')[-1]
    try:
        async with session.get(rep_URL, params={'downloadformat': 'pdf'}, **kwargs) as resp:
            resp.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
            content = await resp.read()

            def __write_file(filepath, contentx):
                with open(filepath, mode='wb') as file:
                    file.write(contentx)
            await asyncio.to_thread(__write_file, path.join(datadir, filename), content)
        print(f'Download of {filename} successful.')
    except Exception as e:
        logging.warning('Download of %s failed b/c %s', filename, e)
        print(f'Download of {filename} failed b/c {e}')


async def download_injreps(ts_start: datetime, ts_end: datetime, datadir: str | PathLike, dl_ceiling: int, sleep: float, **kwargs):
    list_reptimesdl = []
    hourscount = 0
    while (ts_start + timedelta(hours=hourscount)) <= ts_end:
        list_reptimesdl.append(ts_start + timedelta(hours=hourscount))
        hourscount += 1
    async with ClientSession() as session:
        downloadlist = [lmtd_download_file(ts_x, session, datadir, dl_ceiling, sleep, **kwargs) for ts_x in list_reptimesdl]
        await asyncio.gather(*downloadlist, return_exceptions=True)


async def validate_injrepurl(filepath: str | PathLike, session: ClientSession, **kwargs) -> bool:
    try:
        async with session.get(filepath, **kwargs) as resp:
            resp.raise_for_status()
            print(f'Validated {filepath}.')
            return True
    except Exception as e_gen:
        print(f'Validation of {filepath} failed due to {e_gen}')
        logging.warning(f'File at url {filepath} cannot be retrieved due to {e_gen}')
        return False


async def validate_localfile(filepath: str | PathLike) -> bool:
    is_valid = await asyncio.to_thread(path.isfile, filepath)
    if is_valid:
        print(f'Validated {filepath}.')
    else:
        logging.warning(f'File at {filepath} cannot be retrieved/does not exist.')
        print(f'Validation failed of {filepath}.')
    return is_valid


async def extract_injrepdata(filepath: str | PathLike, filestorage: str, area_headpg: list, cols_headpg: list,
                             area_otherpgs: list | None = None, cols_otherpgs: list | None = None, session: ClientSession | None = None,
                             **kwargs) -> pd.DataFrame:
    """
    :param filepath: filepath or URL of the report
    :param filestorage: specify 'local' (local file) or 'url'
    :param area_headpg: tabula area parameters of header page
    :param cols_headpg: tabula column parameters of header page
    :param area_otherpgs: tabula area parameters for non-header pages, if necessary
    :param cols_otherpgs: tabula column parameters for non-header pages, if necessary
    :param session: required if filestorage parameter is 'url' (extracting from URL datasource), otherwise optional
    :param kwargs: requestheaders, etc
    :return:
    """
    if (filestorage == 'local'):
        try:
            pdf_numpgs = await asyncio.to_thread(_pagect_localpdf, filepath)
        except Exception as e_gen:
            print(f'Could not open {str(filepath)} due to {e_gen}')
    elif (filestorage == 'url'):
        if session is None:
            raise ValueError("A ClientSession object must be provided when filestorage is 'url'.")
        if await validate_injrepurl(filepath, session):
            async with session.get(filepath, **kwargs) as resp:
                pdf_content = await resp.read()
                pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
                pdf_numpgs = len(pdf_reader.pages)
        else:
            print(f'File at url {filepath} cannot be retrieved.')
    else:
        raise ValueError("Invalid input for filestorage - use 'local' or 'url'.")

    if area_otherpgs is None:
        area_otherpgs = area_headpg
    if cols_otherpgs is None:
        cols_otherpgs = cols_headpg

    # First page
    dfs_headpg = await asyncio.to_thread(tabula.read_pdf, filepath, stream=True, area=area_headpg,
                                         columns=cols_headpg, pages=1)
    # Following pages
    dfs_otherpgs = []  # default to empty list if only single pg
    if pdf_numpgs >= 2:
        dfs_otherpgs = await asyncio.to_thread(tabula.read_pdf, filepath, stream=True, area=area_otherpgs,
                                               columns=cols_otherpgs, pages='2-' + str(pdf_numpgs), pandas_options={'header': None})
        # TODO address this
        # default to pandas_options={'header': 'infer'}
        # deprecated - pandas_options = kwargs.get('pandas_options')/pandas_options={'header': None}
    # Process and clean data
    df_rawdata = _concat_injreppgs(dflist_headpg=dfs_headpg, dflist_otherpgs=dfs_otherpgs)
    df_cleandata = _clean_injrep(df_rawdata)
    return df_cleandata


def _concat_injreppgs(dflist_headpg: list, dflist_otherpgs: list) -> pd.DataFrame:
    list_dfparts = [dflist_headpg[0]]
    for appenddf_x in dflist_otherpgs:
        if appenddf_x.loc[appenddf_x.index[0]].tolist() == list(dflist_headpg[0].columns):
            appenddf_x.drop(index=appenddf_x.index[0], inplace=True)  # check if header included in dataset
        appenddf_x.columns = dflist_headpg[0].columns
        list_dfparts.append(appenddf_x)
    for df_x in list_dfparts:
        df_x['LastonPgBoundary'] = False
    for df_x in list_dfparts[:-1]:
        # 'LastonPgBoundary' - flag for last entry on page break site (last page doesn't count)
        df_x.at[(df_x.shape[0] - 1), 'LastonPgBoundary'] = True
    df_injrepconcat = pd.concat(list_dfparts, ignore_index=True)
    return df_injrepconcat


def _clean_injrep(dfinjrep_x: pd.DataFrame) -> pd.DataFrame:
    dfcleaning_x = dfinjrep_x.copy()

    # Forward fill nested columns
    ffill_cols = ['Game Date', 'Game Time', 'Matchup', 'Team']  # CONSTANT - modify as needed
    for colname, seriesx in dfcleaning_x.items():
        if (colname in ffill_cols):
            seriesx.ffill(inplace=True)

    # Remove 'Not Yet Submitted' Condition Temporarily
    dfcleaning_x['unsubmitted'] = dfcleaning_x['Reason'].apply(lambda x: str(x).casefold()) == 'NOT YET SUBMITTED'.casefold()
    df_unsubmitted = dfcleaning_x.loc[dfcleaning_x['unsubmitted'], :]
    dfcleaning_x = dfcleaning_x.loc[~(dfcleaning_x['unsubmitted']), :]

    # Previous and next values of relevant columns
    dfcleaning_x['NextReas'] = dfcleaning_x['Reason'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['NextPlname'] = dfcleaning_x['Player Name'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['NextCstatus'] = dfcleaning_x['Current Status'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['Nextx2Reas'] = dfcleaning_x['Reason'].shift(periods=-2, fill_value='N/A')

    dfcleaning_x['PrevReas'] = dfcleaning_x['Reason'].shift(periods=1, fill_value='N/A')
    dfcleaning_x['PrevPlname'] = dfcleaning_x['Player Name'].shift(periods=1, fill_value='N/A')
    dfcleaning_x['PrevCstatus'] = dfcleaning_x['Current Status'].shift(periods=1, fill_value='N/A')
    dfcleaning_x['Prevx2Reas'] = dfcleaning_x['Reason'].shift(periods=2, fill_value='N/A')
    dfcleaning_x['PrevLastonPgBdry'] = dfcleaning_x['LastonPgBoundary'].shift(periods=1, fill_value='N/A')

    # Create Flags
    ## (a) Reason - G League
    dfcleaning_x['GLeague'] = dfcleaning_x['Reason'].str.contains('G League', case=False).astype(pd.BooleanDtype())
    ## (b) Reason - Likely complete, text contains both ';' and ',', except for page boundary
    dfcleaning_x['likely_reas1linecomplete'] = (
            (dfcleaning_x['Reason'].str.contains('-', case=False)) &
            (dfcleaning_x['Reason'].str.contains(';', case=False)) &
            (dfcleaning_x['Player Name'].notna()) &
            (dfcleaning_x['Current Status'].notna()) &
            ~(dfcleaning_x['LastonPgBoundary'])
    )
    ## (c) Reason - Likely complete (alternate flag for edge case), text is complete but doesn't contain either ';' or ',',
    ## but bounded by two other entries (above and below) with split Reason text
    dfcleaning_x['likely_reas1linecomplete_alt'] = (
            (dfcleaning_x['Reason'].notna()) &
            (dfcleaning_x['Player Name'].notna()) &
            (dfcleaning_x['Current Status'].notna()) &
            (dfcleaning_x['Nextx2Reas'].isna()) &
            (dfcleaning_x['Prevx2Reas'].isna()) &
            ~(dfcleaning_x['LastonPgBoundary'])
    )
    ## (d) Reason - Likely complete (second alternate flag), manually handle other edge cases that likely are one line)
    list_uniquecases = ['League Suspension', 'Not with Team', 'Personal Reasons', 'Rest', 'Concussion Protocol']
    uniquecase_regex = r'\b(?:' + '|'.join([case.replace(' ', '') for case in list_uniquecases]) + r')\b'
    dfcleaning_x['likely_reas1linecomplete_alt2'] = (
            (dfcleaning_x['Reason'].notna()) &
            (dfcleaning_x['Player Name'].notna()) &
            (dfcleaning_x['Current Status'].notna()) &
            (dfcleaning_x['Reason'].str.replace(r'\s+', '', regex=True).str.contains(uniquecase_regex, case=False,
                                                                                     na=False, regex=True)) &
            ~(dfcleaning_x['LastonPgBoundary'])
    )
    ## (e) Reason - split on multiple lines
    dfcleaning_x['reas_multilinesplit'] = (
            (dfcleaning_x['NextPlname'].isna()) &
            (dfcleaning_x['NextCstatus'].isna()) &
            (dfcleaning_x['PrevPlname'].isna()) &
            (dfcleaning_x['PrevCstatus'].isna()) &
            (~(dfcleaning_x['LastonPgBoundary'])) &
            (~(dfcleaning_x['likely_reas1linecomplete'])) &
            (~(dfcleaning_x['likely_reas1linecomplete_alt'])) &
            (~(dfcleaning_x['likely_reas1linecomplete_alt2']))
    )
    # Overrides
    ## 'G League - Two-Way' - assume always to be single line text on Reason, override
    dfcleaning_x.loc[dfcleaning_x['GLeague'], 'reas_multilinesplit'] = False

    # Handle multiline text in 'Reason' split onto preceding and succeeding line
    ## (a) Reason has existing value but previous and next line ['Player Name, 'Current Status'] are empty - "three line split"
    dfcleaning_x.loc[((dfcleaning_x['reas_multilinesplit']) & (dfcleaning_x['Reason'].notna())), 'Reason'] = (
            dfcleaning_x['PrevReas'] + ' ' + dfcleaning_x['Reason'] + ' ' + dfcleaning_x['NextReas'])
    ## (b) Reason has a nan value - "two line split"
    dfcleaning_x.fillna(value={'Reason': dfcleaning_x['Reason'].ffill() + ' ' + dfcleaning_x['Reason'].bfill()},
                        inplace=True)
    ## (c) Identify and delete rows that have been addressed
    dfcleaning_x['next_multiline'] = dfcleaning_x['reas_multilinesplit'].shift(periods=-1, fill_value=False).astype(bool)
    dfcleaning_x['prev_multiline'] = dfcleaning_x['reas_multilinesplit'].shift(periods=1, fill_value=False).astype(bool)
    dfcleaning_x['del_multiline'] = (
            (dfcleaning_x['next_multiline']) |
            (dfcleaning_x['prev_multiline'])
    )
    dfcleaning_x = dfcleaning_x.loc[~(dfcleaning_x['del_multiline']), :]

    # Page Break Split - Handle a 'Reason' description which is split by a page break
    ## (a) Refresh Relevant Previous/Next Values
    dfcleaning_x['NextReas'] = dfcleaning_x['Reason'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['NextPlname'] = dfcleaning_x['Player Name'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['NextCstatus'] = dfcleaning_x['Current Status'].shift(periods=-1, fill_value='N/A')
    dfcleaning_x['PrevReas'] = dfcleaning_x['Reason'].shift(periods=1, fill_value='N/A')
    dfcleaning_x['PrevPlname'] = dfcleaning_x['Player Name'].shift(periods=1, fill_value='N/A')
    dfcleaning_x['PrevCstatus'] = dfcleaning_x['Current Status'].shift(periods=1, fill_value='N/A')

    ## (b) Create Flag for Reason - likely split by a page break
    dfcleaning_x['reas_pgbksplit'] = (
            (dfcleaning_x['LastonPgBoundary']) &
            (dfcleaning_x['Reason'].notna()) &
            (dfcleaning_x['Player Name'].notna()) &
            (dfcleaning_x['Current Status'].notna()) &
            (dfcleaning_x['NextPlname'].isna()) &
            (dfcleaning_x['NextCstatus'].isna()) &
            (dfcleaning_x['NextReas'].notna())
    )
    dfcleaning_x.loc[dfcleaning_x['reas_pgbksplit'], 'Reason'] = (
            dfcleaning_x['Reason'] + ' ' + dfcleaning_x['NextReas'])

    ## (c) Identify and delete rows that have been addressed
    dfcleaning_x['prev_pgbksplit'] = dfcleaning_x['reas_pgbksplit'].shift(periods=1, fill_value=False).astype(bool)
    dfcleaning_x = dfcleaning_x.loc[~(dfcleaning_x['prev_pgbksplit']), :]

    # Drop variables used for cleaning (keep first seven cols), add back unsubmitted cols, reindex
    dfcleaning_xfinal = pd.concat([dfcleaning_x[dfcleaning_x.columns[:7]], df_unsubmitted[df_unsubmitted.columns[:7]]])
    dfcleaning_xfinal.sort_index(inplace=True)
    dfcleaning_xfinal.reset_index(inplace=True, drop=True)
    return dfcleaning_xfinal


def _pagect_localpdf(filepath: PathLike):
    with open(filepath, mode='rb') as injrepfile:
        pdf_reader = PyPDF2.PdfReader(injrepfile)
        pdf_numpgs = len(pdf_reader.pages)
        return pdf_numpgs


def round_prev30min(timestamp: datetime) -> datetime:
    if timestamp.minute >= 45:
        redr = timestamp.minute % 30
        tsdelta = timestamp - timedelta(minutes=redr)
        return tsdelta.replace(second=0, microsecond=0)
    elif timestamp.minute <= 30:
        tsdelta = timestamp - timedelta(hours=1) + timedelta(minutes=(30 - timestamp.minute))
        return tsdelta.replace(second=0, microsecond=0)
    else:
        tsdelta = timestamp - timedelta(hours=1) - timedelta(minutes=(timestamp.minute - 30))
        return tsdelta.replace(second=0, microsecond=0)
