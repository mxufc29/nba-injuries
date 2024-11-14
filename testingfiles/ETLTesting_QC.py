import random
import pandas as pd
from collections import defaultdict
from os import path, PathLike
from datetime import datetime, timedelta
import NBALinkGen
import InjRepPDFScrape_async
from aiohttp import ClientSession
import asyncio
import time

# Directories
# TODO - fill in your directory for local downloads
DOWNLOAD_DATADIR = 'C:/Users/Michael Xu/Desktop/Sports Analytics/Projects/Data/Downloads/NBAOfficialInjReports'
# TODO - fill in your directory outgoing data exports from Python
EXPORT_DATADIR = 'C:/Users/Michael Xu/Desktop/Sports Analytics/Projects/Data/Exports'
# TODO - fill in your directory for the 'nbainjrep_keydts.csv' file
DATADIR = 'C:/Users/Michael Xu/Desktop/Sports Analytics/Projects/Data'
requestheaders = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/120.0.0.0 Safari/537.36'}

# --- Don't change ---
# Set the Dimensions for Area and Columns for Extraction, derived from the Tabula app
# format of area params is [top (y1), left(x1), bottom (y1+height), right (x1+width)]
# format of column params is [x2_a (first col, second x coordinate), x1_b, x2_b. x1_c, ...] (total number of values is number of columns - 1)
area_params2223_a = [34.99559288024902, -0.9998814239502193, 566.508092880249, 843.1051185760498]
cols_params2223_a = [83.5684935760498, 157.24349357604981, 230.9184935760498, 360.3759935760498, 483.5184935760498,
                     590.8734935760498]
# use after Injury-Report_2023-05-02_05PM, for rest of the 22-23 season
area_params2223_b = [73.14440731048583, 1.7891566230775788, 530.9547264480591, 841.6342937995912]
cols_params2223_b = [113.34753324050922, 190.17547185440083, 259.6363478614809, 415.39710011978167, 576.4200399543764,
                     658.5101661445619]
area_params2324 = [76.30171985626221, 18.31240634193411, 534.1120389938354, 820.2697929691313]
cols_params2324 = [108.82203265418997, 183.545096237564, 255.11084727516163, 371.93141146888723, 543.4787264560698,
                   655.0371030735015]
area_params2425 = [76.30171985626221, 18.312425612640556, 534.1120389938354, 827.6368748466493]
cols_params2425 = cols_params2324

# Datetime constants for key season dates
dtfilepath = path.join(DATADIR, 'nbainjrep_keydts.csv')
df_keydts = pd.read_csv(dtfilepath, dtype=str)
dict_keydts = defaultdict(lambda: defaultdict(lambda: datetime.min))
for _, row_x in df_keydts.iterrows():
    if pd.isna(row_x['Date']) or pd.isna(row_x['Time']):
        dict_keydts[row_x['Season']][row_x['Desc']] = datetime.min
    else:
        dict_keydts[row_x['Season']][row_x['Desc']] = datetime.strptime(f"{row_x['Date']} {row_x['Time']}",
                                                                        '%Y-%m-%d %H:%M')


async def generate_repdts_url(ts_start: datetime, ts_end: datetime, sleep_dur: float = 0.4, **kwargs) -> list:
    """
    :param ts_start:
    :param ts_end:
    :param sleep_dur: default to 1.0
    :return: list[datetime] - list of datetimes whose corresponding injrep urls are valid
    """
    hourscount = 0
    dt_stamps = []
    tasks_tovalidate = []
    start_timer = time.perf_counter()
    async with ClientSession() as session:
        while (ts_start + timedelta(hours=hourscount)) <= ts_end:
            t = ts_start + timedelta(hours=hourscount)
            dt_stamps.append(t)
            url_x = NBALinkGen.gen_injrepnbacom(timestamp=t)
            tovalidate_x = asyncio.create_task(InjRepPDFScrape_async.validate_injrepurl(url_x, session, **kwargs),
                                               name=f'validate-{url_x}')
            tasks_tovalidate.append(tovalidate_x)
            hourscount += 1
            await asyncio.sleep(sleep_dur)
        validation_results = await asyncio.gather(*tasks_tovalidate, return_exceptions=True)  # list of bools

    # filter for datetimes for "true" validated report urls

    valid_dtrange = [dt_x for dt_x, is_valid in zip(dt_stamps, validation_results) if is_valid]
    end_timer = time.perf_counter()
    print(f'Total validation time of {len(dt_stamps)} urls, {sleep_dur} sleep = {end_timer - start_timer} sec.')
    print(
        f'Passed validation - {len(valid_dtrange)} urls; failed validation - {len(dt_stamps) - len(valid_dtrange)} urls.')
    return valid_dtrange


async def generate_repdts_local(ts_start: datetime, ts_end: datetime, datadir: str | PathLike,
                                sleep_dur: float = 0.01) -> list:
    """
    :param ts_start:
    :param ts_end:
    :param sleep_dur:
    :param datadir:
    :return: list[datetime] - list of datetimes whose corresponding injrep filepaths are valid
    """
    hourscount = 0
    dt_stamps = []
    tasks_tovalidate = []
    start_timer = time.perf_counter()
    while (ts_start + timedelta(hours=hourscount)) <= ts_end:
        t = ts_start + timedelta(hours=hourscount)
        dt_stamps.append(t)
        filepath_x = NBALinkGen.gen_injrep_dlpath(timestamp=t, directorypath=datadir)
        tovalidate_x = asyncio.create_task(InjRepPDFScrape_async.validate_localfile(filepath_x),
                                           name=f'validate-{filepath_x}')
        tasks_tovalidate.append(tovalidate_x)
        hourscount += 1
        await asyncio.sleep(sleep_dur)
    validation_results = await asyncio.gather(*tasks_tovalidate, return_exceptions=True)  # list of bools

    valid_dtrange = [dt_x for dt_x, is_valid in zip(dt_stamps, validation_results) if is_valid]
    end_timer = time.perf_counter()
    print(f'Total validation time of {len(dt_stamps)} local filepaths = {end_timer - start_timer} sec.')
    print(
        f'Passed validation - {len(valid_dtrange)} local filepaths; failed validation - {len(dt_stamps) - len(valid_dtrange)} local filepaths.')
    return valid_dtrange


async def maintest_extfrurl(start_dt: datetime, end_dt: datetime, samplesize: int, area_params: list, cols_params: list,
                            sleep_dur: float = 0.5, **kwargs):
    """
    :param area_params: area extraction parameters for first page or all pages (if uniform across all pages)
    :param cols_params: column extraction parameters for first page or all pages (if uniform across all pages)
    :param kwargs: headers (necessary), area_otherpgs: list | None = None, cols_otherpgs: list | None = None (if needed)
    :return:
    """
    dt_testrange = await generate_repdts_url(start_dt, end_dt, **kwargs)
    dt_testsample = random.sample(dt_testrange, k=samplesize)
    async with ClientSession() as mainsession:
        to_extract = []
        for reptime_x in dt_testsample:
            injrepurl_x = NBALinkGen.gen_injrepnbacom(timestamp=reptime_x)
            to_extract.append(
                InjRepPDFScrape_async.extract_injrepdata(filepath=injrepurl_x, filestorage='url', session=mainsession,
                                                         area_headpg=area_params, cols_headpg=cols_params, **kwargs))
            await asyncio.sleep(sleep_dur)
        extract_results = await asyncio.gather(*to_extract)

    dict_dfinjtest = {}
    for reptime_x, df_x in zip(dt_testsample, extract_results):
        filename = NBALinkGen.gen_injrepnbacom(reptime_x).split('/')[-1]
        df_x['ReportTime'] = reptime_x.strftime('%Y-%m-%d/%H:%M')
        df_x['ReportLink'] = NBALinkGen.gen_injrepnbacom(reptime_x)
        dict_dfinjtest[filename] = df_x
    return dict_dfinjtest


async def maintest_extfrlocal(start_dt: datetime, end_dt: datetime, datadir: str | PathLike, samplesize: int,
                              area_params: list, cols_params: list, sleep_dur: float = 1.0, **kwargs):
    """
    :param area_params: area extraction parameters for first page or all pages (if uniform across all pages)
    :param cols_params: column extraction parameters for first page or all pages (if uniform across all pages)
    :param kwargs: area_otherpgs: list | None = None, cols_otherpgs: list | None = None (if needed)
    :return:
    """
    dt_testrange = await generate_repdts_local(ts_start=start_dt, ts_end=end_dt, datadir=datadir)
    dt_testsample = random.sample(dt_testrange, k=samplesize)
    to_extract = []
    for reptime_x in dt_testsample:
        repfilepath_x = NBALinkGen.gen_injrep_dlpath(timestamp=reptime_x, directorypath=datadir)
        to_extract.append(InjRepPDFScrape_async.extract_injrepdata(filepath=repfilepath_x, filestorage='local',
                                                                   area_headpg=area_params,
                                                                   cols_headpg=cols_params, **kwargs))
        await asyncio.sleep(sleep_dur)
    extract_results = await asyncio.gather(*to_extract)

    dict_dfinjtest_local = {}
    for reptime_x, df_x in zip(dt_testsample, extract_results):
        filename = NBALinkGen.gen_injrepnbacom(reptime_x).split('/')[-1]
        df_x['ReportTime'] = reptime_x.strftime('%Y-%m-%d/%H:%M')
        df_x['ReportLink'] = NBALinkGen.gen_injrepnbacom(reptime_x)
        dict_dfinjtest_local[filename] = df_x
    return dict_dfinjtest_local


async def validateDL_url(start_dt: datetime, end_dt: datetime, sleep_dur: float = 2.0, dl_ceiling: int = 2, **kwargs):
    dt_validrange = await generate_repdts_url(start_dt, end_dt, **kwargs)
    tasks_todl = []
    start_timer = time.perf_counter()
    async with ClientSession() as session:
        for dt_x in dt_validrange:
            dl_x = asyncio.create_task(InjRepPDFScrape_async.lmtd_download_file(dt_x, session, DOWNLOAD_DATADIR,
                                                                                dl_ceiling, sleep_dur, **kwargs))
            tasks_todl.append(dl_x)
        await asyncio.gather(*tasks_todl)
    end_timer = time.perf_counter()
    print(f'Total download time of {len(dt_validrange)} files from web, {sleep_dur} sleep = {end_timer - start_timer} sec.')


async def extconcat_all(startdt: datetime, enddt: datetime, storage: str, validate: bool, sleep_dur: float, area_params: list, cols_params: list,
                        datadir: str | PathLike | None = None, exclude: list[datetime] = [], **kwargs) -> pd.DataFrame:
    """
    :param startdt:
    :param enddt:
    :param storage: specify 'local' or 'url'
    :param validate:
    :param sleep_dur:
    :param area_params: area extraction parameters for first page or all pages (if uniform across all pages)
    :param cols_params: column extraction parameters for first page or all pages (if uniform across all pages)
    :param datadir: required only if storage=='local'
    :param exclude: list of datetimes of exclude, relevant only if validate = False (optional)
    :param kwargs: kwargs: headers (necessary if storage=='url'), area_otherpgs: list | None = None,
    cols_otherpgs: list | None = None (if needed)
    :return:
    """
    if (storage == 'local'):
        if not datadir:
            raise ValueError("Data directory must be specified if file storage is local.")
        if validate:
            dt_validrange = await generate_repdts_local(ts_start=startdt, ts_end=enddt, datadir=datadir)
        else:
            dt_range = [startdt + timedelta(hours=i) for i in range(int((enddt - startdt).total_seconds() / 3600) + 1)]
            exclude_set = set(exclude)  # set is more efficient for lookup purposes
            dt_validrange = [dt_x for dt_x in dt_range if dt_x not in exclude_set]
        start_timer = time.perf_counter()
        to_extract = []
        for reptime_x in dt_validrange:
            repfilepath_x = NBALinkGen.gen_injrep_dlpath(timestamp=reptime_x, directorypath=datadir)
            toextract_x = asyncio.create_task(
                InjRepPDFScrape_async.extract_injrepdata(filepath=repfilepath_x, filestorage='local',
                                                         area_headpg=area_params,
                                                         cols_headpg=cols_params, **kwargs),
                name=f"extract-{repfilepath_x.split('/')[-1]}")
            to_extract.append(toextract_x)
            await asyncio.sleep(sleep_dur)
        extract_results = await asyncio.gather(*to_extract)

    elif (storage == 'url'):
        if validate:
            dt_validrange = await generate_repdts_url(ts_start=startdt, ts_end=enddt)
        else:
            dt_range = [startdt + timedelta(hours=i) for i in range(int((enddt - startdt).total_seconds() / 3600) + 1)]
            exclude_set = set(exclude)
            dt_validrange = [dt_x for dt_x in dt_range if dt_x not in exclude_set]
        start_timer = time.perf_counter()
        async with ClientSession() as ecallsession:
            to_extract = []
            for reptime_x in dt_validrange:
                injrepurl_x = NBALinkGen.gen_injrepnbacom(timestamp=reptime_x)
                toextract_x = asyncio.create_task(
                    InjRepPDFScrape_async.extract_injrepdata(filepath=injrepurl_x, filestorage='url',
                                                             session=ecallsession,
                                                             area_headpg=area_params, cols_headpg=cols_params,
                                                             **kwargs), name=f"extract-{injrepurl_x.split('/')[-1]}")
                to_extract.append(toextract_x)
                await asyncio.sleep(sleep_dur)
            extract_results = await asyncio.gather(*to_extract)

    else:
        raise ValueError("Invalid input for storage parameter - use 'local' or 'url'.")

    for reptime_x, df_x in zip(dt_validrange, extract_results):
        df_x['ReportTime'] = reptime_x.strftime('%Y-%m-%d/%H:%M')
    df_all = pd.concat(extract_results, ignore_index=True)
    end_timer = time.perf_counter()
    print(
        f'Extracted and aggregated data from {len(extract_results)} {storage} reports in {end_timer - start_timer} sec.')
    return df_all


if __name__ == "__main__":
    # TODO - Test 1 (Bulk Download Raw Data from URLs)
    start_dltest = dict_keydts['2425']['regseastart']
    end_dltest = InjRepPDFScrape_async.round_prev30min(datetime.now() - timedelta(days=2))
    asyncio.run(validateDL_url(start_dt=start_dltest, end_dt=end_dltest, headers=requestheaders))

    # # TODO - Test 2 (QC Local ETL)
    # start_localtest = dict_keydts['2425']['regseastart']
    # end_localtest = InjRepPDFScrape_async.round_prev30min(datetime.now() - timedelta(days=2))
    # finaltestdata_local = asyncio.run(maintest_extfrlocal(start_dt=start_localtest, end_dt=end_localtest, datadir=DOWNLOAD_DATADIR,
    #                                                       samplesize=10, area_params=area_params2425, cols_params=cols_params2425))
    # for repname_x, df_x in finaltestdata_local.items():
    #     df_x.to_csv(path.join(EXPORT_DATADIR, f"{repname_x.split('.')[-2]}.csv"), index=False)


    # # TODO - Test 3 (QC URL ETL)
    # start_urltest = datetime(year=2023, month=1, day=1, hour=00, minute=30)
    # end_urltest = datetime(year=2023, month=5, day=2, hour=16, minute=30)
    # finaltestdata_url = asyncio.run(
    #     maintest_extfrurl(start_dt=start_urltest, end_dt=end_urltest, samplesize=10, area_params=area_params2223_a,
    #                       cols_params=cols_params2223_a, headers=requestheaders))
    #
    # for repname_x, df_x in finaltestdata_url.items():
    #     df_x.to_csv(path.join(EXPORT_DATADIR, f"{repname_x.split('.')[-2]}.csv"), index=False)

    # # TODO - Test 4 (Total ETL from)
    # start_extcon = dict_keydts['2122']['plinstart']
    # end_extcon = dict_keydts['2122']['ploffend']
    # exclude_2122po = [*[datetime(year=2022, month=5, day=30, hour=00, minute=30) + timedelta(hours=i) for i in range(24)],
    #                   *[datetime(year=2022, month=5, day=31, hour=00, minute=30) + timedelta(hours=i) for i in range(24)],
    #                   *[datetime(year=2022, month=6, day=3, hour=00, minute=30) + timedelta(hours=i) for i in range(24)],
    #                   *[datetime(year=2022, month=6, day=6, hour=00, minute=30) + timedelta(hours=i) for i in range(24)],
    #                   *[datetime(year=2022, month=6, day=11, hour=00, minute=30) + timedelta(hours=i) for i in range(24)],
    #                   *[datetime(year=2022, month=6, day=14, hour=00, minute=30) + timedelta(hours=i) for i in range(24)]
    #                   ]
    # df_allreps_playoffs = asyncio.run(extconcat_all(startdt=start_extcon, enddt=end_extcon,
    #                                                   storage='url', validate=False, sleep_dur=0.018, area_params=area_params2223_a,
    #                                                   cols_params=cols_params2223_a, exclude=exclude_2122po, headers=requestheaders))
    # df_allreps_playoffs.to_csv(path.join(EXPORT_DATADIR, 'nba_allinjreps_playoffs2122test.csv'), index=False)

