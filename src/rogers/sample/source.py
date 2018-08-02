""" Lookup scripts for VT
"""
import asyncio
import gzip
import os

import aiohttp
import pandas as pd

from .. import config as c
from ..logger import get_logger
from ..util import sha256_key

BASE_URL = 'https://www.virustotal.com/vtapi/v2'
DEFAULT_HEADERS = {"Accept-Encoding": "gzip, deflate"}

SAMPLE_DIR = c.settings.get("SAMPLE_DIR")
DEFAULT_PARAMS = {'apikey': c.settings.get("VT_API_KEY")}

log = get_logger(__name__)

SEMAPHORE = asyncio.Semaphore(100)


async def get(session, url, params):
    async with SEMAPHORE:
        params.update(DEFAULT_PARAMS)
        async with session.get(url, params=params, headers=DEFAULT_HEADERS) as response:
            if response.status == 200:
                ret = await response.json()
                return ret
            else:
                log.error("%s: %s", url, response.status)


async def get_report(session, hashval):
    """
    :param session:
    :param hashval:
    :return:
    """

    url = "%s/file/report" % BASE_URL
    params = {'resource': hashval}
    return await get(session, url, params)


async def get_file(session, hashval):
    """
    :param session:
    :param hashval:
    :return:
    """
    sample_path = os.path.join(SAMPLE_DIR, sha256_key(hashval))
    if os.path.exists(sample_path):
        return "exists"
    sample_dir = os.path.dirname(sample_path)
    url = "%s/file/download" % BASE_URL
    params = {'hash': hashval}
    async with SEMAPHORE:
        params.update(DEFAULT_PARAMS)
        async with session.get(url, params=params, headers=DEFAULT_HEADERS) as response:
            if response.status == 200:
                os.makedirs(sample_dir, exist_ok=True)
                with gzip.open(sample_path, 'wb') as fout:
                    fout.write(await response.read())
            else:
                log.error("%s: %s", url, response.status)
            return str(response.status)


async def get_clusters(session, date):
    """
    :param session:
    :return:
    """
    url = "%s/file/clusters" % BASE_URL
    params = {'date': date}
    return await get(session, url, params)


async def search(session, query, offset=None):
    """
    :param session:
    :param query:
    :param offset:
    :return:
    """
    url = "%s/file/search" % BASE_URL
    params = {'query': query}
    if offset is not None:
        params['offset'] = offset
    return await get(session, url, params)


async def handler(fcn, args=None):
    """
    :param fcn:
    :param args:
    :param kwargs:
    :return:
    """
    tasks = []
    async with aiohttp.ClientSession() as session:
        if args is None:
            tasks.append(asyncio.ensure_future(fcn(session)))
        elif isinstance(args, list):
            for arg in args:
                task = asyncio.ensure_future(fcn(session, *arg))
                tasks.append(task)
        else:
            tasks.append(asyncio.ensure_future(fcn(session, *args)))
        return await asyncio.gather(*tasks)


def reports(hashvals):
    """
    :param hashvals:
    :return:
    """
    rets = runner(get_report, [(hashval,) for hashval in hashvals])
    return dict(zip(hashvals, rets))


def runner(fcn, *args, **kwargs):
    """
    :param fcn:
    :param args:
    :return:
    """
    loop = asyncio.get_event_loop()
    fs = asyncio.ensure_future(handler(fcn, *args, **kwargs))
    loop.run_until_complete(fs)
    return fs.result()


def download_files(hashvals):
    """
    :param hashvals:
    :return:
    """
    rets = runner(get_file, [(hashval,) for hashval in hashvals])
    return dict(zip(hashvals, rets))


def get_cluster_report(date, outfile="vt_cluster_hashvals.csv", min_cluster_size=500, min_avg_positives=20, max_pages=20):
    """
    :param outfile:
    :param min_cluster_size:
    :param min_avg_positives:
    :param max_pages:
    :return:
    """
    cluster_response = runner(get_clusters, (date,))[0]
    cluster_labels = {}
    results = []
    if 'response_code' in cluster_response and cluster_response['response_code'] == 1:
        for cluster in cluster_response['clusters']:
            if cluster['size'] > min_cluster_size and cluster['avg_positives'] > min_avg_positives:
                cluster_labels[cluster['label']] = cluster['id']
    for label in cluster_labels.keys():
        def page_cluster(label, offest=None):
            query = 'cluster:"%s"' % cluster_labels[label]
            query_response = runner(search, (query, offest,))[0]
            if 'response_code' in query_response and query_response['response_code'] == 1:
                for h in query_response['hashes']:
                    results.append({'sha256': h.upper(), 'cluster_id': label})
            return query_response['offset'] if 'offset' in query_response else None
        offset = None
        pages = 0
        while True:
            offset = page_cluster(label, offset)
            log.info("%s %s %s" % (label, offset, len(results)))
            pages += 1
            if offset is None or pages > max_pages:
                break
    df = pd.DataFrame.from_records(results)
    df.to_csv(outfile, index=False)
