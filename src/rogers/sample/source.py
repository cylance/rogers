""" Lookup scripts for VT
"""
import pandas as pd
import asyncio
import aiohttp

import rogers.config as c

from rogers.logger import get_logger


BASE_URL = 'https://www.virustotal.com/vtapi/v2'
DEFAULT_HEADERS = {"Accept-Encoding": "gzip, deflate"}
DEFAULT_PARAMS = {'apikey': ''}


log = get_logger(__name__)


async def get(session, url, params):
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
    params = {'apikey': c.settings.get('VT_API_KEY'),
              'resource': hashval}
    return await get(session, url, params)


async def get_clusters(session):
    """
    :param session:
    :return:
    """
    url = "%s/file/clusters" % BASE_URL
    params = {'date': '2018-01-15'}
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


def runner(fcn, args=None):
    """
    :param fcn:
    :param args:
    :return:
    """
    loop = asyncio.get_event_loop()
    fs = asyncio.ensure_future(handler(fcn, args))
    loop.run_until_complete(fs)
    return fs.result()


def reports(hashvals):
    """
    :param hashvals:
    :return:
    """
    rets = runner(get_report, [(hashval, ) for hashval in hashvals])
    return dict(zip(hashvals, rets))


def main():
    cluster_response = runner(get_clusters)[0]
    cluster_labels = {}
    results = []
    if 'response_code' in cluster_response and cluster_response['response_code'] == 1:
        for cluster in cluster_response['clusters']:
            if cluster['size'] > 1000 and cluster['avg_positives'] > 40:
                cluster_labels[cluster['label']] = cluster['id']
    print(cluster_labels)
    print(len(cluster_labels))
    for label in cluster_labels.keys():
        def page_cluster(label, offest=None):
            query = 'cluster:"%s"' % cluster_labels[label]
            query_response = runner(search, (query, offest,))[0]
            if 'response_code' in query_response and query_response['response_code'] == 1:
                for h in query_response['hashes']:
                    results.append({'sha256': h.upper(), 'cluster_id': label})
            return query_response['offset']
        offset = None
        pages = 0
        while True:
            offset = page_cluster(label, offset)
            print(label, offset, len(results))
            pages += 1
            if offset is None or pages > 5:
                break
    df = pd.DataFrame.from_records(results)
    df.to_csv('vt_cluster_hashvals.csv', index=False)


if __name__ == "__main__":
    main()