# -*- coding: utf-8 -*-
import concurrent.futures
import requests

from pymongo import MongoClient
from socket import gethostname

try:
    from settings import settings
except ModuleNotFoundError:
    import sys
    import os
    path = os.path.abspath('.')
    sys.path.insert(1, path)
    from settings import settings

def get_http_session():
    http_session = requests.Session()
    adapters = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=20)
    http_session.mount('http://', adapters)
    http_session.mount('https://', adapters)
    http_session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36'

    return http_session


http_session = get_http_session()


def check_url(url):
    try:
        response = http_session.get(url, timeout=6)
    except Exception:
        return {'StatusCode': None, 'Deeplink': url}

    if response.history:
        response_to_check = response.history[-1]
    else:
        response_to_check = response

    result = {'StatusCode': response_to_check.status_code, 'Deeplink': url}

    is_redirect = response_to_check.status_code in range(300, 400)
    has_location = response_to_check.headers.get('Location')

    if is_redirect and has_location:
        result['Redirect'] = response_to_check.headers['Location']

    return result


class DeeplinkChecker:
    def __init__(self):
        self.db = MongoClient(settings.MONGODB_DATABASE_URI).titan
        self.errors = []
        self.hostname = gethostname()
        self._rate_limited = False

    def check(self, platform_code, created_at):
        self._check_links_from_aggregate('titanScraping', self._get_pipeline(platform_code, created_at, 'movie'))
        self._check_links_from_aggregate('titanScraping', self._get_pipeline(platform_code, created_at, 'serie'))
        self._check_links_from_aggregate('titanScrapingEpisodes', self._get_pipeline(platform_code, created_at))

        return self.errors

    def _get_pipeline(self, platform_code, created_at, content_type=None):
        pipeline = [
            {'$match': {'PlatformCode': platform_code, 'CreatedAt': created_at}},
            {'$sample': {'size': 80}},
            {'$project': {
                'PlatformCode': 1,
                'CreatedAt': 1,
                'Id': 1,
                'ParentId': 1,
                'Type': 1,
                'Deeplinks.Web': 1,
                'Episode': 1,
                'Season': 1,
            }}
        ]

        if content_type:
            pipeline[0]['$match']['Type'] = content_type

        return pipeline

    def _check_links_from_aggregate(self, col, pipeline):
        cursor = self.db[col].aggregate(pipeline)
        items = list(cursor)
        errors = self._run_concurrently(items, self.check_item)
        for err in errors:
            self._add_error(col, err)

    def check_item(self, item):
        if self._rate_limited:
            return

        check = self.check_deeplink(item['Deeplinks']['Web'])
        if check:
            check.update(
                {
                    'PlatformCode': item['PlatformCode'],
                    'CreatedAt': item['CreatedAt'],
                    'Id': item['Id'],
                    'Type': item.get('Type', 'episode'),
                }
            )
            return check

    def check_deeplink(self, url):
        # print(f'Checking {url}')
        check = check_url(url)
        if check['StatusCode'] == 429:
            self._rate_limited = True
        if check['StatusCode'] not in (200, 301, 302, 307):
            return check

    def _add_error(self, col, error):
        error['Collection'] = col
        error['Source'] = self.hostname
        self.errors.append(error)

    def _run_concurrently(self, items, func, max_workers=10, verbose=False, **kwargs):
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=func.__name__) as executor:
            futures = {executor.submit(func, item, **kwargs): item for item in items}

            for idx, future in enumerate(concurrent.futures.as_completed(futures)):
                key = futures[future]

                try:
                    res = future.result(timeout=6)
                    if res:
                        results.append(res)
                except Exception as exc:
                    print(f'Exception {func.__name__} {key}: <{exc.__class__.__name__}: {exc}>')

                if verbose:
                    print(f'Concurrent idx {idx}')

        return results
