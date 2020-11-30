# -*- coding: utf-8 -*-
import time
import requests
import hashlib
import pymongo
import re
import json
import platform
from handle.replace     import _replace
from common             import config
from datetime           import datetime
from handle.mongo       import mongo
from handle.datamanager import Datamanager
from updates.upload     import Upload
from bs4                import BeautifulSoup


class CartoonNetwork:

    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode": self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
            self._scraping()

        if type == 'scraping':
            self._scraping()

    def __query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}
        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at,
        }
        find_filter.update(extra_filter)
        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )
        query = {item[field] for item in query}
        return query

    def _scraping(self):
        listDBMovie = Datamanager._getListDB(self, self.titanScraping)
        listaEpiDB = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        listaSeriesDB = Datamanager._getListDB(self, self.titanScraping)
        listaSeries = []
        listaEpi = []
        headers = {
            'Referer': 'https://www.freeform.com/',
            'DNT': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0',
            'appversion': '5.37.0'
        }

        packages = [
            {
                'Type': 'tv-everywhere',
            }
        ]

        url = requests.get("https://www.cartoonnetwork.com/video/index.html?atclk_vn=vn_link_homeImg&")
        soup = BeautifulSoup(url.content, 'html.parser')
        series = self.get_json(soup)
        for serie in range(len(series)):
            if series[serie]["videoIndexCanonicalUrl"] is not None:  # esto hace que no se metan juegos en las series
                payload = {
                    'PlatformCode': self._platform_code,
                    'Id': series[serie]['id'],
                    'Title': series[serie]['display_title'],
                    'CleanTitle': _replace(series[serie]['display_title']),
                    'OriginalTitle': None,
                    'Type': 'serie',
                    'Year': None,
                    'Duration': None,
                    'Deeplinks': {
                        'Web': self.get_url_serie(series, serie), # peanuts no tiene link
                        'Android': None,
                        'iOS': None,
                    },
                    'Playback': None,
                    'Synopsis': None,
                    'Image': series[serie]['search_thumbnail'],
                    'Rating': None,
                    'Provider': None,
                    'Genres': None,
                    'Cast': None,
                    'Directors': None,  # [str, str, str...]
                    'Availability': self.get_avaibility(series, serie),
                    'Download': None,
                    'IsOriginal': None,
                    'IsAdult': None,
                    'Packages': packages,
                    'Country': None,  # [str, str, str...]
                    'Timestamp': datetime.now().isoformat(),
                    'CreatedAt': self._created_at
                }
                Datamanager._checkDBandAppend(self, payload, listaSeriesDB, listaSeries)

                url = requests.get(self.get_url_serie(series, serie))
                soup = BeautifulSoup(url.content, 'html.parser')
                for capitulo in soup.find_all('div', {'class': 'feature-video-wrapper clearfix'}):
                    payloadEpi = {
                        'PlatformCode': self._platform_code,
                        'ParentId': series[serie]['id'],
                        'ParentTitle': _replace(series[serie]['display_title']),
                        'Id': hashlib.md5(capitulo.a['href'].encode('UTF-8')).hexdigest(),
                        'Title': self.get_episode_title(capitulo),
                        'Episode': self.get_number_episode(capitulo),
                        'Season': self.get_season(capitulo),
                        'Year': None,
                        'Duration': None,
                        'Deeplinks': {
                            'Web': "https://www.cartoonnetwork.com{}".format(capitulo.a['href']),
                            'Android': None,
                            'iOS': None
                        },
                        'Synopsis': None,
                        'Rating': None,
                        'Provider': None,
                        'Genres': None,
                        'Cast': None,
                        'Directors': None,
                        'Availability': None,
                        'Download': None,
                        'IsOriginal': None,
                        'IsAdult': None,
                        'Country': None,
                        'Packages': packages,
                        'Timestamp': datetime.now().isoformat(),
                        'CreatedAt': self._created_at
                    }
                    Datamanager._checkDBandAppend(self, payloadEpi, listaEpiDB, listaEpi, isEpi=True)
        Datamanager._insertIntoDB(self, listaSeries, self.titanScraping)
        Datamanager._insertIntoDB(self, listaEpi, self.titanScrapingEpisodios)
        """
        Upload
        """
        self.sesion.close()
        Upload(self._platform_code, self._created_at, testing=True)

    @staticmethod
    def get_json(soup):
        for script in soup.findAll('script'):
            if "_cnglobal.searchData" in script.text:
                script_data = script.text.replace("\n        _cnglobal.searchData = apiObjArrayToValidObjArray(", '')\
                    .replace(')', '').replace(";", '')
                return json.loads(script_data)

    @staticmethod
    def get_avaibility(series, serie):
        try:
            anio = series[serie]['video_index_end_date']
            match = re.search(r"\d{4}", anio)
            return int(match.group(0))
        except TypeError:
            return None

    @staticmethod
    def get_url_serie(series, serie):
        link = series[serie]["videoIndexCanonicalUrl"]
        if link is not None:
            return "https://cartoonnetwork.com{}".format(link)
        else:
            return "https://cartoonnetwork.com/{}".format(series[serie]["title"])

    @staticmethod
    def get_episode_title(capitulo): return capitulo.find('div', {'class': 'feature-video-title'}).text

    @staticmethod
    def get_season(capitulo):
        temporada = capitulo.find('span', {'class': 'feature-video-info-season'}).text
        return temporada.split(' ')[-1]

    @staticmethod
    def get_number_episode(capitulo):
        episode = capitulo.find('span', {'class': 'feature-video-info-aired-date'}).text
        return episode.split(' ')[-1]
