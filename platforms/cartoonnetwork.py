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
        for script in soup.findAll('script'):
            if "_cnglobal.searchData" in script.text:
                print(script.text)
        """for series in soup.find_all('li', {'class': 'showLink'}):
            show = requests.get(self.get_url_serie(series))
            soup = BeautifulSoup(show.content, 'html.parser')
            for serie in soup.find_all('section', {'id': 'episodes'}):
                id_serie = hashlib.md5(self.get_url_serie(serie).encode()).hexdigest()
                payload = {
                    'PlatformCode': self._platform_code,
                    'Id': id_serie,
                    'Title': self.get_titulo_serie(series),
                    'CleanTitle': _replace(self.get_titulo_serie(series)),
                    'OriginalTitle': None,
                    'Type': 'serie',  # 'movie' o 'serie'
                    'Year': None,
                    'Duration': None,  # duracion en minutos
                    'Deeplinks': {
                        'Web': self.get_url_serie(series),
                        'Android': None,
                        'iOS': None,
                    },
                    'Playback': None,
                    'Synopsis': None,
                    'Image': None,  # [str, str, str...] # []
                    'Rating': None,
                    'Provider': None,
                    'Genres': None,
                    'Cast': None,
                    'Directors': None,  # [str, str, str...]
                    'Availability': None,
                    'Download': None,
                    'IsOriginal': None,
                    'IsAdult': None,
                    'Packages': packages,
                    'Country': None,  # [str, str, str...]
                    'Timestamp': datetime.now().isoformat(),
                    'CreatedAt': self._created_at
                }
                Datamanager._checkDBandAppend(self, payload, listaSeriesDB, listaSeries)

                for episode in soup.find_all('a', {'class': 'feature-vide-a'}):
                    payloadEpi = {
                        'PlatformCode': self._platform_code,
                        'ParentId': id_serie,
                        'ParentTitle': _replace(self.get_titulo_serie(series)),
                        'Id': None,
                        'Title': "Titulo de prueba",  # self.get_titulo_episodio(episode),  # episodio['title'],
                        'Episode': None,
                        'Season': None,
                        'Year': None,  # rompe en Alone Together
                        'Duration': None,  # rompe en Alone Together
                        'Deeplinks': {
                            'Web': None,
                            'Android': None,
                            'iOS': None
                        },
                        'Synopsis': None,  # episodio['description'],
                        'Rating': None,  # dataSerie['about']['rating'],
                        'Provider': None,
                        'Genres': None,
                        'Cast': None,  # castList,
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
                    print(payloadEpi)"""

    @staticmethod
    def get_titulo_serie(serie): return serie.a.span.text

    @staticmethod
    def get_url_serie(serie): return "https://cartoonnetwork.com{}".format(serie.a['href'])

    @staticmethod
    def get_titulo_episodio(episode): return episode.div.div.text
