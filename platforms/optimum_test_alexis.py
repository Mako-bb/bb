# -*- coding: utf-8 -*-
import time
import requests
import hashlib
import pymongo
import re
import json
import platform
from handle.replace import _replace
from common import config
from datetime import datetime
from handle.mongo import mongo
from slugify import slugify
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager import Datamanager
from updates.upload import Upload


class OptimumTest:

    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']
        self.skippedTitles = 0
        self.currentSession = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}
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
        listPayload = []

        ordenes = ['48265006', '48266006', '48270006', '48268006', '48269006']

        for orden in ordenes:
            offset = 0
            req = self.currentSession.get(
                "https://www.optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination"
                "/{}/20/{}?sort=1&filter=0".format(orden, offset))
            data = req.json()
            titulos = data['data']['result']['titles']
            for titulo in titulos:
                payload = {
                    'PlatformCode': self._platform_code,
                    'Id': str(titulo['title_id']),
                    'Title': titulo['tms_title'],
                    'OriginalTitle': None,
                    'CleanTitle': _replace(titulo['title']),
                    'Type': titulo['network'],
                    'Year': self.release_year(titulo['release_year']),
                    'Duration': self.duracion(titulo['stream_length']),
                    'Deeplinks': {
                        'Web': 'https://www.optimum.net/tv/asset/#/movie/{}'.format(str(titulo['title_id'])),
                        'Android': None,
                        'iOS': None,
                    },
                    'Playback': None,
                    'Synopsis': self.sinopsis(titulo['long_desc']),
                    'Image': None,
                    'Rating': titulo['rating_system'],
                    'Provider': None,
                    'Genres': titulo['genres'],
                    'Cast': titulo['actors'].split(', '),
                    'Directors': self.director(titulo),
                    'Availability': titulo['offer_end_date'],
                    'Download': None,
                    'IsOriginal': None,
                    'IsAdult': None,
                    'Packages': [
                        {
                            'Type': 'transaction-vod',
                            'RentPrice': titulo['price']
                        }
                    ],
                    'Country': None,
                    'Timestamp': datetime.now().isoformat(),
                    'CreatedAt': self._created_at
                }
                Datamanager._checkDBandAppend(self, payload, listDBMovie, listPayload)

            Datamanager._insertIntoDB(self, listPayload, self.titanScraping)

            if data['data']['result']['next'] == '0':
                break
            else:
                offset += 0

    @staticmethod
    def release_year(year):
        if 1870 <= year <= datetime.now().year:
            return year
        else:
            return None

    @staticmethod
    def duracion(tiempo):
        if tiempo > 0:
            return tiempo
        else:
            return None

    @staticmethod
    def sinopsis(descripcion):
        if descripcion != '':
            return descripcion
        else:
            return None

    @staticmethod
    def director(pelicula):
        if 'directors' in pelicula:
            return pelicula['directors']
        else:
            return None
