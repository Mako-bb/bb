# -*- coding: utf-8 -*-
import time
import requests
import hashlib
import pymongo
import re
import json
import platform
from handle.replace                             import _replace
from common                                     import config
from datetime                                   import datetime
from handle.mongo                               import mongo
from slugify                                    import slugify
from bs4                                        import BeautifulSoup
from selenium                                   import webdriver
from selenium.webdriver.common.action_chains    import ActionChains
from selenium.webdriver.common.keys             import Keys
from handle.datamanager                         import Datamanager
from updates.upload                             import Upload


class Freeform:

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
            self._scraping_peliculas()
            self._scraping_series()

        if type == 'scraping':
            self._scraping_peliculas()
            self._scraping_series()

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

    def _scraping_peliculas(self):
        listDBMovie = Datamanager._getListDB(self, self.titanScraping)
        listPayload = []
        URL = "https://prod.gatekeeper.us-abc.symphony.edgedatg.com/api/ws/pluto/v1/module" \
              "/tilegroup/3380748?start=0&size=60&authlevel=0&brand=002&device=001"
        data_peliculas = Datamanager._getJSON(self, URL)

        packages = [
            {
                'Type': 'tv-everywhere',
            }
        ]

        for pelicula in data_peliculas['tiles']:
            payload = {
                'PlatformCode': self._platform_code,
                'Id': str(pelicula['show']['id']),
                'Title': pelicula['title'],
                'OriginalTitle': None,
                'CleanTitle': _replace(pelicula['title']),
                'Type': 'movie',
                'Year': None,  # html
                'Duration': None,  # html
                'Deeplinks': {
                    'Web': self.url(pelicula),
                    'Android': None,
                    'iOS': None,
                },
                'Playback': None,
                'Synopsis': pelicula['show']['aboutTheShowSummary'],
                'Image': None,  # html
                'Rating': None,  # html
                'Provider': None,
                'Genres': [pelicula['show']['genre']],
                'Cast': None,
                'Directors': None,
                'Availability': None,
                'Download': None,
                'IsOriginal': None,
                'IsAdult': None,
                'Packages': packages,
                'Country': None,
                'Timestamp': datetime.now().isoformat(),
                'CreatedAt': self._created_at
            }
            Datamanager._checkDBandAppend(self, payload, listDBMovie, listPayload)
        Datamanager._insertIntoDB(self, listPayload, self.titanScraping)

    url_series = []  # ESTO GUARDA TODOS LOS LINKS DE LAS SERIES QUE SIRVE PARA SCRAPPEARLAS. ¡¡¡¡¡ NO TOCAR !!!!!

    def _scraping_series(self):

        url = "https://prod.gatekeeper.us-abc.symphony.edgedatg.com/api/ws/pluto/v1/module" \
              "/tilegroup/3376167?start=0&size=50&authlevel=0&brand=002&device=001"
        listaSeries = []
        listaSeriesDB = Datamanager._getListDB(self, self.titanScraping)

        packages = [
            {
                'Type': 'tv-everywhere',
            }
        ]
        data_series = Datamanager._getJSON(self, url)

        for serie in data_series['tiles']:
            if serie['title'] != "Kickoff to Christmas":  # esto es un compilado de
                payload = {
                    'PlatformCode': self._platform_code,
                    'Id': str(serie['link']['id']),
                    'Title': serie['title'],
                    'CleanTitle': _replace(serie['title']),
                    'OriginalTitle': None,
                    'Type': 'serie',  # 'movie' o 'serie'
                    'Year': None,
                    'Duration': None,  # duracion en minutos
                    'Deeplinks': {
                        """
                            agrega el link de las series a una lista para scrappearlas, ya que toda la informacion
                            de las temporadas y sus respectivos capítulos están en el html
                        """
                        'Web': self.url(serie) and self.url_series.append(self.url(serie)),
                        'Android': None,
                        'iOS': None,
                    },
                    'Playback': None,
                    'Synopsis': serie['show']['aboutTheShowSummary'],
                    'Image': None,  # [str, str, str...] # []
                    'Rating': None,
                    'Provider': None,
                    'Genres': [serie['show']['genre']],
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
        Datamanager._insertIntoDB(self, listaSeries, self.titanScraping)
        """
        Upload
        """
        self.sesion.close()
        Upload(self._platform_code, self._created_at, testing=True)

    def _scraping_capitulos(self):
        listaEpiDB = []
        listaEpi = []
        listaEpiDB = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        packages = [
            {
                'Type': 'tv-everywhere',
            }
        ]

        for serie in self.url_series:
            pass


    # algunas series y películas no tienen la key con el link, por lo que esta funcion hace que todos la tengan
    @staticmethod
    def url(pelicula):
        try:
            return "https://www.freeform.com/movies-and-specials/{}".format(pelicula['link']['urlValue'])
        except KeyError:
            titulo = pelicula['title']
            palabras_separadas = titulo.lower().replace(':', '').replace(' -', '')\
                .replace("'", '').replace(',', '').replace('!', '').split(' ')
            url = '-'.join(palabras_separadas)
            return "https://www.freeform.com/movies-and-specials/{}".format(url)
