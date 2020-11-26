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
                    'Web': self.url_pelicula(pelicula),
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

    def _scraping_series(self):

        url = "https://prod.gatekeeper.us-abc.symphony.edgedatg.com/api/ws/pluto/v1/module/tilegroup/3376167?start=0&size=50&authlevel=0&brand=002&device=001"
        listaSeries = []
        listaSeriesDB = Datamanager._getListDB(self, self.titanScraping)
        listaEpiDB = []
        listaEpi = []
        listaEpiDB = Datamanager._getListDB(self, self.titanScrapingEpisodios)

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
                        'Web': self.url_serie(serie),
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

                url = requests.get(self.url_serie(serie))
                soup = BeautifulSoup(url.content, 'lxml')
                temporadas = soup.find_all('div', {'class': 'Carousel__Inner flex items-center'})
                for temporada in temporadas:
                    for episode in temporada.find_all('a'):
                        if re.search(r"episode-guide", episode['href']):
                            payloadEpi = {
                                'PlatformCode': self._platform_code,
                                'ParentId': str(serie['link']['id']),
                                'ParentTitle': _replace(serie['title']),
                                'Id': self.get_id_episode(episode),
                                'Title': self.get_title_episode(episode),  # episodio['title'],
                                'Episode': self.get_number_episode(episode),
                                'Season':  self.get_season(episode),
                                'Year': self.get_year(episode),  # rompe en Alone Together
                                'Duration': self.get_duration(episode),  # rompe en Alone Together
                                'Deeplinks': {
                                    'Web': "https://www.freeform.com{}".format(episode['href']),
                                    'Android': None,
                                    'iOS': None
                                },
                                'Synopsis': self.get_synopsis_episode(episode),  # episodio['description'],
                                'Rating': None,  # dataSerie['about']['rating'],
                                'Provider': None,
                                'Genres': [self.get_genre_episode(episode)],
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
        Datamanager._insertIntoDB(self, listaEpi, self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self, listaSeries, self.titanScraping)
        """
        Upload
        """
        self.sesion.close()
        Upload(self._platform_code, self._created_at, testing=True)

    # algunas películas no tienen la key con el link, por lo que esta funcion hace que todos la tengan
    # TODO: limpíar el título de fallen 2 porque la api lo devuelve como serie
    @classmethod
    def url_pelicula(cls, pelicula):
        try:
            return "https://www.freeform.com{}".format(pelicula['link']['urlValue'])
        except KeyError:
            titulo = pelicula['title']
            url = '-'.join(cls.limpia_titulo(titulo))
            return "https://www.freeform.com/movies-and-specials/{}".format(url)

    # algunas serie no tienen la key con el link, por lo que esta funcion hace que todos la tengan
    @classmethod
    def url_serie(cls, serie):
        try:
            url = serie['link']['urlValue'].split('/index')
            return "https://www.freeform.com{}".format(url[0])
        except KeyError:
            titulo = serie['title']
            url = '-'.join(cls.limpia_titulo(titulo))
            return "https://www.freeform.com/shows/{}".format(url)

    # limpia el título para poder generar la parte del link que usa el nombre
    @staticmethod
    def limpia_titulo(titulo):
        return titulo.lower().replace(':', '').replace(' -', '').replace('ñ', 'n')\
                .replace("'", '').replace(',', '').replace('!', '').replace(' &', '').split(' ')

    @staticmethod
    def get_titulo_serie(serie): return serie.split("/")[-1].capitalize()

    @staticmethod
    def get_year(episode):
        estreno = episode.find('div').div['data-track-video_air_date']
        match = re.search(r"\d{4}", estreno)
        return match.group(0)

    @staticmethod
    def get_duration(episode):
        duracion = episode.find('div').div["data-track-video_episode_length"]
        return int(duracion) // 60000

    @staticmethod
    def get_title_episode(episode):
        titulo = episode.find('span', {'class': 'tile__details-season-data'}).span.text
        return titulo.split('-')[1]

    @staticmethod
    def get_season(episode):
        season = episode.find('span', {'class': 'tile__details-season-data'}).span.text
        return int(re.findall(r"[0-9]+", season)[0])

    @staticmethod
    def get_number_episode(episode):
        number = episode.find('span', {'class': 'tile__details-season-data'}).span.text
        return int(re.findall(r"[0-9]+", number)[1])

    @staticmethod
    def get_id_episode(episode): return episode.find('div').div['data-track-video_id_code']

    @staticmethod
    def get_genre_episode(episode): return episode.find('div').div["data-track-video_genre"]

    @staticmethod
    def get_synopsis_episode(episode): return episode.find('span', {'class': 'tile__details-description'}).span.span.text
