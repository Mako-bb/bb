# -*- coding: utf-8 -*-
import time
from typing import Dict

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

"""
    El scraping de la plataforma está dividido en dos funciones principales, una que scrapea las películas y 
    otra que lo hace con las series. Ambos tienen una api separada que devuelve todo lo necesario para el payload
    -y puede modificarse la cantidad de contenidos que trae, teniendo que cambiar los valores de start y size en el
    link de la api-. salvo el de las series que no tiene una api para los capítulos.
    
    La plataforma es free-vod ytv-everywhere dependiendo del contenido, ya que pide loguearse con un operador de cable 
    para poder ver algunas series en su totalidad, en otras todos los capítulos son gratis y en otros casos son
    únicamente tv-everywhere
    
    Los datos de los capítulos de todas las series se sacó del html, teniendo que mandar un único request a cada serie.
    Las funciones get_package y get_package_episode se encargan de ingresar el package de las series y capítulos de
    todas las series.
    
    Dato importante #1: en la categoría de series aparecen dos contenidos que no son series, siendo uno un compilado
    de películas navideñas (que no va a estar después de que pasen las fiestas) y un especial que no aparece en 
    IMDb.
    
    Dato importante #2: Fallen 2: The Journey tiene mal la url en la api, por lo que al ser un caso específico 
    en 50 películas está hardcodeado.
"""


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
                'Type': 'free-vod'
             }
        ]

        for pelicula in data_peliculas['tiles']:
            if pelicula['title'] != "31 Nights of Halloween Fan Fest":
                # esto es un compilado de películas de halloween que aparece en la api como película y no hay forma
                # de esquivarlo a menos que sea de esta forma
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
                    'Image': None,
                    'Rating': None,
                    'Provider': None,
                    'Genres': None,
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

        url = "https://prod.gatekeeper.us-abc.symphony.edgedatg.com/api/ws/pluto/v1/module" \
              "/tilegroup/3376167?start=0&size=50&authlevel=0&brand=002&device=001"

        listaSeries = []
        listaSeriesDB = Datamanager._getListDB(self, self.titanScraping)
        listaEpi = []
        listaEpiDB = Datamanager._getListDB(self, self.titanScrapingEpisodios)

        headers = {
            'Referer': 'https://www.freeform.com/',
            'DNT': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0',
            'appversion': '5.37.0'
        }

        data_series = Datamanager._getJSON(self, url, headers=headers)

        for serie in data_series['tiles']:
            if self.show_valido(serie) and serie['title'] != "The Clock Is Ticking with Yara Shahidi":
                # filtra el compilado de peliculas                          esto es un especial
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
                    'Packages': self.get_package(self.url_serie(serie)),
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
                        # find_all('a') devuelve muchos links basuras,
                        # lo que hace el if de abajo es filtrar los son útiles
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
                                'Packages': self.get_package_episode(episode),
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
    @classmethod
    def url_pelicula(cls, pelicula):
        try:
            if pelicula['title'] == "Fallen: The Journey":
                return "https://www.freeform.com/movies-and-specials/fallen-ii-the-journey"
            else:
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
    def get_synopsis_episode(episode):
        return episode.find('span', {'class': 'tile__details-description'}).span.span.text

    # Esta función lo que hace es recibir el link de la serie mientras se está haciendo el payload del mismo. Lo que
    # hace es guardar todos los payloads de todos los capítulos en una lista, lo pasa a set para eliminar los repetidos
    # y lo vuelve a pasar a lista para que sea del tipo que corresponda y pueda subirse a la base de datos
    @staticmethod
    def get_package(link):
        url = requests.get(link)
        soup = BeautifulSoup(url.content, 'html.parser')
        packages = []
        for capitulo in soup.find_all('div', {'class': 'tile__video__thumbnail'}):
            if capitulo.find('section', {'class': 'tile__top-right-details'}):
                packages.append({'Type': 'tv-everywhere'})
            else:
                packages.append({'Type': 'free-vod'})
        return list(map(dict, set(tuple(sorted(p.items())) for p in packages)))

    # en series hay un compilado de navidad que en la api aparece como collection, esta función lo que hace es filtrarla
    @staticmethod
    def show_valido(show):
        try:
            if show['collection']:
                return False
        except KeyError:
            return True

    # Devuelve el package de cada capítulo dependiendo de un tag en el html que muestra una llave, que significa que
    # el capítulo en cuestión es tv-everywhere
    @staticmethod
    def get_package_episode(episode):
        if episode.find('section', {'class': 'tile__top-right-details'}):
            return [{'Type': 'tv-everywhere'}]
        else:
            return [{'Type': 'free-vod'}]
