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
from handle.datamanager import Datamanager
from updates.upload import Upload
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from time import sleep
import re


class Amc():
    """ Plataforma: AMC
        Pais: Estados Unidos(US)
        Tiempo de Ejecución: 1>min
        Requiere VPN: No
        BS4/API/Selenium: API
        Cantidad de Contenidos (Ultima revisión):
            TitanScraping: 130
            TitanScrapingEpisodes: 746
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config(
        )['mongo']['collections']['episode']
        self.skippedEpis = 0
        self.skippedTitles = 0
        # Urls .YAML
        self._movies_url = self._config['movie_url']
        self._show_url = self._config['show_url']
        self._format_url = self._config['format_url']
        self._episode_url = self._config['episode_url']

        self.sesion = requests.session()
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

        if type == 'testing':
            self._scraping(testing=True)

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

    def _scraping(self, testing=False):
        payloads = []
        payloads_series = []
        ids_guardados = Datamanager._getListDB(self, self.titanScraping)
        ids_guardados_series = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)
        # Definimos los links de las apis y con el Datamanager usamos la función _getJson
        url_movie = self._movies_url
        url_episode = self._episode_url
        episode_data = Datamanager._getJSON(self, url_episode)
        i = 0
        url_serie = self._show_url
        serie_data = Datamanager._getJSON(self, url_serie)
        movie_data = Datamanager._getJSON(self, url_movie)

        while True:
            # condición que rompe el bucle infinito
            try:
                if i == (len(episode_data['data']['children'][2]['children'])-1):
                    break
            except:
                break
            # Ubicamos los valores dentro del Json
            movies = movie_data['data']['children'][4]['children']
            episodes = episode_data['data']['children'][2]['children']
            shows = serie_data['data']['children'][4]['children']
            # Recorremos el Json de Peliculas y definimos los contendios del Payload
            for movie in movies:
                print(movie)
                deeplink = (self._format_url).format(movie['properties']['cardData']['meta']['permalink'])
                payload_peliculas = {
                    "PlatformCode":  self._platform_code,
                    "Title":         movie['properties']['cardData']['text']['title'],
                    "CleanTitle":    _replace(movie['properties']['cardData']['text']['title']),
                    "OriginalTitle": None,
                    "Type":          "movie",
                    "Year":          None,
                    "Duration":      None,

                    "Id":            str(movie['properties']['cardData']['meta']['nid']),
                    "Deeplinks": {

                        "Web":       deeplink.replace('/tve?',''),
                        "Android":   None,
                        "iOS":       None,
                    },
                    "Synopsis":      movie['properties']['cardData']['text']['description'],
                    "Image":         [movie['properties']['cardData']['images']],
                    "Rating":        None,  # Important!
                    "Provider":      None,
                    "Genres":        [movie['properties']['cardData']['meta']['genre']],  # Important!
                    "Cast":          None,
                    "Directors":     None,  # Important!
                    "Availability":  None,  # Important!
                    "Download":      None,
                    "IsOriginal":    None,  # Important!
                    "IsAdult":       None,  # Important!
                    "IsBranded":     None,  # Important!
                    # Obligatorio
                    "Packages":      [{'Type': 'tv-everywhere'}],
                    "Country":       None,
                    "Timestamp":     datetime.now().isoformat(),  # Obligatorio
                    "CreatedAt":     self._created_at,  # Obligatorio
                }
                Datamanager._checkDBandAppend(
                    self, payload_peliculas, ids_guardados, payloads_series
                )
            # Recorremos el Json de las series y definimos los valores del diccionario 
            for show in shows:
                deeplink = (self._format_url).format(show['properties']['cardData']['meta']['permalink'])
                payload_series = {
                    "PlatformCode":  self._platform_code,
                    "Id":            str(show['properties']['cardData']['meta']['nid']),
                    'Title':         show['properties']['cardData']['text']['title'],
                    "Type":          "serie",
                    'OriginalTitle': None,
                    'Year':          None,
                    'Duration':      None,
                    'Deeplinks': {
                        'Web':       deeplink.replace('/tve?',''),
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    "CleanTitle":    _replace(show['properties']['cardData']['text']['title']),
                    'Synopsis':      show['properties']['cardData']['text']['description'],
                    'Image':         [show['properties']['cardData']['images']],
                    'Rating':        None,
                    'Provider':      None,
                    'Genres':        None,
                    'Cast':          None,
                    'Directors':     None,
                    'Availability':  None,
                    'Download':      None,
                    'IsOriginal':    None,
                    'IsAdult':       None,
                    'Packages':
                    [{'Type': 'tv-everywhere'}],
                        'Country':       None,
                        'Timestamp':     datetime.now().isoformat(),
                        'CreatedAt':     self._created_at
                }
                Datamanager._checkDBandAppend(
                    self, payload_series, ids_guardados, payloads_series
                )
            # recorremos el json de series y de episodios para poder definir las variables dentro de cada episodio.
            for episode, show in zip(episodes, shows):
                for episode_data in episode['children']:
                    # un filtro para limpiar la información concatenada del json.
                    filtro = str(
                        episode_data['properties']['cardData']['text']['seasonEpisodeNumber'])
                    season_ = re.sub(
                        '[A-Z] ', "", filtro)
                    episode__ = season_.split(', ')
                    deeplink = (self._format_url).format(episode_data['properties']['cardData']['meta']['permalink'])
                    payload = {
                        "PlatformCode":  self._platform_code,
                        "Id":            str(episode_data['properties']['cardData']['meta']['nid']),
                        "ParentId":      str(show['properties']['cardData']['meta']['nid']),
                        "ParentTitle":   show['properties']['cardData']['text']['title'],
                        "Episode":       int(episode__[1].replace('E', '')),
                        "Season":        int(episode__[0].replace('S', '')),
                        'Id':            str(episode_data['properties']['cardData']['meta']['nid']),
                        'Title':         episode_data['properties']['cardData']
                        ['text']['title'],
                        'OriginalTitle': None,
                        'Year':          None,
                        'Duration':      None,
                        'Deeplinks': {
                            'Web':       deeplink.replace('/tve?',""),
                            'Android':   None,
                            'iOS':       None,
                        },
                        'Playback':      None,
                        "CleanTitle":    _replace(show['properties']['cardData']['text']['title']),
                        'Synopsis':      episode_data['properties']['cardData']['text']['description'],
                        'Image':         [episode_data['properties']['cardData']['images']],
                        'Rating':        None,
                        'Provider':      None,
                        'Genres':        None,
                        'Cast':          None,
                        'Directors':     None,
                        'Availability':  None,
                        'Download':      None,
                        'IsOriginal':    None,
                        'IsAdult':       None,
                        'Packages':
                            [{'Type': 'tv-everywhere'}],
                        'Country':       None,
                        'Timestamp':     datetime.now().isoformat(),
                        'CreatedAt':     self._created_at
                    }
                    Datamanager._checkDBandAppend(
                        self, payload, ids_guardados_series, payloads, isEpi=True
                    )

        Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)
        Datamanager._insertIntoDB(
            self, payloads, self.titanScrapingEpisodios)
        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)