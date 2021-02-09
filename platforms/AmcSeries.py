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


class AmcSeries():
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
        url_movie = 'https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/movies?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web'
        url_episode = 'https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/episodes?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web'
        episode_data = Datamanager._getJSON(self, url_episode)
        i = 0
        url_serie = 'https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/shows?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web'
        serie_data = Datamanager._getJSON(self, url_serie)
        movie_data = Datamanager._getJSON(self, url_movie)
        while True:
            try:
                if i == (len(episode_data['data']['children'][2]['children'])-1):
                    break
            except:
                break
            movies = movie_data['data']['children'][4]['children']
            episodes = episode_data['data']['children'][2]['children']
            shows = serie_data['data']['children'][4]['children']
            for movie in movies:
                payload_peliculas = {
                    "Title":         movie['properties']['cardData']['text']['title'],
                    "CleanTitle":    _replace(movie['properties']['cardData']['text']['title']),
                    "OriginalTitle": None,
                    "Type":          "movie",
                    "Year":          None,
                    "Duration":      None,

                    "Id":            movie['properties']['cardData']['meta']['nid'],
                    "Deeplinks": {

                        "Web":       'https://www.amc.com/tve?redirect={}'.format(movie['properties']['cardData']['meta']['permalink']),
                        "Android":   None,
                        "iOS":       None,
                    },
                    "Synopsis":      movie['properties']['cardData']['text']['description'],
                    "Image":         None,
                    "Rating":        None,  # Important!
                    "Provider":      None,
                    "Genres":        None,  # Important!
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
            for show in shows:
                payload_series = {
                    "PlatformCode":  self._platform_code,
                    "Id":            str(show['properties']['cardData']['meta']['nid']),
                    'Title':         show['properties']['cardData']['text']['title'],
                    "Type":          "serie",
                    'OriginalTitle': None,
                    'Year':          None,
                    'Duration':      None,
                    'Deeplinks': {
                        'Web':       'https://www.amc.com/tve?redirect={}'.format(show['properties']['cardData']['meta']['permalink']),
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    "CleanTitle":    _replace(show['properties']['cardData']['text']['title']),
                    'Synopsis':      show['properties']['cardData']['text']['description'],
                    'Image':         None,
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
            for episode, show in zip(episodes, shows):
                for episode_data in episode['children']:
                    filtro = str(
                        episode_data['properties']['cardData']['text']['seasonEpisodeNumber'])
                    season_ = re.sub(
                        '[A-Z] ', "", filtro)
                    episode__ = season_.split(', ')

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
                            'Web':       'https://www.amc.com/tve?redirect={}'.format(episode_data['properties']['cardData']['meta']['permalink']),
                            'Android':   None,
                            'iOS':       None,
                        },
                        'Playback':      None,
                        "CleanTitle":    _replace(show['properties']['cardData']['text']['title']),
                        'Synopsis':      episode_data['properties']['cardData']['text']['description'],
                        'Image':         None,
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

        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)
