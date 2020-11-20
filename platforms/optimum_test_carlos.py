# -*- coding: utf-8 -*-
import time
import requests
import hashlib
import pymongo
import re
import json
import platform
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from bs4                    import BeautifulSoup
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class Optimum_Test_Carlos():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.counting = 0

        self.sesion = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']

            self._scraping()

        if type == 'scraping':
            self._scraping()


    def _scraping(self):

        listPayload = []
        listDBMovie = Datamanager._getListDB(self,self.titanScraping)

        URL = "https://www.optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination/48265002/20/0?sort=1&filter=0"

        optimum_categories = ("Optimum A-E", "Optimum F-J", "Optimum K-O", "Optimum P-T", "Optimum U-Z", "Optimum Other")
        movie_categories = {
            "Optimum A-E":"48265002",
            "Optimum F-J":"48266002",
            "Optimum K-O":"48270002",
            "Optimum P-T":"48268002",
            "Optimum U-Z":"48269002",
            "Optimum Other":"48267002",
            'HBO': '213279202',
            'Starz': '175970002',
            'Starz Encore': '175982002',
            'Showtime': '210895202',
            'Cinemax': '7301002',
            'TOKU': '16841002',
            'here!': '167988002',
            'Eros Now': '23300002',
            'TMC': '44165002'
        }

        for category in movie_categories:
            json_with_movies = Datamanager._getJSON(self, f"https://www.optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination/{movie_categories[category]}/5000/0?sort=1&filter=0")

            for movie in json_with_movies["data"]["result"]["titles"]:
                title = movie["tms_title"] if movie.get("tms_title") else movie["title"]

                id = ""
                if movie.get("asset_id") in movie.keys():
                    id = movie["asset_id"]
                elif movie.get("hd_asset"):
                    id = movie["hd_asset"]
                elif movie.get("sd_asset"):
                    id = movie["sd_asset"]
                else:
                    print(f"Película '{title}' no contiene ID, imposible generar link. Skipeando.")
                    continue

                deeplink = f"https://www.optimum.net/tv/asset/#/movie/{id}"

                desc = ""
                if movie.get("long_desc") and movie["long_desc"] != "":
                    desc = movie["long_desc"]
                elif movie.get("short_desc") and movie["short_desc"] != "":
                    desc = movie["short_desc"]
                else:
                    desc = None

                genres = movie["genres"].split(", ") if "genres" in movie.keys() else None

                actors = movie["actors"].split(", ") if "actors" in movie.keys() else None

                directors = movie["directors"].split(", ") if "directors" in movie.keys() else None

                rent_price = float(movie["price"])

                payload = {
                    'PlatformCode':  self._platform_code,
                    'Id':            str(id),
                    'Title':         title,
                    'OriginalTitle': None,
                    'CleanTitle':    _replace(title),
                    'Type':          'movie',
                    'Year':          movie['release_year'] if movie['release_year'] >= 1870 and movie['release_year'] <= datetime.now().year else None,
                    'Duration':      movie['stream_length'] if movie['stream_length'] > 0 else None,
                    'Deeplinks': {
                        'Web':       deeplink,
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      desc,
                    'Image':         None,
                    'Rating':        movie['rating_system'],
                    'Provider':      [category] if category not in optimum_categories else "Optimum",
                    'Genres':        genres,
                    'Cast':          actors,
                    'Directors':     directors,
                    'Availability':  None,
                    'Download':      None,
                    'IsOriginal':    None,
                    'IsAdult':       None,
                    'Packages':      [{'Type':'transaction-vod', 'RentPrice': rent_price}],
                    'Country':       None,
                    'Timestamp':     datetime.now().isoformat(),
                    'CreatedAt':     self._created_at
                }

                Datamanager._checkDBandAppend(self, payload, listDBMovie, listPayload)

        Datamanager._insertIntoDB(self, listPayload, self.titanScraping)

"""
FALTA CONECTAR Y COMPARAR CON MONGO
"""
