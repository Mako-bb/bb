# -*- coding: utf-8 -*-
import json
import time
import requests
import hashlib
from common                 import config
from bs4                    import BeautifulSoup
from datetime               import datetime
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.datamanager  import Datamanager
from handle.replace         import _replace

class CWtv_Test_Carlos():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0

        if type == "scraping":
            self._scraping()
        elif type == "testing":
            self._scraping(testing=True)

    def _scraping(self, testing=False):

        HOME_URL = "https://www.cwtv.com/"
        episode_metadata = "https://images.cwtv.com/feed/mobileapp/video-meta/apiversion_9/guid_"

        soup_home = Datamanager._getSoup(self, HOME_URL)

        for serie in soup_home.find("div",{"class":"footercol fcol1"}).findAll("a"):

            deeplink = HOME_URL + serie.get("href")

            guid = requests.get(deeplink).url.split("?play=")[1]

            #obtiene json con ficha técnica que contiene un poco más de info de la serie
            json_serie = Datamanager._getJSON(self, episode_metadata+guid)["video"]

            #skips if the serie is a CW Seed platform exclusive
            if json_serie["show_type"] == "cw-seed":
                continue

            id_ = hashlib.md5(deeplink.encode('utf-8')).hexdigest()
            title = json_serie["series_name"]

            payload = {
                'PlatformCode'      : self._platform_code,
                'Id'                : id_,
                'Type'              : "serie",
                'Title'             : title,
                'CleanTitle'        : _replace(title),
                'OriginalTitle'     : None,
                'Year'              : None,
                'Duration'          : None,
                'Deeplinks'         : {
                                    'Web': deeplink,
                                    'Android': None,
                                    'iOS': None
                },
                'Synopsis'          : None,
                'Rating'            : json_serie["rating"],
                'Provider'          : None,
                'Genres'            : [genre for genre in json_serie["comscore_genre"]] if json_serie["comscore_genre"] != [] else None,
                'Cast'              : None,
                'Directors'         : None,
                'Availability'      : None,
                'Download'          : None,
                'IsOriginal'        : None,
                'IsAdult'           : None,
                'Packages'          : [{"Type":"free i dunno"}],
                'Country'           : None,
                'Timestamp'         : datetime.now().isoformat(),
                'CreatedAt'         : self._created_at
        }
#        Datamanager._checkDBandAppend(self, payload, listDBMovie, listPayload)



        self.sesion.close()
