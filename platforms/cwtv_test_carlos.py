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
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys


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

        listPayload, listPayloadEpi = [], []
        listDBEpi = []
        listDB = []

        HOME_URL = "https://www.cwtv.com/"
        episode_metadata_url = "https://images.cwtv.com/feed/mobileapp/video-meta/apiversion_9/guid_"

        soup_home = Datamanager._getSoup(self, HOME_URL)

        for serie in soup_home.find("div",{"class":"footercol fcol1"}).findAll("a"):

            deeplink = HOME_URL + serie.get("href")

            guid_serie = requests.get(deeplink).url.split("?play=")[1]

            #obtiene json con ficha técnica que contiene un poco más de info de la serie
            json_serie = Datamanager._getJSON(self, episode_metadata_url+guid_serie)["video"]
            soup_serie = Datamanager._getSoup(self, deeplink)

            #skips if the series is a CW Seed platform exclusive
            if json_serie["show_type"] == "cw-seed":
                print("CW Seed exclusive. skipped.")
                self.skippedTitles += 1
                continue
            elif json_serie["orig_content_type"] != "Full Episodes":
                print("No episodes available yet. skipped.")
                self.skippedTitles += 1
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
                'Genres'            : [json_serie["comscore_genre"]],
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
            self._append_to_list(payload, listPayload)
#        Datamanager._checkDBandAppend(self, payload, listDBMovie, listPayload)

            season_container = soup_serie.find("section", {"id":"videosandtouts"}).findAll("div")[0]

            if "no-seasons" in season_container.find("div", {"id":"show-seasons"}).get("class"):
                for episode_link in season_container.find("ul",{"id":"list_1"}).findAll("a", {"class":"thumbLink"}):
                    episode_link = episode_link.get("href")
                    guid_epi = episode_link.split("?play=")[1]

                    json_epi = Datamanager._getJSON(self, episode_metadata_url+guid_epi)["video"]

                    payloadEpi = {
                        'PlatformCode'  : self._platform_code,
                        'ParentId'      : id_,
                        'ParentTitle'   : title,
                        'Id'            : json_epi["tms_id"],
                        'Title'         : json_epi["title"],
                        #'SeasonName'    : seasonName,
                        'Episode'       : int(json_epi["episode"][1:]),
                        'Season'        : int(json_epi["season"]),
                        'Year'          : None,
                        'Duration'      : int(json_epi["duration_secs"])/60,
                        'Deeplinks'     : {
                            'Web': episode_link,
                            'Android': None,
                            'iOS': None
                        },
                        'Synopsis'      : json_epi["description_long"],
                        'Rating'        : json_epi["rating"],
                        'Provider'      : None,
                        'Genres'        : [json_epi["comscore_genre"]],
                        'Cast'          : None,
                        'Directors'     : None,
                        'Availability'  : None,
                        'Download'      : None,
                        'IsOriginal'    : None,
                        'IsAdult'       : None,
                        'Country'       : None,
                        'Packages'      : [{"i dunno"}],
                        'Timestamp'     : datetime.now().isoformat(),
                        'CreatedAt'     : self._created_at
                    }
                    self._append_to_list(payloadEpi, listPayloadEpi)
                    #Datamanager._checkDBandAppend(self, payloadEpi, listDBEpi, listPayloadEpi, isEpi=True)
            else:
                for season_menu in season_container.find("div", {"id":"show-seasons-menu2"}).findAll("li"):
                    soup_season = Datamanager._getSoup(self,HOME_URL + season_menu.find("a").get("href"))
                    for episode_link in soup_season.find("section", {"id":"videosandtouts"}).findAll("div", recursive=False)[0].findAll("a", {"class":"thumbLink"}):
                        print("\n\n\n", len(soup_season.find("section", {"id":"videosandtouts"}).findAll("div", recursive=False)[0].findAll("a", {"class":"thumbLink"})), "\n\n\n")
                        episode_link = episode_link.get("href")
                        guid_epi = episode_link.split("?play=")[1]

                        json_epi = Datamanager._getJSON(self, episode_metadata_url+guid_epi)["video"]

                        payloadEpi = {
                            'PlatformCode'  : self._platform_code,
                            'ParentId'      : id_,
                            'ParentTitle'   : title,
                            'Id'            : json_epi["tms_id"],
                            'Title'         : json_epi["title"],
                            #'SeasonName'    : seasonName,
                            'Episode'       : int(json_epi["episode"][1:]),
                            'Season'        : int(json_epi["season"]),
                            'Year'          : None,
                            'Duration'      : int(json_epi["duration_secs"])/60,
                            'Deeplinks'     : {
                                'Web': episode_link,
                                'Android': None,
                                'iOS': None
                            },
                            'Synopsis'      : json_epi["description_long"],
                            'Rating'        : json_epi["rating"],
                            'Provider'      : None,
                            'Genres'        : [json_epi["comscore_genre"]],
                            'Cast'          : None,
                            'Directors'     : None,
                            'Availability'  : None,
                            'Download'      : None,
                            'IsOriginal'    : None,
                            'IsAdult'       : None,
                            'Country'       : None,
                            'Packages'      : [{"i dunno"}],
                            'Timestamp'     : datetime.now().isoformat(),
                            'CreatedAt'     : self._created_at
                        }
                        self._append_to_list(payloadEpi, listPayloadEpi)
                        #Datamanager._checkDBandAppend(self, payloadEpi, listDBEpi, listPayloadEpi, isEpi=True)

        #Datamanager._insertIntoDB(self,listPayload,self.titanScraping)
        #Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios)
        with open("series.json", "w"):
            json.dumps(listPayload)
        with open("episodes.json", "w"):
            json.dumps(listPayload)

        self.sesion.close()
        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)

    def _append_to_list(self, payload, list_of_payloads):
        if payload not in list_of_payloads:
            list_of_payloads.append(payload)
