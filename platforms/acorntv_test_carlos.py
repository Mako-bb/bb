# -*- coding: utf-8 -*-
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

class AcornTV_Test_Carlos():
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

        URL = "https://acorn.tv/wp-admin/admin-ajax.php"
        data = "action=browse_order_filter&active_section=all&order_by=a-z&filter_by=all&token=a23f19d2f0"
        headers = {
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }

        listPayload = []
        listPayloadEpi = []

        packages = [
            {
                'Type': 'subscription-vod'
            }
        ]

        json = Datamanager._getJSON(self, URL, usePOST=True, data=data, headers=headers)

        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        soup = BeautifulSoup(json["data"]["html"], features="lxml")

        for item in soup.findAll("div", {"class":"col-sm-6 col-md-6 col-lg-3"}):

            title = item.find("p", {"class":"franchise-title"}).text.replace(" (Danish Version)", "").replace(" (Scandinavian Version)", "")

            if title == 'What is Acorn TV?' or "Coming Soon".lower() in title.lower():
                continue

            deeplink = item.find("a").get("href")
            image = item.find("img", {"class":"wp-post-image"}).get("src")

            soup = Datamanager._getSoup(self,deeplink)

            tipo = "movie" if soup.find("div", {"class":"franchise-eps-bg"}).find("h6").text.strip().lower() in ("movie", "feature") else "serie"
            desc = soup.find("p", {"id":"franchise-description"}).text

            id = hashlib.md5(deeplink.encode('utf-8')).hexdigest()

            payload = {
                'PlatformCode'      : self._platform_code,
                'Id'                : id,
                'Type'              : tipo,
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
                'Synopsis'          : desc,
                'Rating'            : None,
                'Provider'          : None,
                'Genres'            : None,
                'Cast'              : None,
                'Directors'         : None,
                'Availability'      : None,
                'Download'          : None,
                'IsOriginal'        : None,
                'IsAdult'           : None,
                'Packages'          : packages,
                'Country'           : None,
                'Timestamp'         : datetime.now().isoformat(),
                'CreatedAt'         : self._created_at
            }
            Datamanager._checkDBandAppend(self, payload, listDBMovie, listPayload)

            if tipo == "serie":
                for season in soup.findAll("span", {"itemprop":"containsSeason"}):
                    nroSeason = season.find("meta", {"itemprop":"seasonNumber"}).get("content")
                    for episode in season.findAll("span", {"itemprop":"episode"}):
                        linkEpi = episode.find("a", {"itemprop":"url"}).get("href")
                        idEpi = hashlib.md5(linkEpi.encode('utf-8')).hexdigest()
                        nroEpi = episode.find("span", {"itemprop":"episodeNumber"}).text if episode.find("span", {"itemprop":"episodeNumber"}).text != None else 1
                        nombreEpi = episode.find("h5", {"itemprop":"name"}).text

                        payloadEpi = {
                            'PlatformCode'  : self._platform_code,
                            'ParentId'      : id,
                            'ParentTitle'   : title,
                            'Id'            : idEpi,
                            'Title'         : nombreEpi,
                            #'SeasonName'    : seasonName,
                            'Episode'       : int(nroEpi),
                            'Season'        : int(nroSeason),
                            'Year'          : None,
                            'Duration'      : None,
                            'Deeplinks'     : {
                                'Web': linkEpi,
                                'Android': None,
                                'iOS': None
                            },
                            'Synopsis'      : None,
                            'Rating'        : None,
                            'Provider'      : None,
                            'Genres'        : None,
                            'Cast'          : None,
                            'Directors'     : None,
                            'Availability'  : None,
                            'Download'      : None,
                            'IsOriginal'    : None,
                            'IsAdult'       : None,
                            'Country'       : None,
                            'Packages'      : packages,
                            'Timestamp'     : datetime.now().isoformat(),
                            'CreatedAt'     : self._created_at
                        }
                        Datamanager._checkDBandAppend(self, payloadEpi, listDBEpi, listPayloadEpi)



        Datamanager._insertIntoDB(self,listPayload,self.titanScraping)
        Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios)

        self.sesion.close()
        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)
