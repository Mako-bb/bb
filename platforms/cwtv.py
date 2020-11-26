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

class CWtv():
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

        listPayload = []
        listPayloadEpi = []
        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        HOME_URL = "https://www.cwtv.com/"
        data_js = "https://images.cwtv.com/data/r_20201126090/shows/data.js"
        episode_metadata_url = "https://images.cwtv.com/feed/mobileapp/video-meta/apiversion_9/guid_"

        list_of_specials = self._scrape_specials()

        #gets slugs to build all the links for all content
        list_of_slugs = []
        list_of_content = requests.get(data_js).content.decode("utf-8").split("shows_list = ")[1].split(";")[0]
        decoder = json.JSONDecoder()
        list_of_content = decoder.decode(list_of_content)
        for content in list_of_content.keys():
            if list_of_content[content]["schedule"] == "cwtv":
                list_of_slugs.append(list_of_content[content]["slug"])
            else:
                title = list_of_content[content]["title"]
                print(f"{title} is a CW Seed exclusive and has been skipped.")
                self.skippedTitles += 1

        for slug in list_of_slugs:

            #so it doesn't include the "specials" section which is full of cast interviews and repeated episodes
            if slug == "more-video":
                continue

            deeplink = HOME_URL + "shows/" + slug

            try:
                guid_serie = requests.get(deeplink).url.split("?play=")[1]
            except IndexError:
                print(f"{deeplink} no contiene capítulos")
                self.skippedTitles += 1
                continue

            json_serie = Datamanager._getJSON(self, episode_metadata_url+guid_serie)["video"]
            soup_serie = Datamanager._getSoup(self, deeplink)

            #skips if the series is marked as anything other than an episode
            if json_serie["orig_content_type"] != "Full Episodes":
                print("No episodes available yet. skipped.")
                self.skippedTitles += 1
                continue

            id_ = hashlib.md5(deeplink.encode('utf-8')).hexdigest()
            title = json_serie["series_name"]

            payload = {
                'PlatformCode'      : self._platform_code,
                'Id'                : id_,
                'Type'              : "serie" if json_serie["orig_content_type"] == "Full Episodes" else json_serie["orig_content_type"],
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
                'Packages'          : [{"Type":"free-vod"}],
                'Country'           : None,
                'Timestamp'         : datetime.now().isoformat(),
                'CreatedAt'         : self._created_at
            }

            Datamanager._checkDBandAppend(self, payload, listDBMovie, listPayload)

            specials_episode_count = 1

            season_container = soup_serie.find("section", {"id":"videosandtouts"}).findAll("div")[0]

            for episode_link in season_container.find("ul",{"id":"list_1"}).findAll("a", {"class":"thumbLink"}):
                episode_link = episode_link.get("href")
                try:
                    guid_epi = episode_link.split("?play=")[1]
                except IndexError:
                    print("Not a valid link, doesn't contain slug.")
                    continue

                #skips the episode if it's an special episode contained in the more-video section of the website
                if guid_epi in list_of_specials:
                    self.skippedEpis += 1
                    print(f"{episode_link} links to an special episode that is part of the 'more-video' section and is not valid.")
                    continue

                json_epi = Datamanager._getJSON(self, episode_metadata_url+guid_epi)["video"]

                title_epi = json_epi["title"]
                season_epi = 0 if "special:" in title_epi.lower() else int(json_epi["season"])

                #set episode number depending if episode is an special or not
                if season_epi == 0:
                    episode_number = specials_episode_count
                    specials_episode_count += 1
                else:
                    episode_number = int(json_epi["episode"].replace(str(season_epi),"", 1))

                    external_id = None
                    if json_epi["tms_id"] != "":
                        external_id = [{'Provider': 'tms', 'Id': json_epi["tms_id"]}]

                payloadEpi = {
                    'PlatformCode'  : self._platform_code,
                    'ParentId'      : id_,
                    'ParentTitle'   : title,
                    'Id'            : guid_epi,
                    'ExternalIds'   : external_id,
                    'Title'         : title_epi,
                    #'SeasonName'    : seasonName,
                    'Episode'       : episode_number,
                    'Season'        : season_epi,
                    'Year'          : None,
                    'Duration'      : int(json_epi["duration_secs"])//60,
                    'Deeplinks'     : {
                        'Web': HOME_URL+(episode_link[1:]),
                        'Android': None,
                        'iOS': None
                    },
                    'Synopsis'      : json_epi["description_long"],
                    'Rating'        : json_epi["rating"],
                    'Provider'      : None,
                    'Genres'        : [json_epi["comscore_genre"]],
                    'Cast'          : None,
                    'Directors'     : None,
                    'Availability'  : json_epi["expire_time"].split("T")[0],
                    'Download'      : None,
                    'IsOriginal'    : None,
                    'IsAdult'       : None,
                    'Country'       : None,
                    'Packages'      : [{"Type":"free-vod"}],
                    'Timestamp'     : datetime.now().isoformat(),
                    'CreatedAt'     : self._created_at
                }

                Datamanager._checkDBandAppend(self, payloadEpi, listDBEpi, listPayloadEpi, isEpi=True)

        Datamanager._insertIntoDB(self,listPayload,self.titanScraping)
        Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios)

        self.sesion.close()
        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)

    def _scrape_specials(self):
        URL = "https://www.cwtv.com/shows/more-video"
        specials_guid_list = []

        specials_soup = Datamanager._getSoup(self, URL).find("section", {"id":"videosandtouts"}).findAll("div")[0]

        for link in specials_soup.find("ul",{"id":"list_1"}).findAll("a", {"class":"thumbLink"}):
            link = link.get("href")
            try:
                guid = link.split("?play=")[1]
                specials_guid_list.append(guid)
            except IndexError:
                print("Not a valid link, doesn't contain slug.")
                continue

        return specials_guid_list
