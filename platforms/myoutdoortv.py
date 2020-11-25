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
from handle.datamanager     import Datamanager
from updates.upload         import Upload

class MyOutdoorTV():
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
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)

    def _scraping(self, testing = False):
        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        listPayload = []
        listPayloadEpi = []
        scraped = self.__query_field(self.titanScraping, 'Id')
        scraped_episodes = self.__query_field(self.titanScrapingEpisodes, 'Id')
        ### {ID : {TITLE,DEEPLINK}} OF SERIES ###
        series_list = {}
        ### CATEGORIES WEBSITE
        path = ['/category/hunting',
            '/category/eu-hunting',
            '/category/fishing',
            '/category/adventure',
            '/category/subtitles',
            '/category/watchlists',
            '/category/global-watchlists',
            '/category/shooting',
            '/category/hunters-video-channel',
            '/category/recipes',
            '/category/tips']
        ### GET SERIES LIST
        offset = 0
        for category in path:
            URL = ('https://prod-api-cached-2.viewlift.com/content/pages?site=myoutdoortv&path='+category+'&includeContent=true&offset='+str(offset)+'&languageCode=en&countryCode=AR')
            reqjson = Datamanager._getJSON(self,URL)
            content_list = reqjson['modules'][1]['contentData']
            while content_list != []:
                for content in content_list:
                    content_id = content['gist']['id']
                    content_title = content['gist']['title']
                    deeplink = content['gist']['permalink']
                    contentType = content['gist']['contentType']
                    if not content_id in series_list:
                        series_list[content_id] = {'title':content_title,'deeplink':deeplink,'type':contentType}
                        
                offset += 20
                URL = ('https://prod-api-cached-2.viewlift.com/content/pages?site=myoutdoortv&path='+category+'&includeContent=true&offset='+str(offset)+'&languageCode=en&countryCode=AR')
                reqjson = Datamanager._getJSON(self,URL)
                content_list = reqjson['modules'][1]['contentData']
        ### GET SERIES & EPISODES INFO
        for serie in series_list:
            ### ID ###
            serie_id = serie
            ### TITLE ### 
            serie_title = series_list[serie]['title']
            ### DEEPLINK ###
            serie_deeplink = series_list[serie]['deeplink']
            #contentType = series_list[serie]['contentType']
            URL = 'https://prod-api-cached-2.viewlift.com/content/pages?path='+serie_deeplink+'&site=myoutdoortv&includeContent=true&moduleOffset=0&moduleLimit=6&languageCode=en&countryCode=AR'
            request = Datamanager._getJSON(self,URL)
            ### TYPE ### 
            typeOf = 'serie' 
            ### SYNOPSIS ###
            synopsis = request['metadataMap']['description']
            ### IMAGE ###
            image = request['metadataMap']['image']
            ### PACKAGE ### 
            packages = [
                            {
                            'Type': 'subscription-vod'
                            }
                        ]


            payload = {
                    'PlatformCode'      : self._platform_code,
                    'Id'                : serie_id,
                    'Type'              : typeOf,
                    'Title'             : serie_title,
                    'CleanTitle'        : _replace(serie_title),
                    'OriginalTitle'     : None,
                    'Year'              : None,
                    'Duration'          : None,
                    'Deeplinks'         : {
                                        'Web': serie_deeplink,
                                        'Android': None,
                                        'iOS': None
                    },
                    'Synopsis'          : synopsis,
                    'Image'             : image,
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

            if payload['Id'] in scraped:
                print("Ya existe el id {}".format(payload['Id']))
            else:
                listPayload.append(payload)
                scraped.add(payload['Id'])
                print("Insertado {} - ({} / {})".format(payload['Title'], i + 1))