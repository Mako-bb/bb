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
from bs4                    import BeautifulSoup as BS
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class Vh1_test():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.test = True if type == "testing" else False
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']
        self.currentSession = requests.session()
        self.payloads = []
        self.payloads_epi = []
        self.payloads_db = Datamanager._getListDB(self, self.titanScraping)
        self.payloads_epi_db = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        self.skippedTitles = 0
        self.skippedEpis = 0
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
        
        if type == 'scraping': #or self.testing :
            self._scraping()


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

    def _scraping(self):
        
        api_series = "http://www.vh1.com/feeds/ent_m150/de947f9a-0b22-4d65-a3ea-5d39a6d0e4f5"
        api_series_request = self.currentSession.get(api_series).json()

        items = api_series_request["result"]["data"]["items"]

        for item in items:
            for serie in item["sortedItems"]:

                id_serie = serie["itemId"]
                title_serie = serie["title"]
                cleantitle_serie = _replace(serie["title"])
                deeplink_serie = serie["url"]
                type_serie = "serie"
                 #hacemos una request para encontrar la descripcion dentro de la pagina
                request_serie = self.currentSession.get(deeplink_serie)
                html_serie = BS(request_serie.text,features="lxml")
                contenedor_synopsis = html_serie.find("div",{"id":"t5_lc_promo1"})
                synopsis_serie = contenedor_synopsis.find("div",{"class":"info"}).text

                contenedor_image = html_serie.find("div",{"class":"image_holder"})
                image_serie = contenedor_image["data-info"].split(",")[2].split(": ")[-1].replace('"',"")

                print(title_serie)
                print(image_serie)

                
                

                
