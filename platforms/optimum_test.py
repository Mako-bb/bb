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

class OptimumTest():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']

        self.currentSession = requests.session()
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

        if type == 'testing':
            self._scraping(testing=False)

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

    def _scraping(self, testing = False):

        payloads = []

        payload = {}

        ids_guardados = self.__query_field('')

        request = self.currentSession.get('https://www.optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination/48265008/20/0?sort=1&filter=0')
        print(request.status_code())

        data_json = request.json()

        titulos = data_json['data']['result']['titles']

        for titulo in titulos:
            title = titulo['title']

            year = titulo['release_year']

            cast = titulo['actors'].split(', ')

            packages = [
                {
                    'Type' : 'transaction-vod',
                    'RentPrice' : titulo['price']
                }
            ]

            id_ = str(titulo['title_id'])

        if payload['Id'] in ids_guardados:
            print("Ya existe el id {}".format(payload['Id']))
        else:
            payloads.append(payload)
            ids_guardados.add(payload['Id'])
            print("Insertado {} - ({} / {})".format(payload['Title'], i + 1, len(data['titles'])))


        # if not testing
            # Upload(self._platform_code, self._created_at, testing=True)
