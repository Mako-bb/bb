from handle.payload import Payload
import time
import requests
from handle.replace         import _replace
from common import config
from handle.mongo import mongo
from time import sleep
import re
from datetime               import datetime
from updates.upload         import Upload
import pandas
import numpy

class Starz_panda():
    """
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']

        self.api_url = self._config['api_url']
        self.api_series = self._config['api_series']
        self.session = requests.session()

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
            print('se ingresaron todos los contenidos correctamente')
            

        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self.request_series
            self._scraping(testing=True)
           
                    
    #def _scraping(self, testing=False):