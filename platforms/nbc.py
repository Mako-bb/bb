# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from handle.datamanager     import Datamanager
from updates.upload         import Upload
from bs4                    import BeautifulSoup
from selenium.webdriver     import ActionChains
from handle.payload_testing import Payload
from platforms.BravoNBC              import BravoNBC
from platforms.oxygenNBC              import OxygenNBC
from platforms.SyfyNbc              import SyfyNBC
from platforms.telemundo            import Telemundo
import sys

class NBC():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']
        # self.driver                 = webdriver.Firefox()
        self.type2 = type


        self.sesion = requests.session()
        self.skippedTitles=0
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
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)

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
        """
        ¿VPN? SI
        ¿API,HTML o SELENIUM? API

        Telemundo por si sola no tiene api pero cuandop queres ver un contenido de telemundo la misma pagina te manda
        a NBC que presenta una api con todo el contenido de telemundo, por lo que hacer un scraping de telemundo o 
        hacerlo a NBC filtrando el contenindo a telemundo es casi lo mismo. Por lo que realizo con la api de NBC.
        """
        OxygenNBC(ott_site_uid='OxygenNBC',ott_site_country='US',type=self.type2)._scraping()
        SyfyNBC(ott_site_uid='SyfyNBC',ott_site_country='US',type=self.type2)._scraping()
        BravoNBC(ott_site_uid='BravoNBC',ott_site_country='US',type=self.type2)._scraping()
        Telemundo(ott_site_uid='Telemundo',ott_site_country='US',type=self.type2)._scraping()
    
