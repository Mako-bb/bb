import time
from typing import Dict

import requests
import hashlib
import pymongo
import re
import json
import platform
from requests import api
import selenium
from selenium.webdriver.firefox.webdriver import WebDriver
from handle.replace     import _replace
from common             import config
from datetime           import datetime
from handle.mongo       import mongo
from handle.datamanager import Datamanager
from updates.upload     import Upload
from bs4                import BeautifulSoup
from selenium           import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


class Allblk_panda:
    """Allblk es una plataforma estadounidense que si bien tiene todos los contenidos en una misma url
    es necesario ir a cada url de cada película para obtener la info.
    La información que contiene cada url es el Título, Sinopsis y a veces el cast y el director"""

    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0
        self.start_url = self._config['start_url']

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

        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing=True)

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

        

          
    def _get_url(self):
        """Método que trae una lista de url de cada contenido de la plataforma"""
        req = self.sesion.get(self.start_url)
        soup = BeautifulSoup(req.text, 'html.parser')
        contenedor = soup.find_all('a', href=True, itemprop=True)
        url_list = []
        for url in contenedor:
            #print(url)
            print('Found the URL: ', url['href'])
            url_list.append(url['href'])
        return url_list
    def _request_url(self, url_list):
        for url in url_list:
            req = self.sesion.get(url)
            soup = BeautifulSoup(req.text, 'html.parser')
            contents = soup.find('meta', content=True, itemprop=True)
            print(contents['content'])## Con esto puedo saber si es una serie o una pelicula.
            for content in contents:
                if content == dict:
                    print()


    def _scraping(self, testing=False):
        list_url = self._get_url() 
        req = self._request_url(list_url)
        return