# -*- coding: utf-8 -*-
import time
from typing import Dict

import requests
import hashlib
import pymongo
import re
import json
import platform
from handle.replace     import _replace
from common             import config
from datetime           import datetime
from handle.mongo       import mongo
from handle.datamanager import Datamanager
from updates.upload     import Upload
from bs4                import BeautifulSoup

class HboPrueba:

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

        req = self.sesion.get('https://www.hbo.com/movies/catalog')

        soup = BeautifulSoup(req.text, 'html.parser')

        contenedor = soup.find('div', {'class':'components/MovieGrid--container'})

        # print(contenedor

        lista_contenidos = contenedor.find_all('div', {'class': 'modules/cards/CatalogCard--container modules/cards/MovieCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})

        # print(len(lista_contenidos))

        for contenido in lista_contenidos:

            title = contenido.find('p', {'class': 'modules/cards/CatalogCard--title'}).text

            print(title)

            # generos_y_rating = contenido.find('p', {'class':'modules/cards/CatalogCard--details'})
            # print(generos_y_rating)

            # generos = [generos_y_rating.text.split(' | ')[0]]
            # rating = generos_y_rating.text.split(' | ')[1]

            titulo_depurado = title.lower()
            titulo_depurado = titulo_depurado.replace(' - ', '-')
            titulo_depurado = titulo_depurado.replace(' ', '-')
            titulo_depurado = titulo_depurado.replace('&', 'and')

            titulo_depurado = titulo_depurado.replace('*', '')
            titulo_depurado = titulo_depurado.replace(',', '')
            titulo_depurado = titulo_depurado.replace('!', '')
            titulo_depurado = titulo_depurado.replace('¡', '')
            titulo_depurado = titulo_depurado.replace('/', '-')

            

            # string = "Hey! What's up?"
            # characters = "'!?"
            # for x in range(len(characters)):
            #     string = string.replace(characters[x],"")

            deeplink = 'https://www.hbo.com/movies/{}/'.format(titulo_depurado)

            req_prueba = self.sesion.get(deeplink)
            # print(req_prueba.url)
            
            if req_prueba.url == 'https://www.hbo.com/movies/catalog':
                print("La request fallo {}".format(req_prueba.status_code))
                asd = input(deeplink)

            soup_contenido = BeautifulSoup(req_prueba.text, 'html.parser')







