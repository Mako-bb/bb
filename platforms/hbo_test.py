# -*- coding: utf-8 -*-
import json
import time
import requests
import hashlib
import platform
import sys, os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from common                 import config
from bs4                    import BeautifulSoup, element
from datetime               import datetime, timedelta
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.datamanager     import Datamanager
from handle.replace         import _replace


class HBO_Test():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0

        if type == 'scraping':
            self._scraping()
        
        elif type == 'testing':
            self._scraping(testing=True)
        
        elif type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''

            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
            self._scraping()

    def _scraping(self, testing = False):
        
        req = self.sesion.get('https://www.hbo.com/movies/catalog')
        print(req.status_code, req.url)

        soup = BeautifulSoup(req.text, 'html.parser')

        contenedor = soup.find('div', {'class':'components/MovieGrid--container'})

        # print(contenedor)

        contenidos = contenedor.find_all('div', {'class':'modules/cards/CatalogCard--container modules/cards/MovieCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})

        # print(type(contenidos))
        # print(len(contenidos))

        for content in contenidos:

            title = content.find('p', {'class':'modules/cards/CatalogCard--title'}).text
            generos_y_rating = content.find('p', {'class':'modules/cards/CatalogCard--details'}).text

            genero = generos_y_rating.split('  |  ')[0]
            rating = generos_y_rating.split('  |  ')[1]

            title_depurado = title.replace(' - ', '-')
            title_depurado = title_depurado.replace(' ', '-')

            title_depurado = title_depurado.replace('*', '')
            title_depurado = title_depurado.replace(',', '')


            deeplink = 'https://www.hbo.com/movies/{}'.format(title_depurado)

            req_info = self.sesion.get(deeplink)
            print(req_info.status_code, req_info.url)

            soup_info = BeautifulSoup(req_info.text, 'html.parser')

            elemento_superior = soup_info.find('div', {'class':'modules/InfoSlice--content'})

            descripciones = elemento_superior.find_all('span', {'class':'components/AiringDetailsBlock--detailsText'})

            duration = ''
            year = ''

            for desc in descripciones:
                if 'MIN' in desc.text or 'HR' in desc.text:
                    duration = desc.text
                elif len(desc.text) == 4:
                    try:
                        year = int(desc.text.strip())
                    except:
                        continue


            if duration == '':
                duration = None
            else:
                horas = ''
                minutos = ''
                if 'HR' in duration:
                    horas = int(duration.split(' ')[0])
                if 'MIN' in duration:
                    if 'HR' in duration:
                        minutos = int(duration.split(' ')[2])
                    else:
                        minutos = int(duration.split(' ')[0])
                if horas == '':
                    horas = 0
                if minutos == '':
                    minutos = 0
                duration = horas * 60 + minutos

            if year == '':
                year = None
            elif year < 1870 or year > datetime.now().year():
                year = None


            print(title)
            print(year, ', ', duration)
            
            x = input()
