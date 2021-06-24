# -*- coding: utf-8 -*-
from collections import namedtuple
import json
import time
from pymongo.collation import validate_collation_or_none
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

class HBO_Fev():
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

    #documentaries_hbo_metadata = dict()
    #documentaries_hbo_metadata['title'] = ()
    #documentaries_hbo_metadata['duration'] = ()
    #documentaries_hbo_metadata['title_cleared'] = ()
    #documentaries_hbo_metadata['deeplink'] = ()

    def _scraping(self, testing = False):
        req = self.sesion.get('https://www.hbo.com/documentaries/catalog')
        print(req.status_code, req.url)

        soup = BeautifulSoup(req.text, 'html.parser')
        contenedor = soup.find('div', {'class':'components/MovieGrid--container'})
        #print (contenedor)

        contenidos = contenedor.find_all('div', {'class':'modules/cards/CatalogCard--container modules/cards/DocumentaryCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
        print("cantidad de documentales:", len(contenidos))

        deeplink_url_list = []
        for documentaries in contenidos:
            title = documentaries.find('p', {'class':'modules/cards/CatalogCard--title'})
            duration = documentaries.find('p', {'class':'modules/cards/CatalogCard--details'})
            #print (title.text, duration.text)
            
            title_cleared = title.text.lower() #mayusculas por minusculas
            title_cleared = title_cleared.replace(' ', '-') #espacios por guiones
            title_cleared = title_cleared.replace('&','and') #& por and
            title_cleared = title_cleared.replace(':','')#: por nada
            title_cleared = title_cleared.replace('9/11','september-11')#9/11 por september-11
            title_cleared = title_cleared.replace('/','-')#/ por -
            title_cleared = title_cleared.replace('.','')#. por nada
            title_cleared = title_cleared.replace('(','')#( por nada
            title_cleared = title_cleared.replace(')','')#) por nada
            title_cleared = title_cleared.replace(')','')#) por nada
            title_cleared = title_cleared.replace('!','')#! por nada
            title_cleared = title_cleared.replace('¢','cents')#¢ por centavos
            title_cleared = title_cleared.replace("'",'') #' por centavos
            title_cleared = title_cleared.replace("’",'') # ’ por nada
            title_cleared = title_cleared.replace('12th-and-delaware', '12th-and-delaware-doc') #tiene un doc al final
            title_cleared = title_cleared.replace('mavis', 'mavis-doc')
            title_cleared = title_cleared.replace('quinceaneras', 'quinceanera')
            #print(title_cleared)
          
            deeplink = 'https://www.hbo.com/documentaries/{}'.format(title_cleared)
            deeplink_url_list.append(deeplink)
       
        #print(deeplink_url_list)
        details_list = []

        try:
            for clean_url in deeplink_url_list:
                req_info = self.sesion.get(clean_url)
                soup2 = BeautifulSoup(req_info, 'html.parser')
                details = soup2.find('div', {'class':'modules/InfoSlice--assetDetails'})
                details_list.append(details)
                
        except:
            print("deeplink roto: " + deeplink )
    
        url_ejemplo = "https://www.hbo.com/documentaries/wig"
        req_ejemplo = self.sesion.get(url_ejemplo)
        print(req_ejemplo.status_code, req_ejemplo.url)
        soup_ejemplo = BeautifulSoup(req_ejemplo.text, 'html.parser')
        details_ejemplo = soup_ejemplo.find('div', {'modules/InfoSlice--assetDetails'})
        print(details_ejemplo)
    
    payload = {
        "Id": "1",
        "Title": "Titanic",
        "Type": "movie"# Lo pueden hacer completo.
        }
    
    print(payload)
    #self.mongo.insert("titanScraping", payload)