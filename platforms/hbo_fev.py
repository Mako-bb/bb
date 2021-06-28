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

    def _scraping(self, testing = False):
        self.payloads = []
        #self.contenidos_list = []
        req = self.sesion.get('https://www.hbo.com/documentaries/catalog')
        print(req.status_code, req.url)
        #Sopa de la página principal con todos los documentales
        soup = BeautifulSoup(req.text, 'html.parser')
        contenedor = soup.find('div', {'class':'components/MovieGrid--container'})
        #print (contenedor)
        #Dentro del contendor encontramos 448 contenidos (documentales)
        contenidos = contenedor.find_all('div', {'class':'modules/cards/CatalogCard--container modules/cards/DocumentaryCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
        print("cantidad de documentales:", len(contenidos))
        #print(contenidos)
        for contenido in contenidos:
            payload = self.get_documentaries_payload(contenido)
            self.payloads.append(payload)
            #return self.payloads
            print(payload)
        #for payload in self.payloads:
        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)       
        
         
                    
    def get_name(self, contenidos):
        title_list = []
        title = contenidos.find('p', {'class':'modules/cards/CatalogCard--title'})
        title_list.append(title.text)
        print (title)
        return title_list
        print(title_list)
    
    def get_duration(self, contenidos):
        duration_list = []          
        for documentaries in contenidos:
            duration = documentaries.find('p', {'class':'modules/cards/CatalogCard--details'})
            duration_list.append(duration.text)
        return duration_list
        print(duration_list)

    def get_documentaries_payload(self, contenido):
        title =  self.get_name(contenido)
        duration = self.get_duration(contenido)
        cleanTitle = self.get_title_clean(title)
        deeplink = self.get_deeplink(cleanTitle)
        #yerar = self._get_year()
        #rating = self_get_rating()      
        
        payload = { 
            "PlatformCode": None, #self._platform_code, #Obligatorio 
            "Id": None, #['_id'], #Obligatorio
            "Title": title, #['name'], #Obligatorio 
            "CleanTitle": cleanTitle, #Obligatorio 
            "OriginalTitle": None, #item['name'], 
            "Type": None, #item['type'], #Obligatorio 
            "Year": None, #None, #Important! 
            "Duration": duration,
            "ExternalIds": None, #None,  #No estoy seguro de si es
            "Deeplinks": { 
            "Web": deeplink, #deeplink, #Obligatorio 
            "Android":  None, 
            "iOS": None, 
            }, 
            "Synopsis": None, #item['summary'], 
            "Image": None, #[image],
            "Rating": None, #item['rating'], #Important! 
            "Provider": None, #None,
            "Genres": None, #[item['genre']], #Important!
            "Cast": None,
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": 'Free', #Obligatorio 
            "Packages": [{'Type':'free-vod'}],
            "Country": None, 
            "Timestamp": None, #datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": None, #self._created_at, #Obligatorio
            }
        self.payloads.append(payload)
        return payload
        
   
    def get_title_clean(self, title):
        for title in title:
                
            title_cleared = title.lower() #mayusculas por minusculas
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
            return title_cleared
            #print(title_cleared)
     
    def get_duration (self, documentaries):
        duration = documentaries.find('p', {'class':'modules/cards/CatalogCard--details'})
        return duration.text
           
    def get_deeplink(self, title_cleared):
        deeplink_url = 'https://www.hbo.com/documentaries/{}'.format(title_cleared)
        deeplink_url_list = []
        deeplink_url_list.append(deeplink_url)
        return deeplink_url, deeplink_url_list
       
    def get_details(self, deeplink_url_list):
        details_list = []
        for details in deeplink_url_list:
            try:
                for clean_url in deeplink_url_list:
                    req_info = self.sesion.get(clean_url)
                    soup2 = BeautifulSoup(req_info, 'html.parser')
                    details = soup2.find('div', {'class':'modules/InfoSlice--assetDetails'})
                    details_list.append(details)
                
            except:
                print("deeplink roto: ")
    
    #url_ejemplo = "https://www.hbo.com/documentaries/wig"
    #req_ejemplo = self.sesion.get(url_ejemplo)
    #print(req_ejemplo.status_code, req_ejemplo.url)
    #soup_ejemplo = BeautifulSoup(req_ejemplo.text, 'html.parser')
    #details_ejemplo = soup_ejemplo.find('div', {'modules/InfoSlice--assetDetails'})
    #print(details_ejemplo)