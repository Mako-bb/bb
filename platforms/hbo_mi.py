# -*- coding: utf-8 -*-
import json
import time
import datetime
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
from selenium               import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC




class HboMI():
    """
    HBO es una ott de Estados Unidos que opera en todo el mundo.

    DATOS IMPORTANTES:
    - VPN: No
    - ¿Usa Selenium?: Si.
    - ¿Tiene API?: No.
    - ¿Usa BS4?: Si.
    - ¿Cuanto demoró la ultima vez?.
    - ¿Cuanto contenidos trajo la ultima vez?.

    OTROS COMENTARIOS:
    Saco con BS4 unicamente el title y los datos para generar el deeplink. Una vez se tiene el deeplink es necesario usar Selenium
    para cargarlo y completar el resto de los datos.
    Al cargarse el deeplink de algunos contenidos no estan disponibles y ofrecen redirigir a HBOMAX.
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_country = ott_site_country
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

        self.episodes_payloads = []
        self.moviesPayloads()
        #urls={'docums':'https://www.hbo.com/documentaries/catalog',
        #        'movies':'https://www.hbo.com/movies/catalog',
        #        'series':'https://www.hbo.com/series/all-series'}

    def moviesPayloads(self):
        PATH = 'C:\Program Files\chromedriver.exe'
        driver = webdriver.Chrome(PATH)
        req = self.sesion.get('https://www.hbo.com/movies/catalog')
        soup = BeautifulSoup(req.text, 'html.parser')
        conteiner= soup.find('div', class_="components/MovieGrid--container")
        contents=conteiner.find_all('div',{'class':'modules/cards/CatalogCard--container modules/cards/MovieCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
        counter=0
        for content in contents:
            image_list=[]
            status=True
            if counter==10:
                return 1
            title = content.find('p', {'class':'modules/cards/CatalogCard--title'}).text
            title_depurate=self.depurateTitle(title)
            deeplink = 'https://www.hbo.com/movies/{}'.format(title_depurate)
            deeplinksDict={
                "Web": deeplink,
                'Android': None,
                'iOS': None,
            }
            try:
                driver.get(deeplink)
                time.sleep(20)
                html=driver.page_source
                soup_info = BeautifulSoup(html, 'html.parser')
            except:
                status=False
            if status:
                details = soup_info.find('div', {'class':'components/AiringDetailsBlock--airingDetailsBlock'})
                childs = details.find_all('span', attrs={'class':'components/AiringDetailsBlock--detailsText'})
                image_conteiner=soup_info.find('div', {'class':'components/HeroImage--heroImageContainer'})
                image=image_conteiner.find('image')
                imgage_url=image['xlink:href']
                if '/content/dam' in imgage_url:
                    imgage_url= 'https://www.hbo.com'+imgage_url
                image_list.append(imgage_url)
                self.get_details(childs)
                type_='movie'
                packages=self.get_packages()
                #self.payloads.append(self.generic_payload(None,None,title,None,type_,year,duration,None,deeplinksDict,sinop,image_list,rating,genres,None,None,None,None,None,None,None))
            else:
                pass
            counter+=1

    def seriesPayloads(self,contents):
        #conteiner.find_all('div',{'class':'modules/cards/CatalogCard--container modules/cards/SamplingCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
        pass

    def documsPayloads(self,contents):
        #conteiner.find_all('div',{'class':'modules/cards/CatalogCard--container modules/cards/DocumentaryCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
        pass

    def depurateTitle(self, title):
        title = title.lower()
        chars = ' *.,!/\|¬"£$%^_+{@<>:;¿?[]()}`='
        special1 = "'"
        special2 = '&'
        newTitle = '' 
        if special1 in title:
            title = title.replace(special1,'')
        if special2 in title:
            title = title.replace(special2,'and')
        if '-' in title:
            title = title.replace('-'," ")
        for c in chars:
            title = title.replace(c,'-')
        for i in range(len(title)):
            if title[i - 1] =='-' and title[i]=='-':
                newTitle += ""
            else:
                newTitle += title[i]
        if newTitle[-1]=='-':
            newTitle = newTitle[:-1]
        if newTitle[0]=='-':
            newTitle = newTitle[1:]
        return newTitle
    
    def generic_payload(self,id_,crew,title,originalTitle,type_,year,duration,externalIds,deeplinks,
            synopsis, image, rating, genres, cast, directors, availability, download, isoriginal, isadult, isbranded):
        payload = { 
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": id_, #Obligatorio
            "Crew": crew,
            "Title": title, #Obligatorio 
            "CleanTitle": _replace(title), #Obligatorio 
            "OriginalTitle": originalTitle, 
            "Type": type_, #Obligatorio #movie o serie 
            "Year": year, #Important! 1870 a año actual 
            "Duration": duration, 
            "ExternalIds": externalIds,
            "Deeplinks": deeplinks, 
            "Synopsis": synopsis, 
            "Image": image, 
            "Rating": rating, #Important!  "Provider": , 
            "Genres": genres, #Important! 
            "Cast": cast, #Important! 
            "Directors": directors, #Important! 
            "Availability": availability, #Important! 
            "Download": download, 
            "IsOriginal": isoriginal, #Important! 
            "IsAdult": isadult, #Important! 
            "IsBranded": isbranded, #Important! (ver link explicativo)
            "Packages": self.get_packages(), #Obligatorio 
            "Country": [self.ott_site_country], 
            "Timestamp": datetime.datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }
        return payload

    def payload_episodes(self,id_,parentId,parentTitle,episode_num,season,crew,title,originalTitle,year,duration,externalIds,deeplinks,
            synopsis, image, rating, provider, genres, cast, directors, availability, download, isoriginal, isadult, isbranded):
        payload = { 
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": id_, #Obligatorio
            "ParentId": parentId, #Obligatorio #Unicamente en Episodios
            "ParentTitle":parentTitle , #Unicamente en Episodios 
            "Episode": episode_num, #Obligatorio #Unicamente en Episodios 
            "Season":season , #Obligatorio #Unicamente en Episodios
            "Crew": crew,
            "Title":title , #Obligatorio 
            "OriginalTitle": originalTitle, 
            "Year": year, #Important! 
            "Duration": duration, 
            "ExternalIds":externalIds ,
            "Deeplinks": deeplinks,
            "Synopsis": synopsis, 
            "Image":image , 
            "Rating":rating , #Important! 
            "Provider": provider, 
            "Genres": genres, #Important! 
            "Cast": cast, #Important! 
            "Directors": directors, #Important! 
            "Availability": availability, #Important! 
            "Download": download, 
            "IsOriginal": isoriginal, #Important! 
            "IsAdult": isadult, #Important! 
            "IsBranded": isbranded, #Important! 
            "Country": [self.ott_site_country], 
            "Timestamp": datetime.datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }
        return payload

    def get_packages(self):
        '''
            Se hardcodea el package hasta averiguar como conseguirlo apropiadamente.
        '''
        return [{'Type':'subscription-vod'}]

    def get_details(self, content_details):
        #genres=''
        #rating=''
        #duration=''
        #year=''        
        #sinop = soup_info.find('div', {'class':'modules/Text--text modules/Text--headerHeavy components/RichText--richText'})
        pass
