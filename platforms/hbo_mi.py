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
from selenium               import webdriver


class HboMI():
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
        urls={'docums':'https://www.hbo.com/documentaries/catalog',
                'movies':'https://www.hbo.com/movies/catalog',
                'series':'https://www.hbo.com/series/all-series'}
        self.getContents(urls)
    
    def getContents(self,url_dict):
        for key,val in url_dict.items():
            source= self.sesion.get(val)
            soup = BeautifulSoup(source.text, 'html.parser')
            conteiner= soup.find('div', class_="components/MovieGrid--container")
            if key=='docums':
                docums=conteiner.find_all('div',
                    {'class':'modules/cards/CatalogCard--container modules/cards/DocumentaryCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
                self.documsPayloads(docums)
            elif key=='movies':
                movies = conteiner.find_all('div',
                    {'class':'modules/cards/CatalogCard--container modules/cards/MovieCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
                self.moviesPayloads(movies)
            else:
                series=conteiner.find_all('div',
                    {'class':'modules/cards/CatalogCard--container modules/cards/SamplingCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
                self.seriesPayloads(conteiner)
    
    
    def documsPayloads(self,contents):
        doc={}
        for content in contents:
            title = content.find('p', {'class':'modules/cards/CatalogCard--title'}).text
            title_depurate=self.depurateTitle(title)
            deeplink = 'https://www.hbo.com/documentaries/{}'.format(title_depurate)
            req_info = self.sesion.get(deeplink)
            soup_info = BeautifulSoup(req_info.text, 'html.parser')
            sinop = soup_info.find('div', {'class':'modules/Text--text modules/Text--headerHeavy components/RichText--richText'})
            print(title, deeplink,sinop)
            #element_superior = soup_info.find('div', {'class':'modules/InfoSlice--assetDetails'})
            #childsList=element_superior.find_all('span',{'class':'components/AiringDetailsBlock--detailsText'})
            #details=self.validateData(childsList,url)  
            #if self.mongo.search("titanScraping",doc)==False:
            #    self.mongo.insert("titanScraping",doc)
            #else:
            #    pass
    
    def moviesPayloads(self,contents):
        movie={}
        for content in contents:
            title = content.find('p', {'class':'modules/cards/CatalogCard--title'}).text
            title_depurate=self.depurateTitle(title)
            deeplink = 'https://www.hbo.com/movies/{}'.format(title_depurate)
            req_info = self.sesion.get(deeplink)
            soup_info = BeautifulSoup(req_info.text, 'html.parser')
            sinop = soup_info.find('div', {'class':'modules/Text--text modules/Text--headerHeavy components/RichText--richText'})
            print(title, deeplink,sinop)
            #element_superior = soup_info.find('div', {'class':'modules/InfoSlice--assetDetails'})
            #childsList=element_superior.find_all('span',{'class':'components/AiringDetailsBlock--detailsText'})
            #details=self.validateData(childsList,url)
            
            #if self.mongo.search("titanScraping",movie)==False:
            #    self.mongo.insert("titanScraping",movie)
            #else:
            #    pass
    
    def seriesPayloads(self,contents):
        series={}
        for content in contents:
            title = content.find('p', {'class':'modules/cards/CatalogCard--title'}).text
            title_depurate=self.depurateTitle(title)
            deeplink = 'https://www.hbo.com/{}'.format(title_depurate)
            req_info = self.sesion.get(deeplink)
            soup_info = BeautifulSoup(req_info.text, 'html.parser')
            sinop = soup_info.find('div', {'class':'components/RichText--richText'}).text
            print(title, deeplink,sinop)
            #element_superior = soup_info.find('div', {'class':'modules/InfoSlice--assetDetails'})
            #childsList=element_superior.find_all('span')
            #details=self.validateData(childsList,url)
            #if self.mongo.search("titanScraping",series)==False:
            #    self.mongo.insert("titanScraping",series)
            #else:
            #    pass


    def depurateTitle(self, title):
        chars=' *,./|&¬!"£$%^()_+{@:<>?[]}`=;¿'
        title=title.lower()#paso el titulo original a minusculas
        newTitle=''
        if '-' in title:#primero elimino los guiones que vengan con el titulo original
            title=title.replace('-'," ")
        for c in chars:#luego elimino el resto de los caracteres especiales
            title=title.replace(c,'-')
        if "'" in title:#elimino los apostrofes simples que quedan fuera de la lista de caracteres especiales, este paso quizas se pueda evitar de otro modo.
            title=title.replace("'","")
        for i in range(len(title)):#por ultimo elimino los dobles guiones medios que puedan llegar a quedar y almaceno el titulo final depurado en la variable nueva
            if i>0: 
                if newTitle[0]=='-':
                    newTitle=''
            if title[i - 1] =='-' and title[i]=='-':
                newTitle+=""
            else:
                newTitle+=title[i]
        return newTitle
    '''    
    def validateData(self, descriptions_list,type_content):
        pass
    '''
