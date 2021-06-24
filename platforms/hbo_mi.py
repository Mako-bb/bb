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
        
        urls={
            'docum':'https://www.hbo.com/documentaries/catalog',
            'movies':'https://www.hbo.com/movies/catalog',
            'series':'https://www.hbo.com/series/all-series',
        }

        docum_list=[]
        movies_list=[]
        series_list=[]
        content_payload={}

        for url,value in urls.items():
            req = self.sesion.get(value)
            soup = BeautifulSoup(req.text, 'html.parser')
            conteiner = soup.find('div', {'class':'components/MovieGrid--container'})
            if url == 'movies':
                contents = conteiner.find_all('div', {'class':'modules/cards/CatalogCard--container modules/cards/MovieCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
                for content in contents:
                    title = content.find('p', {'class':'modules/cards/CatalogCard--title'}).text
                    title_depurate=self.depurateTitle(title)
                    deeplink = 'https://www.hbo.com/movies/{}'.format(title_depurate)
                    req_info = self.sesion.get(deeplink)
                    soup_info = BeautifulSoup(req_info.text, 'html.parser')
                    element_superior = soup_info.find('div', {'class':'modules/InfoSlice--assetDetails'})
                    childsList=element_superior.find_all('span',{'class':'components/AiringDetailsBlock--detailsText'})
                    details=self.validateData(childsList,url)
                    sinop = soup_info.find('div', {'class':'modules/Text--text modules/Text--headerHeavy components/RichText--richText'}).p.text
                    content_payload={
                        'title':title,
                        'deepLink':deeplink,
                        'summary':sinop,
                        'details':details,
                    }
                    movies_list.append(content_payload)
            elif url == 'series':
                contents = conteiner.find_all('div', {'class':'modules/cards/CatalogCard--container modules/cards/SamplingCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
                for content in contents:
                    title = content.find('p', {'class':'modules/cards/CatalogCard--title'}).text
                    title_depurate=self.depurateTitle(title)
                    deeplink = 'https://www.hbo.com/{}'.format(title_depurate)
                    req_info = self.sesion.get(deeplink)
                    soup_info = BeautifulSoup(req_info.text, 'html.parser')
                    element_superior = soup_info.find('div', {'class':'modules/InfoSlice--assetDetails'})
                    childsList=element_superior.find_all('span')
                    details=self.validateData(childsList,url)
                    sinop = soup_info.find('div', {'class':'components/RichText--richText'}).p.text
                    content_payload={
                        'title':title,
                        'deepLink':deeplink,
                        'summary':sinop,
                        'details':details,
                    }
                    series_list.append(content_payload)
            else:
                contents = conteiner.find_all('div', {'class':'modules/cards/CatalogCard--container modules/cards/DocumentaryCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
                for content in contents:
                    title = content.find('p', {'class':'modules/cards/CatalogCard--title'}).text
                    title_depurate=self.depurateTitle(title)
                    deeplink = 'https://www.hbo.com/documentaries/{}'.format(title_depurate)
                    req_info = self.sesion.get(deeplink)
                    soup_info = BeautifulSoup(req_info.text, 'html.parser')
                    element_superior = soup_info.find('div', {'class':'modules/InfoSlice--assetDetails'})
                    childsList=element_superior.find_all('span',{'class':'components/AiringDetailsBlock--detailsText'})
                    details=self.validateData(childsList,url)
                    sinop = soup_info.find('div', {'class':'modules/Text--text modules/Text--headerHeavy components/RichText--richText'}).p.i.text
                    content_payload={
                        'title':title,
                        'deepLink':deeplink,
                        'summary':sinop,
                        'details':details,
                    }
                    docum_list.append(content_payload)
    
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
        
    def validateData(self, descriptions_list,type_content):
        details={}
        '''
        if type_content=='movies':
            genre=''
            rating=''
            duration=''
            year=''
            for desc in descriptions_list:
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
                hours = ''
                minutes = ''
                if 'HR' in duration:
                    hours = int(duration.split(' ')[0])
                if 'MIN' in duration:
                    if 'HR' in duration:
                        minutes = int(duration.split(' ')[2])
                    else:
                        minutes = int(duration.split(' ')[0])
                if hours == '':
                    hours = 0
                if minutes == '':
                    minutes = 0
                duration = hours * 60 + minutes

            if year == '':
                year = None
            elif year < 1870 or year > datetime.now().year():
                year = None
            
            details{
                'genre':genre,
                'rating':rating,
                'duration':duration,
                'year':year,
            }
        elif type_content=='series':
            seasons=''
            episodes=''
            rating=''

            details{
                'seasons':seasons,
                'episodes':episodes,
                'rating':rating,                
            }
        else:
            rating=''
            duration=''
            year=''

            details{
                'rating':rating,
                'duration':duration,
                'year':year,
            }
        '''
        return details
    
