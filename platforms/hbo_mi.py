# -*- coding: utf-8 -*-
import json
import time
import datetime
import requests
import hashlib
import platform
import sys
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from common import config
from bs4 import BeautifulSoup, element
import datetime
from handle.mongo import mongo
from updates.upload import Upload
from handle.datamanager import Datamanager
from handle.replace import _replace
from selenium import webdriver
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
    - ¿Tiene API?: Si. (Hay API pero sigo la consigna original de scrapear con BS4)
    - ¿Usa BS4?: Si.
    - ¿Cuanto demoró la ultima vez?.
    - ¿Cuanto contenidos trajo la ultima vez?.

    OTROS COMENTARIOS:
    Se scrapea BS4 unicamente el title del catalogo de contenidos y se generan los deeplink. Una vez se tiene el deeplink es necesario usar Selenium
    para cargar esa url y completar el resto de los datos ya que hay contenido dinamico. Hay deeplinks de contenido sin datos.
    Usando BS4 quedan muchos campos del payload sin completar, el id es uno de ellos. Podria manejarse con bash¿?
    Los datos que si se pueden completar son:
    title
    cleanTitle
    image
    deeplinks
    genres
    rating
    duration
    year
    package
    type
    sinopsis
    Timestamp
    country
    createdAt
    PlatformCode
    seasons *(series only)
    episodes *(series only)
    cast *(series only)
    crew *(series only)
    """

    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self._platform_code = self._config['countries'][ott_site_country]
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes = config(
        )['mongo']['collections']['episode']
        self.sesion = requests.session()
        self.skippedEpis = 0
        self.skippedTitles = 0

        if type == 'scraping':
            self._scraping()

        elif type == 'testing':
            self._scraping(testing=True)

        elif type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''

            params = {"PlatformCode": self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
            self._scraping()

    def _scraping(self, testing=False):
        self.payloads = []
        self.episodes_payloads = []
        #self.moviesPayloads()
        self.seriesPayloads()
        '''
        for payload in self.payloads:
            for key,val in payload.items():
                print(key,val)
                print('-----------')
        '''
    '''
    def moviesPayloads(self):
        PATH = 'C:\Program Files\chromedriver.exe'
        driver = webdriver.Chrome(PATH)
        req = self.sesion.get('https://www.hbo.com/movies/catalog')
        soup = BeautifulSoup(req.text, 'html.parser')
        conteiner = soup.find(
            'div', {'class': 'components/MovieGrid--container'})
        contents = conteiner.find_all('div', {
                                      'class': 'modules/cards/CatalogCard--container modules/cards/MovieCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
        counter = 0
        for content in contents:
            self.image_list = []
            self.details_dict = self.init_dict()
            status = True
            if counter == 5:
                return 1
            title = content.find(
                'p', {'class': 'modules/cards/CatalogCard--title'}).text
            title_depurate = self.depurateTitle(title)
            deeplink = 'https://www.hbo.com/movies/{}'.format(title_depurate) ###Esto se puede modularizar para conseguir deeplinks segun si el type por parametro es serie o movie
            deeplinksDict = {
                "Web": deeplink,
                'Android': None,
                'iOS': None,
            }
            try:
                self.driver.get(deeplink)
                time.sleep(20)
                html = self.driver.page_source
                soup_info = BeautifulSoup(html, 'html.parser')
            except:
                status = False
            if status:
                self.get_details(soup_info)
                self.get_sinopsis(soup_info)
                self.get_image(soup_info)
                packages = self.get_packages()
                type_ = 'movie'
                self.payloads.append(self.generic_payload(None,self.details_dict['crew'],title,None,type_,self.details_dict['year'],self.details_dict['duration'],None,deeplinksDict,self.details_dict['sinopsis'],self.image_list,self.details_dict['rating'],self.details_dict['genres'],self.details_dict['cast'],self.details_dict['directors'],None,None,None,None,None))
            else:
                print('No valid deeplink')
            counter += 1
    '''
    def seriesPayloads(self):
        PATH = 'C:\Program Files\chromedriver.exe'
        driver = webdriver.Chrome(PATH)
        req = self.sesion.get('https://www.hbo.com/series/all-series')
        soup = BeautifulSoup(req.text, 'html.parser')
        conteiner = soup.find(
            'div', {'class': 'components/MovieGrid--container'})
        contents = conteiner.find_all('div',{'class':'modules/cards/CatalogCard--container modules/cards/SamplingCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
        counter=0
        for content in contents:
            self.image_list = []
            self.details_dict = self.init_dict()
            status = True
            if counter==6:
                return 1
            title = content.find('p', {'class': 'modules/cards/CatalogCard--title'}).text
            title_depurate = self.depurateTitle(title)
            print(' ---- ',title,' ---- ')
            deeplink = 'https://www.hbo.com/{}'.format(title_depurate) ###Esto se puede modularizar para conseguir deeplinks segun si el type por parametro es serie o movie
            deeplinksDict = {
                "Web": deeplink,
                'Android': None,
                'iOS': None,
            }
            try:
                driver.get(deeplink)
                time.sleep(20)
                html = driver.page_source
                soup_info = BeautifulSoup(html, 'html.parser')
            except:
                status = False
            if status:
                self.get_series_details(soup_info)
                self.get_sinopsis_series(soup_info)
                '''
                if self.details_dict['seasons']:
                    seasons_deeplinks = self.get_seasons_deeplinks(self.details_dict['seasons'],self.details_dict['title'])
                    for season in seasons_deeplinks:
                        status=True
                        try:
                            driver.get(season)
                            time.sleep(20)
                            html = driver.page_source
                            soup_season = BeautifulSoup(html, 'html.parser')
                        except:
                            status = False
                        if status:
                            self.get_series_image(soup_season)
                else:
                    pass
                '''
                packages = self.get_packages()
                type_ = 'serie'
                #self.payloads.append(self.generic_payload(None,self.details_dict['crew'],title,None,type_,self.details_dict['year'],self.details_dict['duration'],None,deeplinksDict,self.details_dict['sinopsis'],self.image_list,self.details_dict['rating'],self.details_dict['genres'],self.details_dict['cast'],self.details_dict['directors'],None,None,None,None,None))
            else:
                print('No valid deeplink')
            counter += 1



    def documsPayloads(self, contents):
        #conteiner.find_all('div',{'class':'modules/cards/CatalogCard--container modules/cards/DocumentaryCatalogCard--container modules/cards/CatalogCard--notIE modules/cards/CatalogCard--desktop'})
        pass

    def depurateTitle(self, title):
        title = title.lower()
        chars = ' *.,!/\|¬"£$%^_+{@<>:;¿?[]()}`='
        special1 = "'"
        special2 = '&'
        newTitle = ''
        if special1 in title:
            title = title.replace(special1, '')
        if special2 in title:
            title = title.replace(special2, 'and')
        if '-' in title:
            title = title.replace('-', " ")
        for c in chars:
            title = title.replace(c, '-')
        for i in range(len(title)):
            if title[i - 1] == '-' and title[i] == '-':
                newTitle += ""
            else:
                newTitle += title[i]
        if newTitle[-1] == '-':
            newTitle = newTitle[:-1]
        if newTitle[0] == '-':
            newTitle = newTitle[1:]
        return newTitle

    def generic_payload(self, id_, crew, title, originalTitle, type_, year, duration, externalIds, deeplinks,
                        synopsis, image, rating, genres, cast, directors, availability, download, isoriginal, isadult, isbranded):
        payload = {
            "PlatformCode": self._platform_code,  # Obligatorio
            "Id": id_,  # Obligatorio
            "Crew": crew,
            "Title": title,  # Obligatorio
            "CleanTitle": _replace(title),  # Obligatorio
            "OriginalTitle": originalTitle,
            "Type": type_,  # Obligatorio #movie o serie
            "Year": year,  # Important! 1870 a año actual
            "Duration": duration,
            "ExternalIds": externalIds,
            "Deeplinks": deeplinks,
            "Synopsis": synopsis,
            "Image": image,
            "Rating": rating,  # Important!  "Provider": ,
            "Genres": genres,  # Important!
            "Cast": cast,  # Important!
            "Directors": directors,  # Important!
            "Availability": availability,  # Important!
            "Download": download,
            "IsOriginal": isoriginal,  # Important!
            "IsAdult": isadult,  # Important!
            "IsBranded": isbranded,  # Important! (ver link explicativo)
            "Packages": self.get_packages(),  # Obligatorio
            "Country": [self.ott_site_country],
            "Timestamp": datetime.datetime.now().isoformat(),  # Obligatorio
            "CreatedAt": self._created_at,  # Obligatorio
        }
        return payload

    def payload_episodes(self, id_, parentId, parentTitle, episode_num, season, crew, title, originalTitle, year, duration, externalIds, deeplinks,
                         synopsis, image, rating, provider, genres, cast, directors, availability, download, isoriginal, isadult, isbranded):
        payload = {
            "PlatformCode": self._platform_code,  # Obligatorio
            "Id": id_,  # Obligatorio
            "ParentId": parentId,  # Obligatorio #Unicamente en Episodios
            "ParentTitle": parentTitle,  # Unicamente en Episodios
            "Episode": episode_num,  # Obligatorio #Unicamente en Episodios
            "Season": season,  # Obligatorio #Unicamente en Episodios
            "Crew": crew,
            "Title": title,  # Obligatorio
            "OriginalTitle": originalTitle,
            "Year": year,  # Important!
            "Duration": duration,
            "ExternalIds": externalIds,
            "Deeplinks": deeplinks,
            "Synopsis": synopsis,
            "Image": image,
            "Rating": rating,  # Important!
            "Provider": provider,
            "Genres": genres,  # Important!
            "Cast": cast,  # Important!
            "Directors": directors,  # Important!
            "Availability": availability,  # Important!
            "Download": download,
            "IsOriginal": isoriginal,  # Important!
            "IsAdult": isadult,  # Important!
            "IsBranded": isbranded,  # Important!
            "Country": [self.ott_site_country],
            "Timestamp": datetime.datetime.now().isoformat(),  # Obligatorio
            "CreatedAt": self._created_at,  # Obligatorio
        }
        return payload

    def get_packages(self):
        '''
            Se hardcodea el package hasta averiguar como conseguirlo apropiadamente.
        '''
        return [{'Type': 'subscription-vod'}]

    def get_details(self, content):
        '''
            Los campos de genres, rating, duration, year vienen juntos en un contenedor,
            este metodo valida que el contenedor exista, ya que en algunos casos no viene (contenidos de hbomax).
            Luego saca los childs y limpia los separadores. Finalmente ejecuta el metodo validate_key que va a revisar a que campo corresponde
            cada child y lo asigna en el diccionario self.details_dict que sirve para llenar el payload final.
        '''
        try:
            details = content.find(
                'div', {'class': 'components/AiringDetailsBlock--airingDetailsBlock'})
            childs = details.find_all(
                'span', attrs={'class': 'components/AiringDetailsBlock--detailsText'}) 
            for child in childs:
                clean_child = child.text
                if '|' in clean_child:
                    clean_child = clean_child.split('|')[0]
                clean_child = clean_child.lower().strip()
                self.validate_key(clean_child)
        except:
            pass
    
    def get_series_details(self, content):
        try:
            details = content.find(
                'div', {'class': 'modules/InfoSlice--customInfo'})
            try:
                childs = details.find_all(
                    'span', attrs={'class': None})
                for child in childs:
                    clean_child = child.text
                    if '|' in clean_child:
                        clean_child = clean_child.split('|')[0]
                    clean_child = clean_child.lower()
                    self.validate_series_key(clean_child)
            except:
                other_child = details.text
                self.validate_series_key(other_child)
        except:
            print('No childs here!')
            print('')
    
    def get_image(self, content):
        '''
            Algunas url vienen completas y otras no, por eso la validacion con el if
            y se le concatena la parte faltante.
        '''
        try:
            image_conteiner = content.find(
                'div', {'class': 'components/HeroImage--heroImageContainer'})
            image = image_conteiner.find('image')
            imgage_url = image['xlink:href']
            if '/content/dam' in imgage_url:
                imgage_url = 'https://www.hbo.com'+imgage_url
            self.image_list.append(imgage_url)
        except:
            self.image_list = None

    def get_series_image(self, season):
        try:
            section = season.find(
                'section', {'class': 'components/WrapperContent--wrapperContent components/WrapperContent--fadeIn'})
            image_container = section.find(
                'div', {'class': 'components/CardImage--imageContainer'})
            imgage = image_container.find('img')
            imgage_url = imgage['src']
            if '/content/dam' in imgage_url:
                imgage_url = 'https://www.hbo.com'+imgage_url
            print(imgage_url)
            #self.image_list.append(imgage_url)
        except:
            self.image_list = None

    def get_seasons_deeplinks(self,season_num,season_title):
        pass

    def get_genres(self,content):
        '''
            Limpia los caracteres especiales de mas que pueda tener el genero como se recibe el dato.
            Primero valida si el genero pertenece a sci-fi, ya que en este caso no corresponde eliminar el caracter "-",
            finalmente elimina los espacios en blanco de los objetos de la lista.
        '''
        genres=[]
        chars='&/-_|,'
        ok=True
        for c in chars:
            if c in content:
                ok=False
                if (c=='-') and ('sci'in content):
                    pass
                else:
                    genres+=content.split(c)
        if ok:
            genres.append(content)
        genres = [x.strip(' ') for x in genres]
        return genres
    
    def validate_key(self, value):
        if (value.isnumeric()) and (len(value) == 4):
            self.details_dict['year'] = int(value)
        elif 'hr' in value or 'min' in value:
            duration = self.get_duration(value)
            self.details_dict['duration'] = duration
        elif len(value) > 2 and ('.' not in value and '-' not in value) or 'sci-fi' in value:
            genres = self.get_genres(value)
            self.details_dict['genres'] = genres
        elif '.' in value or value == 'hd':
            '''
            aca se guardan los valores de calidad de video (HD) y formato de sonido (5.1)
            en un futuro si estos datos se necesitan se puede hacer una validacion mejor para captar
            distintas resoluciones y formatos de sonido, no solo hd y 5.1. 
            '''
            pass
        else:
            self.details_dict['rating'] = value

    def validate_series_key(self,value):
        section=None
        if 'miniseries' in value:
            value = value.split('-')[0]
            value.strip()
            value = int(value) 
            section = 'episodes:'
        elif 'season' in value:
            value = value.split(' ')[0]
            value = int(value)
            section = 'seasons:'
        elif 'episode' in value:
            value = value.split(' ')[0]
            value = int(value)
            section = 'episodes:'
        else:
            section = 'rating:'
        print(section,value)
        

    def get_duration(self,value):
        horas = 0
        minutos = 0
        clean_value = value.split(' ')
        if 'hr' in clean_value:
            horas = int(clean_value[0])
        if 'min' in clean_value:
            if 'hr' in clean_value:
                minutos = int(clean_value[2])
            else:
                minutos = int(clean_value[0])
        duration = horas * 60 + minutos
        return duration
    
    def get_sinopsis(self, content):
        try:
            sinopsis_container = content.find('div', {'class': 'modules/Text--text modules/Text--headerHeavy components/RichText--richText'})
            p_tag = sinopsis_container.find('p')
            sinopsis_text = p_tag.text
            self.details_dict['sinopsis'] = sinopsis_text
        except:
            pass

    def get_sinopsis_series(self, content):
        try:
            sinopsis_container = content.find('div', {'class': 'components/RichText--richText'})
            try:
                p_tag = sinopsis_container.find('p')
                sinopsis_text = p_tag.text
                print(sinopsis_text)
                #self.details_dict['sinopsis'] = sinopsis_text
            except:
                sinopsis_text = sinopsis_container.text
                print(sinopsis_text)
                #self.details_dict['sinopsis'] = sinopsis_text
        except:
            print('No sinopsis here!')
            pass
    
    def init_dict(self):
        dict = {
            'genres' : None,
            'rating' : None,
            'duration' : None,
            'year' : None,
            'sinopsis' : None,
            'seasons' : None,
            'episodes' : None,
            'cast' : None,
            'directors': None,
            'crew': None,
        }
        return dict