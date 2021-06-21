from os import replace
import time
from typing import Dict, cast
import regex
import requests
import hashlib
import pymongo
import re
import json
import platform
from requests import api
import selenium
from handle.replace         import _replace
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
    La información que contiene cada url es el Título, Sinopsis y a veces el cast y el director
    VER PLATAFORMA DE CWSEED.PY QUE ES PARECIDA A ESTA."""

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

        
    def _get_request(self, url):
        """Método hace request por url"""
        req = self.sesion.get(url)
        return req
          
    def _get_url_list(self, req):
        """Método que busca url y devuelve una lista con ellos"""
        soup = BeautifulSoup(req.text, 'html.parser')
        contenedor = soup.find_all('a', href=True, itemprop=True)
        url_list = []
        for url in contenedor:
            #print(url)
            print('Found the URL: ', url['href'])
            url_list.append(url['href'])
        return url_list
        
    
    def _get_movies_or_series(self, url_list):
        """Método que hace una request por cada contenido y distingue si es serie o pelicula"""
        for url in url_list:
            req = self.sesion.get(url)
            soup = BeautifulSoup(req.text, 'html.parser')
            contents = soup.find('meta', content=True, itemprop=True)
            if int(contents['content']) > 1:
                self.series_list.append(req)
                print('serie')
            else:
                self.movies_list.append(req)
                print('Pelicula')
        return

    def _get_movie_payload(self, req):
        """metodo que extrae la info de html con bs4 y devuelve un payload de cada pelicula"""
        soup = BeautifulSoup(req.text, 'html.parser')
        name_html = soup.find('span', itemprop=True)#Busca la etiqueta
        for title in name_html:
            title = title.strip()
        content_description = soup.find('p', {'itemprop':'description'})
        for item in content_description:#Extraemos la Descripcion
            description = str(item)
        content_cast= soup.find('p', {'itemprop':'actor'})#Buscamos los actores del contenido en el html
        cast_list = []
        for item in content_cast:
            cast_list.append(str(item))#Creo una lista con el resultado de la busqueda de los actores
        cast_html = cast_list[2]#saco la string del cast que me interesa
        cast_html_list = re.split(string=cast_html, pattern= ',')
        cast = []#creo una lista de actores para el cast
        for item in cast_html_list:
            item = item.strip()#limpio todos los items de la lista de cast. (tabulaciones, etc.)
            cast.append(item)
        content_director = soup.find('p', {'itemprop':'director'})
        ###Agregar el Director y pasar a series    
        


    def _get_season_url_list(self, req):
        soup = BeautifulSoup(req.text, 'html.parser')
        contenedor = soup.find_all('h4', {"class": True})
        season_url_list = []
        for url in contenedor:
            print('Found the URL: ', url['href'])
            season_url_list.append(url['href'])
        return season_url_list

    def _scraping(self, testing=False):
        self.movies_list = []
        self.series_list = []
        req = self._get_request(self.start_url)#Hago una req a la plataforma
        #Esto esta hardcodeado para no hacer una recuest por pelicula hasta que resuelva las payloads
        lista_url_prueba = ['https://allblk.tv/winnie-mandela/', 'https://allblk.tv/nephew-tommy-just-thoughts/']
        list_url = self._get_url_list(req)#me traigo una lista de todos los contenidos que tiene
        prueba = lista_url_prueba
        self._get_movies_or_series(prueba)#diferencio los contenidos entre series y movies
        #self._get_movie_payload(prueba)
        for movie in self.movies_list:
            self._get_movie_payload(movie)
            print('extraer el contenido de la pelicula con un payload')
        for serie in self.series_list:
            print('Aca va el pyload de cada serie')
            seasons_url_list = self._get_season_url_list(serie)#url por temporadas
            episode_url_list = self._get_url_list(serie)#aca guardamos das las urls de los episodios 
           # for episode in episode_url_list:
            #    episode_req = self._get_request(episode)

        