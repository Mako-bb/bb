from os import replace
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
import itertools
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from bs4                    import BeautifulSoup
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager  import Datamanager
from updates.upload         import Upload
from handle.payload_testing import Payload

class WWENetwork():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']
        self.skippedTitles = 0
        self.skippedEpis = 0
        self.start_url = self._config['urls']['start_url']
        self.api_url = self._config['urls']['api_url']
        self.cdn_watch_url = self._config['urls']['cdn_watch_url']
        self.payloadsShows = []
        self.payloadsEpisodes = []

        self.sesion = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
                    
            self._scraping()
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)

    def _scraping(self, testing = False):
        """ Datos importantes:
                Necesita VPN: NO Al correr el script en Argentina o USA, trae el mismo contenido.
                Tiempo de ejecucion: Depende del internet, es todo con API. Aprox: 5 Mins.
        """

        saved_show_ids = Datamanager._getListDB(self,self.titanScraping)
        saved_episode_ids = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        api_url_originals = self.api_url + 'lists/1837?page={}'
        api_url_in_Ring = self.api_url + 'page?list_page_size=15&path=%2Fin-rings%2Fall-shows'

        total_pages = Datamanager._getJSON(self,api_url_originals.format(1)).get('paging').get('total')
        
        for i in range(1,total_pages):
            data = Datamanager._getJSON(self,api_url_originals.format(i))

            items = data['items']

            self.save_all_shows(items, saved_show_ids,saved_episode_ids)
        
        in_rings_data = Datamanager._getJSON(self,api_url_in_Ring)
        in_rings_items = in_rings_data.get('entries')[0]['list']['items']

        self.save_all_shows(in_rings_items, saved_show_ids,saved_episode_ids)

        Datamanager._insertIntoDB(self,self.payloadsShows,self.titanScraping)
        Datamanager._insertIntoDB(self,self.payloadsEpisodes,self.titanScrapingEpisodios)
        
        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)
        
    def save_all_shows(self, items, saved_show_ids, saved_episode_ids):
        """ Funcion que almacena todos los shows con el Datamaganer
            y extrae los episodios de cada show.
        """
        for item in items:
            payload = self.get_show_payload(item)
            
            Datamanager._checkDBandAppend(self,payload.payloadJson(),saved_show_ids,self.payloadsShows)

            self.extract_episodes(payload, saved_episode_ids)

    def get_show_payload(self, item):
        """ Funcion para obtener la payload completa de un show dado un diccionario.
        
            Returns:
                Payload()
        """
        payload = Payload()

        payload.platformCode = self._platform_code
        payload.createdAt = self._created_at
        payload.timestamp = datetime.now().isoformat()
        try:
            payload.year = item.get('releaseDate').split('-')[0]
        except:
            pass
        payload.genres = item['genres']
        if item['customFields']['Class'] == 'Original':
            payload.isOriginal = True
        payload.id = item['id']
        payload.title = item['title']
        print(payload.title)
        payload.cleanTitle=_replace(payload.title)
        payload.type = 'serie'
        payload.deeplinksWeb = self.cdn_watch_url + item['path']
        payload.packages=[{'Type' : 'subscription-vod'}]
        try:
            payload.rating = item.get('classification').get('name')
        except:
            pass
        return payload

    def extract_episodes(self,show_payload, saved_episode_ids):
        episode_url = self.api_url + 'filter/episodes?page_size=100&showIds={}&page={}'

        total_pages = Datamanager._getJSON(self,episode_url.format(show_payload.id,1)).get('paging').get('total')

        if total_pages == 1:
            episode_data = Datamanager._getJSON(self,episode_url.format(show_payload.id,1))
            
            for item in episode_data['items']:
                payload = self.get_episode_payload(show_payload, item)

                Datamanager._checkDBandAppend(self,payload.payloadEpisodeJson(),saved_episode_ids,self.payloadsEpisodes,isEpi=True)
        else:
            for i in range(1,total_pages):
                episode_data = Datamanager._getJSON(self,episode_url.format(show_payload.id,i))

                for item in episode_data['items']:
                    payload = self.get_episode_payload(show_payload, item)

                    Datamanager._checkDBandAppend(self,payload.payloadEpisodeJson(),saved_episode_ids,self.payloadsEpisodes,isEpi=True)

    def get_episode_payload(self, show_payload, item):
        """ Funcion para obtener la payload completa de un episodio dado un diccionario
            y la payload de su show.
        
            Returns:
                Payload()
        """
        
        payload = Payload()
        payload.id = item['id']
        payload.parentId = show_payload.id
        payload.parentTitle = show_payload.title
        payload.title = item['episodeName']
        payload.genres = item['genres']
        try:
            payload.year = item.get('releaseDate').split('-')[0]
        except:
            pass
        try:
            payload.rating = item.get('classification').get('name')
        except:
            pass
        payload.duration = int(int(item.get('duration'))/60)
        payload.season = int(item['customFields']['SeasonNumber'])
        payload.episode = int(item['episodeNumber'])
        payload.synopsis = item['shortDescription']
        if payload.synopsis == '-1':
            payload.synopsis = None
        payload.packages= [{'Type' : 'subscription-vod'}]
        payload.deeplinksWeb = self.cdn_watch_url + item['path']
        payload.platformCode = self._platform_code
        payload.createdAt = self._created_at
        payload.timestamp = datetime.now().isoformat()

        return payload