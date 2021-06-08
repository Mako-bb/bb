import time
import requests
from handle.replace         import _replace
from common import config
from handle.mongo import mongo
from time import sleep
import re
from datetime               import datetime
from updates.upload         import Upload
import pandas
import numpy

class Pluto_gg():
    """
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']

        self.api_url = self._config['api_url']
        self.api_series = self._config['api_series']
        self.session = requests.session()

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
            print('se ingresaron todos los contenidos correctamente')
            

        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self.request_series
            self._scraping(testing=True)
           
                    
    def _scraping(self, testing=False):
        """Método que realiza el scraping y controla los duplicados """
        payloads = []
        payload_episodes = []
        dict_metadata = {}
        for categorie in self._get_contents():
            items_list = categorie['items']
            for _item in items_list:
                    payload = self.get_payloads(_item)
                    if payload in payloads:
                        print('se pudrio')
                        continue
                    else:
                        payloads.append(payload)
                        self.mongo.insert(self.titanScraping, payload)
                        print(f"Se ingreso {payload} correctamente")
                        if _item['type'] != 'movie': 
                            episode =  self.get_payloadepisode(_item['slug'])
                            if episode in payload_episodes:
                                continue
                            else:
                                result = self.get_payloadepisode(_item['slug'])
                                if result == None:
                                    continue
                                payload_episodes.append(episode)
                                self.mongo.insert(self.titanScrapingEpisodios, episode)
                                print(f"Se ingreso {episode} correctamente")  
            

    def _get_contents(self):
        """Método que trae un diccionario de contenidos """
        list_categories = self.request().json()
        dict_contents = list_categories['categories']
        return dict_contents


    def request(self): 
       """metodo para hacer una peticion a un servidor"""

       uri = self.api_url
       response = self.session.get(uri)
       return response  

    def request_series(self, slug):
        """Método que hace requests y devielve una lista de temporadas
        Esta funcion rompe en una parte, casi al final de extraer todos los contenidos ###revisar """
        uri_series = self.api_series + slug
        response = self.session.get(uri_series)
        if response.status_code != 200:
            try:
                request_timeout = 5
                response = self.session.get(uri_series, timeout=request_timeout)
            except:
                return None

        dict_seasons = response.json()
        seasons = dict_seasons['seasons']
        return seasons            
            
    def get_payloadepisode(self, slug):
        """Método que trae payloads por episodio """
        season = self.request_series(slug)
        if season == None:
            return None
        for episodes in season['episodes']:
            payload_episode = {'Id': episodes['_id'],
                'Episode': episodes['name'],
                'season': episodes['season'],}
            return payload_episode




    def get_payloads(self, _item):
        """Metodo que genera payloads por contenido """ 
       
            
        payload = {}
        payload["Id"] = _item['_id']
        payload["Title"] = _item['name']
        payload["Type"] = _item['type']
        payload["Duración"] = self.get_duration(_item.get('duration'))
        payload['Synopsis'] = _item['description']
        payload["Year"] = _item.get('year')
        payload['Rating'] = _item['rating']
        payload["Género"] = _item['genre']
        payload['Directors'] = _item.get('directors')
        payload["CleanTitle"] = _replace(_item['name'])
        payload['Deeplinks'] = {
                    "Web": self.api_series + _item['slug'], 
                    "Android": None,
                    "iOS": None,
                                }
        payload['Timestamp']= datetime.now().isoformat()
        return payload
      

    
    def get_duration(self, payload):
        """Método que convierte la duración en horas(?)"""
        if payload == 0:
            return None
        else:
            return None
