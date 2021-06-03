from pprint import pp
import time
import requests
#from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload         import Upload

# from time import sleep
# import re
 
class Pluto():
    """
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
 
        self.api_url = self._config['api_url']
        self.api_serie = self._config['api_serie']
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
 
        if type == 'scraping':
            self._scraping()
 
        if type == 'testing':
            self._scraping(testing=True)
    
    def _scraping(self, testing=False):
        print("hoal")
        payloads = []
        episodes = []
        
        contents_list = self.get_contents()
        for content in contents_list:
            payload = self.get_payload(content)
            payloads.append(payload)

            if payload['Type'] == 'serie':
                episodes.append(self.get_episodes(content['slug']))
                # ¿Puedo reutilizar get_payload?
                      
        
        self.mongo.insertMany(self.titanScraping, payloads)
        self.mongo.insertMany(self.titanScrapingEpisodios, episodes)
        
       
    def request(self, url):
        """Método para hacer y validar una petición a un servidor.

        Args:
            url (str): Url a la cual realizaremos la petición.

        Returns:
            obj: Retorna un objeto tipo requests.
        """
        response = self.session.get(url)
        if response.status_code == 200:
            return response

    def get_contents(self):
        """Método que trae los contenidos en forma de diccionario.

        Returns:
            list: Lista de diccionarios
        """
        uri = self.api_url
        response = self.request(uri)
        dict_contents = response.json()
        list_categories = dict_contents['categories']
        for categories in list_categories:
            return categories['items']

    def get_payload(self, dict_metadata):
        payload = {}
        payload['Id'] = dict_metadata['_id']
        payload['Title'] = dict_metadata['name']
        payload['Type'] = dict_metadata['type']
        payload['Synopsis'] = dict_metadata.get('description')
        payload['Duration'] = self.get_duration(dict_metadata)

        return payload

    def get_duration(self, dict_metadata):
        return int(dict_metadata['allotment'] // 60) or int(dict_metadata['duration'] // 60000)
 
    def get_episodes(self, slug):
        """Metodo que trae los episodios en form de diccionario

        Returns:
            list: lista de episodios.
        """
        url_serie = self.api_serie + slug
        response = self.request(url_serie)
        dict_serie = response.json()
        list_seassons = dict_serie['seasons']
        for episodes in list_seassons:
            episode = self.get_payload(episodes)
            episodes.append(episode)
