import time
import requests
from requests import api 
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from pprint import pprint 
from bs4 import BeautifulSoup
from updates.upload         import Upload
from handle.datamanager import Datamanager
import json
from handle.payload import Payload

class Pluto_tomas():
    """    """    
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        #self._start_url = self._config['start_url']        
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.api_url = self._config['api_url']
        self.sesion = requests.session()
        if type == 'return':
            '''            Retorna a la Ultima Fecha            '''            
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

        # Listas de contentenido scrapeado:
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')

        # TODO: Aprender Datamanager
        # scraped = Datamanager._getListDB(self,self.titanScraping)
        # scraped_episodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        # Listas con contenidos y episodios dentro (DICT):
        self.payloads = []
        self.episodes_payloads = []

        contents_list = self.get_contents()
        for content in contents_list:
            # TODO: Agregar enumerate
            self.content_scraping(content)
            # Almaceno la lista de payloads en mongo:
        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodios, self.episodes_payloads)

        self.sesion.close()

        # Validar tipo de datos de mongo:
        Upload(self._platform_code, self._created_at, testing=True)

        print("Fin")

    def _parse_response(self, response_json):
        data_response = []

        list_categories = response_json['categories']
        for category in list_categories:
            titles_categories = category['name']
            items = category['items']
            for item in items:
                payload = self._get_payload(item)
  
                data_response.append(payload)
        return data_response

    def _get_payload(self, item):
        """[summary]

        Args:
            item ([type]): [description]

        Returns:
            [type]: [description]
        """

        title = item['name']
        description = item['description']
        slug = item['slug']
        _type = item['type']
        duration = item.get('duration')
        genre = item['genre']
        id_ = item['_id']

        deep_link = self._get_deep_link(_type, slug)

        payload ={
                'Title': title,
                'Description' : description,
                'Duration': duration,
                'ID': id_,
                'Genre': genre,
                'Deep Link': deep_link,
                'Type': _type, 
                #'Seasons': seasonsNumbers,
            }
        return payload

    def _get_deep_link(self, _type, slug ):

        if _type == 'series':
            deep_link = "https://pluto.tv/on-demand/{}/{}".format(_type, slug)
            #seasonsNumbers = categories.get('seasonsNumbers')
        else:
            deep_link = "https://pluto.tv/on-demand/{}s/{}".format(_type, slug)

        return deep_link
    
    def query_field(self, collection, field=None):
        """Método que devuelve una lista de una columna específica
        de la bbdd.

        Args:
            collection (str): Indica la colección de la bbdd.
            field (str, optional): Indica la columna, por ejemplo puede ser
            'Id' o 'CleanTitle. Defaults to None.

        Returns:
            list: Lista de los field encontrados.
        """
        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at
        }

        find_projection = {'_id': 0, field: 1, } if field else None

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection=find_projection,
            no_cursor_timeout=False
        )

        if field:
            query = [item[field] for item in query if item.get(field)]
        else:
            query = list(query)

        return query

    def _scraping(self, testing=False):
        # Listas de contentenido scrapeado:
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')

        # TODO: Aprender Datamanager
        # scraped = Datamanager._getListDB(self,self.titanScraping)
        # scraped_episodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        # Listas con contenidos y episodios dentro (DICT):
        self.payloads = []
        self.episodes_payloads = []

        contents_list = self.get_contents()
        for content in contents_list:
            # TODO: Agregar enumerate
            self.content_scraping(content)
            # Almaceno la lista de payloads en mongo:
        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodios, self.episodes_payloads)

        self.sesion.close()

        # Validar tipo de datos de mongo:
        Upload(self._platform_code, self._created_at, testing=True)

        print("Fin")

    def content_scraping(self, content):
        content_id = content['_id']

        if not content_id in self.scraped:
            payload = self.get_payload(content)
            if payload['Type'] == 'serie':
                self.get_episodes(content)
            if payload:
                # 1) Almaceno el dict en la lista.
                self.payloads.append(payload)
                # 2) Almaceno el id (str) en la lista.
                self.scraped.append(content_id)

    def get_episodes(self, content):
        # TODO: Pensar la lógca de episodios.
        self.scraped_episodes.append('Id')
        self.episodes_payloads.append({})

    def request(self, url, headers=None):
        """Método para hacer y validar una petición a un servidor.

        Args:
            url (str): Url a la cual realizaremos la petición.

        Returns:
            obj: Retorna un objeto tipo requests.
        """
        request_timeout = 5
        while True:
            try:
                # Request con header:
                response = self.sesion.get(
                    url,
                    headers=headers,
                    timeout=request_timeout
                )
                if response.status_code == 200:
                    return response
                else:
                    raise Exception(f"ERROR: {response.status_code}")
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(request_timeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...')
                time.sleep(request_timeout)
                continue

    def get_contents(self):
        """Método que trae los contenidos en forma de diccionario.

        Returns:
            list: Lista de diccionarios
        """
        content_list = []
        uri = self.api_url
        response = self.request(uri)
        dict_contents = response.json()
        list_categories = dict_contents['categories']
        for categories in list_categories:
            content_list += categories['items']

        return content_list

    def get_payload(self, dict_metadata):
        
        # payload = Payload()

        payload = {}
        payload['Id'] = dict_metadata['_id']
        payload['Title'] = dict_metadata['name']
        payload['Type'] = dict_metadata['type']
        payload['Synopsis'] = dict_metadata.get('description')
        #payload['Duration'] = self.get_duration(dict_metadata)

        return payload

    # def get_duration(self, dict_metadata):
    #     return int(dict_metadata['duration'] // 60000) or int(dict_metadata['allotment'])// 60
