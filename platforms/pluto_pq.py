from logging import getLogRecordFactory
import threading
import time
import requests
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
from datetime               import datetime
# from time import sleep
# import re


class PlutoPQ():
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
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']

        self.all_titles_url = self._config['all_titles_url']
        self.all_episodes_url = self._config['all_episodes_url']
        self.all_episodes_url2 = self._config['all_episodes_url2']

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
        
        self.payloads = []
        self.episodes_payloads = []        
        
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodes, field='Id')
        
        all_titles = self.get_contents()
        for title in all_titles:
            if title["_id"] in self.scraped:
                pass
            else:
                self.scraped.append(title["_id"])    
                payload = self.get_payload(title)
                self.payloads.append(payload)

                if title["type"] == "series":
                    self.episodes_payloads = self.get_episodes(title)

     
        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, self.episodes_payloads)
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)


    def get_episodes(self, content):
        episode_payload = []
        url = self.all_episodes_url2.format(content['_id'])
        res_episodes = self.request(url)
        scraped_episodes = []
        try:
            for season in res_episodes["seasons"]:
                for episode in season["episodes"]:
                    if episode["_id"] in scraped_episodes:
                        pass
                    else:
                        self.scraped_episodes.append(episode["_id"])    
                        episode_payload = self.get_payload_episodes(episode, res_episodes)
                        self.episodes_payloads.append(episode_payload)
                        print("agregado")
            return self.episodes_payloads
        except:
            print(content["name"])
        
    
    def request(self, url):
            '''
            Método para hacer una petición
            '''
            requestsTimeout = 5
            while True:
                try:
                    response = self.session.get(url, timeout=requestsTimeout)
                    return response.json()
                except requests.exceptions.ConnectionError:
                    print("Connection Error, Retrying")
                    time.sleep(requestsTimeout)
                    continue
                except requests.exceptions.RequestException:
                    print('Waiting...')
                    time.sleep(requestsTimeout)
                    continue

    def get_contents(self):
            """Método para obtener contenidos en forma de dict,
            almancenados en una lista.

            Returns:
                list: Lista de contenidos.
            """
            print("\nObteniendo contenidos...\n")
            contents = [] # Contenidos a devolver.
            contents_metadata = self.request(self.all_titles_url)
            categories = contents_metadata["categories"]

            for categorie in categories:
                contents += categorie["items"]
            return contents

    def get_payload(self, content_dict):
            """Método para crear el payload. Para titanScraping.

            Args:
                content_metadata (dict): Indica la metadata del contenido.

            Returns:
                dict: Retorna el payload.
            """
            payload = {}

            # Indica si el payload a completar es un episodio:        
            payload['PlatformCode'] = self._platform_code
            payload['Id'] = content_dict["_id"]
            payload['Title'] = content_dict["name"]
            payload['OriginalTitle'] = None
            payload['CleanTitle'] = _replace(content_dict["name"])
            payload['Duration'] = None
            payload['Type'] = "serie" if content_dict["type"] == "series" else "movie" 
            payload['Year'] = None
            payload['Deeplinks'] = self.get_deeplinks(content_dict)
            payload['Playback'] = None
            payload['Synopsis'] = None
            payload['Image'] = None
            payload['Rating'] = None
            payload['Provider'] = None
            payload['Genres'] = None
            payload['Cast'] = None
            payload['Directors'] = None
            payload['Availability'] = None
            payload['Download'] = None
            payload['IsOriginal'] = None
            payload['Seasons'] = None
            payload['IsBranded'] = None
            payload['IsAdult'] = None
            payload['Packages'] = [{"Type":"free-vod"}]
            payload['Country'] = None
            payload['Crew'] = None        
            payload['Timestamp'] = datetime.now().isoformat()
            payload['CreatedAt'] = self._created_at
           
            return payload
    
    def get_deeplinks(self, metadata):
        deeplinks = {
                "Web": self.all_titles_url + "prueba",
                "Android": None,
                "iOS": None,
            }
        return deeplinks
    
    def get_payload_episodes(self, content_dict, content_perent):
        """Método para crear el payload. Para titanScrapingEpisodes.

        Args:
            content_metadata (dict): Indica la metadata del contenido.

        Returns:
            dict: Retorna el payload.
        """
        payload = {}

        # Indica si el payload a completar es un episodio:        
        payload['PlatformCode'] = self._platform_code
        payload['Id'] = content_dict["_id"]
        payload['Title'] = content_dict["name"]
        payload['Duration'] = content_dict["duration"]
        payload["ParentTitle"] = content_perent["name"]
        payload["ParentId"] = content_perent["_id"]
        payload["Season"] = content_dict["season"]
        payload["Episode"] = content_dict["number"]
        payload['Year'] = None
        payload['Deeplinks'] = self.get_deeplinks(content_dict)
        payload['Playback'] = None
        payload['Synopsis'] = content_dict["description"]
        payload['Image'] = None
        payload['Rating'] = content_dict["rating"]
        payload['Provider'] = None
        payload['Genres'] = None
        payload['Cast'] = None
        payload['Directors'] = None
        payload['Availability'] = None
        payload['Download'] = None
        payload['IsOriginal'] = None
        payload['IsAdult'] = None
        payload['Packages'] = [{"Type":"free-vod"}]
        payload['Country'] = None
        payload['Timestamp'] = datetime.now().isoformat()
        payload['CreatedAt'] = self._created_at
        return payload