import time
import requests
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
from datetime import datetime
from handle.payload import Payload
# from time import sleep
# import re

class PlutoCapacitacion():
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

        self.url = self._config['url']
        self.api_url = self._config['api_url']

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
        # Pensando algoritmo:
        # 1) Método request (request)-> Validar todo.
        # 2) Método payload (get_payload)-> Para reutilizarlo.
        # 3) Método para traer los contenidos (get_contents)

        # Listas de ids scrapeados:
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodes, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodes} {len(self.scraped_episodes)}")

        # Lista de contenidos a obtener (Almacenan dict):
        self.payloads = []
        self.episodes_payloads = []

        contents = self.get_contents()
        for n, content in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n")

            if content["_id"] in self.scraped:
                print("Ya ingresado")
            else:
                self.scraped.append(content["_id"])    
                payload = self.get_payload(content)
                self.payloads.append(payload)

            
            if payload["Type"] == 'serie':
                self.get_episodes(payload)

        self.mongo.insertMany(self.titanScraping, payloads)
        self.mongo.insertMany(self.titanScrapingEpisodes, epi_payloads)


    def get_contents(self):
        """Método para obtener contenidos en forma de dict,
        almancenados en una lista.

        Returns:
            list: Lista de contenidos.
        """
        print("\nObteniendo contenidos...\n")
        contents = [] # Contenidos a devolver.
        response = self.request(self.api_url)
        contents_metadata = response.json()        
        categories = contents_metadata["categories"]

        for categorie in categories:
            print(categorie.get("name"))
            contents += categorie["items"]
        return contents

    def request(self, url):
        '''
        Método para hacer una petición
        '''
        requestsTimeout = 5
        while True:
            try:
                response = self.session.get(url, timeout=requestsTimeout)
                return response
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(requestsTimeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...')
                time.sleep(requestsTimeout)
                continue

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
        payload['CleanTitle'] = None
        payload['Duration'] = None
        payload['Type'] = self.get_type(content_dict["type"]) 
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
        payload['Packages'] = None
        payload['Country'] = None
        payload['Crew'] = None        
        payload['Timestamp'] = datetime.now().isoformat()
        payload['CreatedAt'] = self._created_at

        print(f"Url: {payload['Deeplinks']['Web']}")
        print(f"{payload['Type']}:\t{payload['Title']}")

        return payload

    def get_payload_episodes(self, content_dict):
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
        payload['Duration'] = self.get_duration(content_dict)

        payload["ParentTitle"] = self.get_parent_title(content_dict)
        payload["ParentId"] = self.get_parent_id(content_dict)
        payload["Season"] = self.get_season(content_dict)
        payload["Episode"] = self.get_episode(content_dict)

        payload['Year'] = None
        payload['Deeplinks'] = self.get_deeplinks(content_dict)
        payload['Playback'] = None
        payload['Synopsis'] = self.get_synopsis(content_dict)
        payload['Image'] = self.get_image(content_dict)
        payload['Rating'] = self.get_ratings(content_dict)
        payload['Provider'] = None
        payload['Genres'] = None
        payload['Cast'] = self.get_cast(content_dict)
        payload['Directors'] = self.get_directors(content_dict)
        payload['Availability'] = None
        payload['Download'] = None
        payload['IsOriginal'] = None
        payload['IsAdult'] = None
        payload['Packages'] = self.get_packages(content_dict)
        payload['Country'] = None
        payload['Timestamp'] = datetime.now().isoformat()
        payload['CreatedAt'] = self.created_at

        print(f"Url: {payload['Deeplinks']['Web']}")
        print(f"{payload['Type']}:\t{payload['Title']}")
        
        return payload

    def get_deeplinks(self, metadata):
        deeplinks = {
                "Web": self.url + "prueba",
                "Android": None,
                "iOS": None,
            }
        return deeplinks

    def get_type(self, type_):
        if type_ == 'series': # Se puede solucionar con regex.
            return 'serie'
        else:
            return type_
    
    def get_episodes(self, content_metadata):
        pass