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

class StarzFEV():
    """
    Analizamos los contenidos (series y peliculas) para Pluto en Argentina.

    DATOS IMPORTANTES:
    - VPN: No (Recomendación: Usar ExpressVPN).
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si.
    - ¿Usa BS4?: No.
    - ¿Cuanto demoró la ultima vez? tiempo + fecha.
    - ¿Cuanto contenidos trajo la ultima vez? cantidad + fecha.

    OTROS COMENTARIOS:
    Conseguimos la info de peliculas y series de una api general y la info sobre los episodios
     de cada serie a partir de una api por serie.
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        self.api_url             = self._config['api_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']

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
   
    def _scraping(self, testing=True):
        # Listas de contentenido scrapeado:
        self.payloads = []
        self.episodes_payloads = []
        # Comparando estas listas puedo ver si el elemento ya se encuentra scrapeado.
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")
        contents = self.get_contents()
        for n, item in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n")            
            if item['_id'] in self.scraped:
                # Que no avance, el _id está repetido.
                print(item['name'] + ' ya esta scrapeado!')
                continue
            else:   
                self.scraped.append(item['_id'])
                if (item['type']) == 'movie':
                    self.movie_payload(item)
                elif (item['type']) == 'series':
                    self.serie_payload(item)
        # Validar tipo de datos de mongo:
        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        else:
            print(f'\n---- Ninguna serie o pelicula para insertar a la base de datos ----\n')
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodios, self.episodes_payloads)
        else:
            print(f'\n---- Ningun episodio para insertar a la base de datos ----\n')
        #Upload(self._platform_code, self._created_at, testing=True)
        print("Scraping finalizado")
        self.session.close()

    def get_contents(self):
        """Metodo que hace reques a la api de StarZ y devuelve un diccionario con metadata en formato json"""
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