from os import replace
import time
import requests
from yaml.tokens import FlowMappingStartToken
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from handle.payload import Payload
from handle.datamanager import Datamanager
import datetime
# from time import sleep
import re
start_time = time.time()

class Iviru():
    def __init__(self, ott_site_uid, ott_site_country, type):
        """
        Starz es una ott de Estados Unidos que opera en todo el mundo.

        DATOS IMPORTANTES:
        - VPN: No
        - ¿Usa Selenium?: No.
        - ¿Tiene API?: Si.
        - ¿Usa BS4?: No.
        - ¿Cuanto demoró la ultima vez?. 0.7531681060791016 seconds
        - ¿Cuanto contenidos trajo la ultima vez? titanScraping: 184, titanScrapingEpisodes: 970, CreatedAt: 2021-07-05 .

        OTROS COMENTARIOS:
        ---
        """
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

        self.api_url = self._config['api_collections_url']
        self.url=self._config['url']

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

    def _scraping(self, testing = False):

        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodes, field='Id')
        self.payloads = []
        self.episodes_payloads = []
        self.collections_ids=[]
        self.contents_ids = []
        
        self.get_collections()
        self.get_contents()
        self.get_content_data()

        self.insert_payloads_close(self.payloads,self.episodes_payloads)
        print("--- %s seconds ---" % (time.time() - start_time))
    
    def get_collections(self):
        '''
            Obtenemos todos los ids correspondientes a cada categoria de contenidos, como esta dividida la pagina
        '''
        collections_api = self.api_url
        response = self.session.get(collections_api)
        json_data=response.json()
        for content in json_data:
            self.collections_ids.append(content['id'])

    def get_contents(self):
        '''
            Obtenemos los ids correspondientes a los contenidos por categoria
        '''
        for collection in self.collections_ids:
            collection_api = 'https://api.ivi.ru/mobileapi/collection/catalog/v5/?id={}&app_version=870'.format(str(collection))
            response = self.session.get(collection_api)
            json_data=response.json()
            for content in json_data:
                self.contents_ids.append(content['id'])

    def get_content_data(self):
        '''
            Obtenemos la data de cada contenido por su id y armamos el payload
        '''
        for id in self.contents_ids:
            content_api = 'https://api.ivi.ru/mobileapi/videoinfo/v6/?id={}'.format(str(id))
            response = self.session.get(content_api)
            json_data=response.json()
            #for content in json_data:
            #    self.get_payload(content)
    
    def isDuplicate(self, scraped_list, key_search):
        '''
            Metodo para validar elementos duplicados segun valor(key) pasado por parametro en una lista de scrapeados.
        '''
        isDup=False
        if key_search in scraped_list:
            isDup = True
        return isDup
    
    def insert_payloads_close(self,payloads,epi_payloads):
        '''
            El metodo checkea que las listas contengan elementos para ser subidos y corre el Upload en testing.
        '''     
        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
        if epi_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodes, epi_payloads)
        self.session.close()
        Upload(self._platform_code, self._created_at, testing=True)
