import time
import requests
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload         import Upload
from handle.datamanager import Datamanager
# from time import sleep
# import re

class StarzCapacitacion():
    """
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = "ar.pluto"
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-07")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']

        # self.api_url = self._config['api_url']

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
        # Listas de contentenido scrapeado:
        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')

        # 1) Trae los payloads ingreados a mongo:
        scraped = Datamanager._getListDB(self,self.titanScraping)
        scraped_episodes = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        URL = "http://127.0.0.1:8000/personajes/"
        # 2) Trae un json
        # data = Datamanager._getJSON(self,URL)
        data = self.session.get(URL).json()

        payloads = []
        episodes = []

        id_ = '5e6a42c9e2fa10001becd458'
        # 3) Checkea si un id está en lista de scraped:
        if Datamanager._checkDBContentID(self,id_,scraped):
            print("está scrapeado")

        # 4) checkdbandappend validadba que un contenido no este
        # ingresado en la lista de payload, si no esta lo agrega, sino lo skipea:
        # Datamanager._checkDBandAppend(self,payloadEpi,listaEpiDB,listaEpi,isEpi=True)

        # 5) Insertar en mongo
        Datamanager._insertIntoDB(self,payloads,self.titanScraping)
        Datamanager._insertIntoDB(self,episodes,self.titanScrapingEpisodios)        