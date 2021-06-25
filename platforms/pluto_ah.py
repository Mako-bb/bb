import time
import requests
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.payload         import Payload
from handle.datamanager     import Datamanager
# from time import sleep
# import re

class PlutoAH():
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

        #self.api_url = self._config['api_url']

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
        #1) API
        # 2) bs4
        #3)selenium
        url = 'https://service-vod.clusters.pluto.tv/v3/vod/categories?includeItems=true&includeCategoryFields=imageFeatured%2CiconPng&itemOffset=10000&advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=5ba90432-9a1d-46d1-8f93-b54afe54cd1e&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=5ba90432-9a1d-46d1-8f93-b54afe54cd1e&deviceLat=-34.5106&deviceLon=-58.7536&deviceMake=Microsoft%2BEdge&deviceModel=web&deviceType=web&deviceVersion=91.0.864.54&marketingRegion=VE&serverSideAds=true'
        response = self.session.get(url)
        dictionary = response.json()
        items_list = []#Creo lista
        content_list = dictionary['categories']

        for categories in content_list:
            #append al array con todas las peliculas/Series de cada categoria
            items_list.append(categories['items'])
        
        #Filtra lo que necesito de cada pelicula/serie
        def payloads_films(films):
            payload = {
                "Id": films['_id'],
                "Title": films['name'],
                "Type": films['type'],
                "Genre": films['genre'],
                "Rating": films['rating']
            }

            return payload
        content_id=[]
        for items in items_list:
            for films in items:
                act=payloads_films(films)
                if not act['Id'] in content_id:
                    self.mongo.insert("titanScraping", act)
                    print("se inserto con exito")
                    content_id.append(act['Id'])
                else:
                    print("ya existe")
                