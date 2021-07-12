import json
import logging
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
from bs4                    import BeautifulSoup, element
# from time import sleep
# import re
from concurrent.futures     import ThreadPoolExecutor, as_completed
class VicePQ():
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
    
    # NO DIGAN COMO PROGRAMO :(
    def _scraping(self, testing=False):
        all_shows_id = []
        for i in range(1, 100):
            url = "https://video.vice.com/api/v1/shows?locale=en_us&page="+str(i)+"&per_page=50"
            res = self.request(url)
            res_json = res.json()
            if len(res_json) == 0:
                print(i)
                break

    def get_payload(self, content_dicc):
        """Método para crear el payload. Para titanScraping.

        Args:
            content_dicc (dicc): donde esta contenita la info que necesito.

        Returns:
            dict: Retorna el payload.
        """      
        payload = {}
        payload = { 
            "PlatformCode": self._platform_code,   #Obligatorio 
            "Id": content_dicc["id"],  #Obligatorio
            "Seasons": None, #Lo hago aparte
            "Crew": None,
            "Title": content_dicc["title"], #Obligatorio 
            "CleanTitle": _replace(content_dicc["title"]), #Obligatorio 
            "OriginalTitle": _replace(content_dicc["slug"]), 
            "Type": "serie", #Obligatorio #movie o serie 
            "Year": None, #Important! 1870 a año actual 
            "Duration": None, #en minutos 
            "ExternalIds": None, #consultar
            "Deeplinks": { 
                "Web": None, #Obligatorio 
                "Android": None, 
                "iOS": None, 
            }, 
            "Synopsis": None, 
            "Image": None, 
            "Rating": None, #Important!  "Provider": "list", 
            "Genres": None, #Important! 
            "Provider": None,
            "Cast": None, #Important! 
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": [{"Type":"subscription-vod"}], #Obligatorio 
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }

        return payload

    
    
    
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