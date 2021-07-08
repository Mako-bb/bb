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
class HBOPQ():
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

        self.all_movies_url = self._config['all_movies']
        self.info_movie_url = self._config['info_movie']

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
        
        #res = self.request("https://proxy-v4.cms.hbo.com/v1/schedule/")
      
        response = self.request(self.all_movies_url)
        soup = BeautifulSoup(response.content, "html.parser")
        all_movies_titles = soup.find_all("p", class_="modules/cards/CatalogCard--title")
        print("Traje "+str(len(all_movies_titles))+" peliculas")
        ini = time.time()
        self.get_info_movie(all_movies_titles)
        fin = time.time()
        print(fin - ini)
        self.session.close()
   
    def get_info_movie(self, list_movies_titles):
        processes = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            i = 0
            for title in list_movies_titles:
                processes.append(executor.submit(self.get_payload, title))
                if i == 100:
                    break
                i += 1            

    def get_payload(self, title):
        title_ok = self.clean_title(title)
        url = self.info_movie_url+title_ok
        res = self.request(url)
        if res.status_code != 404:
            try:
                soup = BeautifulSoup(res.content, "html.parser")
                noScript = soup.find("noscript", id="react-data")
                jjson = json.loads(noScript["data-state"])
                print(jjson["bands"][1]["data"]["infoSlice"]["streamingId"]["id"])
                
            except:
                pass
        else:
            print("ERROR")

    def clean_title(self, title):
       #Método para acomodar el title, sacando / o * y agregandole los - en cada espacio
       valid_chars = [" ", "&","/"]
       new_string = ''.join(char for char in title if char.isalnum() or char in valid_chars)
       new_string = new_string.lower()
       new_string = new_string.replace("  ", " ")
       new_string = new_string.replace("/","-").replace("&","and").replace(" ", "-")
       return new_string
    
    
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