from pprint import pp
import time
import requests
from requests.models import Response
#from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload         import Upload

# from time import sleep
# import re
 
class Pluto_mv():
    """
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config =                  config()['ott_sites'][ott_site_uid]
        self._platform_code =           self._config['countries'][ott_site_country]
        # self._start_url =             self._config['start_url']
        self._created_at =              time.strftime("%Y-%m-%d")
        self.mongo =                    mongo()
        self.titanPreScraping =         config()['mongo']['collections']['prescraping']
        self.titanScraping =            config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios =   config()['mongo']['collections']['episode']
 
        self.api_url =      self._config['api_url']
        self.api_serie =    self._config['api_serie']
        self.session =      requests.session()
        
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
   
    #####
    def query_field(self, collection, field=None):
        """Método que devuelve una lista de una columna específica
        de la bbdd.

        Args:
            collection (str): Indica la colección de la bbdd.
            field (str, optional): Indica la columna, por ejemplo puede ser
            'Id' o 'CleanTitle. Defaults to None.

        Returns:
            list: Lista de los field encontrados.
        """""" """
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

        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")
       
        contents = self.get_contents()
        for n, item in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n")
            if item['_id'] in self.scraped:
                # Que no avance, está repetido.
                continue                 
            self.scraped.append(item['_id'])
            if (item['type']) == 'movie':
                self.movie_payload(item)
            elif (item['type']) == 'series':
                self.serie_payload(item)
            break

    def request(self, url):
        response = self.session.get(url)
        if response.status_code == 200:
            return response


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

    def movie_payload(self, item):
        #deeplink = self.get_deeplink(item, 'movie')
        duration = self.get_duration(item)
        image = self.get_image(item, 'movie')
        print(item['name'])
        payload = { 
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": item['_id'], #Obligatorio
            "Title": item['name'], #Obligatorio 
            "CleanTitle": item['slug'], #Obligatorio 
            "OriginalTitle": item['name'], 
            "Type": item['type'], #Obligatorio 
            "Year": None, #Important! 
            "Duration": duration,
            "ExternalIds": 'falta',  #No estoy seguro de si es
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio 
            "Android": None, 
            "iOS": None, 
            }, 
            "Synopsis": item['summary'], 
            "Image": image,
            "Rating": item['rating'], #Important! 
            "Provider": None,
            "Genres": item['genre'], #Important!
            "Cast": None, 
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": 'Free', #Obligatorio 
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
            }
        self.mongo.insert(self.titanScraping, payload)
        
    def get_duration(self, item):
        duration = str(int((item['duration']) / 60000)) + ' min'
        return duration