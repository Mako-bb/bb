from os import name
import requests
import time 
import requests
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from pprint import pprint 
from bs4 import BeautifulSoup
import json
from updates.upload         import Upload
from datetime import datetime


class Starz():
    """clase para la plataforma Starz
    """

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
        self.session = requests.session()
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

    
    def _scraping(self, testing = False ):
        """metodo para scrapear la plataforma

        Args:
            self, testing o scraping 
        """
        " Metodo de scrap que inserta datos validados a la BBDD "

        self.payloads = []
        self.episodes_payloads = []

        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")
        
        contents = self._request_parser()

        for n, item in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n")            
            if item['_id'] in self.scraped:
                print(item['name'] + ' ya esta scrapeado!')
                continue
            else:
                self.scraped.append(item['_id'])
                if (item['type']) == 'movie':
                    self._movies_payload(item)
                elif (item['type']) == 'series':
                    self._series_payload(item)

        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        else:
            print(f'\n---- Ninguna serie o pelicula para insertar a la base de datos ----\n')
        if self.episodes_payloads:
            self.mongo.insertMany(self.titanScrapingEpisodios, self.episodes_payloads)
        else:
            print(f'\n---- Ningun episodio para insertar a la base de datos ----\n')

        Upload(self._platform_code, self._created_at, testing=True)

        print("Scraping finalizado")

        self.session.close()
        
    def _request_parser(self):
        """recibe una uri de la API y devuelve la response parseada (JSON)

        Args:
            uri : uri de la API

        Returns:
            content_dict[dict]: devuelve la response parseada (JSON)
        """
        uri = self.api_url
        response = self.session.get(uri)

        if (response.status_code == 200): 
            dict_content = response.json()
        else:
            print('SERVER ERROR' + response.status_code)
        return dict_content


    def _movies_payload(self, items):
        
        items_list = items['playContentArray']['playContents']
        payload_movies = []
   
        for item in items_list:

            duration = int(item['runtime'] / 60)

            payload = { 

                "PlatformCode": self._platform_code, 
                "Id": item['contentId'], 
                "Title": item['title'],
                "CleanTitle": _replace(item['title']),  
                "OriginalTitle": item['titleSort'], 
                "Type": item['contentType'],  
                "Year":item['releaseYear'], 
                "Duration": duration if duration != 0 else None,
                "ExternalIds": item['mediaId'],
                "Deeplinks": { 
                "Web": self._get_deeplink(item), 
                "Android": None, 
                "iOS": None,
            }, 
                "Synopsis": item['logLine'], 
                "Image": None,
                "Rating": item['ratingCode'], 
                "Provider":item['studio'],
                "Genres": [item['genres']], 
                "Crew": self._get_crew(item),
                "Cast": self._get_cast(item), 
                "Directors": None, 
                "Availability": None, 
                "Download": None, 
                "IsOriginal": None, 
                "IsAdult": None, 
                "IsBranded": None, 
                "Packages": [{'Type':'free-vod'}],
                "Country": None, 
                "Timestamp": datetime.now().isoformat(), 
                "CreatedAt": self._created_at, 
            }

    def _get_deeplink(self, item):

        title_deep = item['title'].lower()
        title_deep = item['title'].replace(':', '')
        title_deep = item['title'].replace(' ', '-') 
        title_deep = item['title'].replace('¡', '') 
        title_deep = item['title'].replace('!', '')
        title_deep = item['title'].replace('ó', 'o')
        title_deep = item['title'].replace('á', 'a')
        title_deep = item['title'].replace('é', 'e')
        title_deep = item['title'].replace('í', 'i')
        title_deep = item['title'].replace('ú', 'u')      
        title_deep = item['title'].replace("'", '') 

        if item['contentType'] == 'Series with Season':
            deep_link = "https://starz.com.ar/ar/es/series/{}/{}".format(title_deep, item['contentId'])
        if item['contentType'] == 'Movie' :
            deep_link = "https://pluto.tv/on-demand/{}s/{}".format(title_deep, item['contentId'])
        # elif _type == 'episode':
        #     deep_link = "https://pluto.tv/on-demand/series/{}/episode/{}".format(parentTitle, slug )
        # elif _type == 'season':
        #     deep_link = "https://pluto.tv/on-demand/series/{}/season/{}".format(parentTitle, slug)
        
        return deep_link

    
    def _payloads(self, items):
        
        items_list = items['playContentArray']['playContents']
        payload_list = []
        for item_list in items_list:
            payload = {}
            payload['Title'] = item_list['title']
            payload['ID'] = item_list['contentId']
            payload['Type'] = item_list['contentType']
            payload['Credits'] = self._get_crew(item_list)
            payload['Log Line'] = item_list['logLine']

            if payload['Type'] == "Series with Season" :
                payload['isSerie'] = self._get_data_serie(item_list['childContent'])
                payload['isSerie'] = item_list['episodeCount']
            
            try:
                payload['Actors'] = self._get_cast(item_list['actors'])
            except KeyError: 
                pass
            payload_list.append(payload)
        return payload_list
         
            
    def _get_cast (self, items):
        payload = {}
        for item in items:
            payload = {
            'Name' : item['fullName'],
            'ID' : item['id']
            }
        return payload 

    def _get_crew(self, credits):
        
        credits = credits['credits']
        payload = [{}]
        for credit in credits:  
            payload['Name'] = credit['name']
            payload['Rol'] = credit['keyedRoles']
            payload['ID'] = credit['id']               

        return payload 

    def _get_data_serie(self, items):

        for item in items:
            payload = {
                'Season Title' : item['childContent'][0]['title'],
                'ID' : item['childContent'][0]['contentId'],
                'Log Line' : item['logLine'],
                'Episodes' :self._get_episodes(item['childContent']),
                'Credits' : self._get_crew(item),
                'Genres' : item['childContent'][0]['genres'],
            }
        return payload 
    
    def _get_episodes(self, items):

        for item in items:
            payload = {
                "Title" : item['title'],
                'ID' : item['contentId'],
                'Logline' : item['logLine'],
                'isHD' : item['HD'],
                #'Audio Type' : item['audioType'],
                #'Downloadable' : item['downloadable']
            }
        return payload

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



        
        #         payload = { 

        #     "PlatformCode": self._platform_code, 
        #     "Id": item['_id'], 
        #     "Title": item['name'],
        #     "CleanTitle": _replace(item['name']),  
        #     "OriginalTitle": item['name'], 
        #     "Type": item['type'],  
        #     "Year": None, 
        #     "Duration": duration,
        #     "ExternalIds": None,
        #     "Deeplinks": { 
        #     "Web": deeplink, 
        #     "Android": None, 
        #     "iOS": None,
        # }, 
        #     "Synopsis": item['summary'], 
        #     "Image": [image],
        #     "Rating": item['rating'] if item['rating'] != 'Not Rated' else None, 
        #     "Provider": None,
        #     "Genres": [item['genre']], 
        #     "Cast": None, 
        #     "Directors": None, 
        #     "Availability": None, 
        #     "Download": None, 
        #     "IsOriginal": None, 
        #     "IsAdult": None, 
        #     "IsBranded": None, 
        #     "Packages": [{'Type':'free-vod'}],
        #     "Country": None, 
        #     "Timestamp": datetime.now().isoformat(), 
        #     "CreatedAt": self._created_at, 
        # }