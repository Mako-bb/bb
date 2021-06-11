import time
import requests
import json
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from pprint import pprint 
from bs4 import BeautifulSoup
from updates.upload import Upload
from handle.datamanager import Datamanager
from handle.payload import Payload
from pymongo.message import insert
import ast
from datetime import datetime

class Pluto_tomas():
    """    """    
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
        self.season_url = self._config['season_api_url']
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

    def _scraping(self, testing=False):

        " Metodo de scrap que inserta datos validados a la BBDD "

        self.payloads = []
        self.episodes_payloads = []

        self.scraped = self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")
        
        contents = self._get_contents()

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

    def _get_contents(self):

        """ Metodo que realiza una request a la uri (API) y retorna la response parseada (JSON).
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

    def _movies_payload(self, item):
        """Metodo que ordena el payload de las movies"""
    
        deeplink = self._get_deep_link('movie', item['slug'], parentTitle= None)
        duration = self._get_duration(item)
        image = self._get_image('movie', item)
        print('Movie: ' + item['name'])

        payload = { 

            "PlatformCode": self._platform_code, 
            "Id": item['_id'], 
            "Title": item['name'],
            "CleanTitle": _replace(item['name']),  
            "OriginalTitle": item['name'], 
            "Type": item['type'],  
            "Year": None, 
            "Duration": duration,
            "ExternalIds": None,
            "Deeplinks": { 
            "Web": deeplink, 
            "Android": None, 
            "iOS": None,
        }, 
            "Synopsis": item['summary'], 
            "Image": [image],
            "Rating": item['rating'] if item['rating'] != 'Not Rated' else None, 
            "Provider": None,
            "Genres": [item['genre']], 
            "Cast": None, 
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
        self.payloads.append(payload)

    def _series_payload(self, item):
        """Metodo que ordena el payload de las series"""

        deeplink = self._get_deep_link(item['type'],item['slug'], parentTitle=None)
        image = self._get_image(item, 'serie')
        print('Serie: ' + item['name'])
        seasons = self.get_seasons(item['_id'], item['slug'])
        #year = self._get_year(item)

        serie_payload = {
            
            "PlatformCode": self._platform_code, 
            "Id": item['_id'], 
            "Seasons": seasons,
            "Title": item['name'], 
            "CleanTitle": _replace(item['name']), 
            "OriginalTitle": item['name'], 
            "Type": 'serie', 
            "Year": None,  
            "Duration": None, 
            "ExternalIds": None, 
            "Deeplinks": { 
            "Web": deeplink,  
            "Android": None, 
            "iOS": None, 
        }, 
           "Synopsis": item['description'], 
            "Image": [image], 
            "Rating": item['rating'] if item['rating'] != 'Not Rated' else None, 
            "Provider": None, 
            "Genres": [item['genre']], 
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

        self.payloads.append(serie_payload)

    def _get_deep_link(self, _type, slug, parentTitle):

        """Metodo para formatear el deeplink"""

        if _type == 'series':
            deep_link = "https://pluto.tv/on-demand/{}/{}".format(_type, slug)
        elif _type == 'movie' :
            deep_link = "https://pluto.tv/on-demand/{}s/{}".format(_type, slug)
        elif _type == 'episode':
            deep_link = "https://pluto.tv/on-demand/series/{}/episode/{}".format(parentTitle, slug )
        elif _type == 'season':
            deep_link = "https://pluto.tv/on-demand/series/{}/season/{}".format(parentTitle, slug)

        return deep_link

    def _get_duration(self, item):
        """Metodo que obtiene la duracion del contenido

        Args:
            item ([dict]): contenidos en formato json

        Returns:
            devuelve la duracion parseada (int) en minutos
        """

        try:
            duration = int((item['duration']) / 60000)
        except:
            duration = int((item['allotment']) / 60)

        return duration

    def get_seasons(self, id, parentTitle):
        """Metodo que ordena el payload de las seasons"""

        season_return = []

        uri = self.season_url + str(id) + '/seasons?deviceType=web' 
        items = self.request(uri).json()

        seasons = items['seasons']
        synopsis = items['description']
        name = items['name']
        self.totalSeasons = 0
        for season in seasons:
            self.totalSeasons += 1
            deeplink = self._get_deep_link('season', items['slug'], parentTitle)
            season_payload = {
                "Id": None, 
                "Synopsis": synopsis,
                "Title": name, 
                "Deeplink": deeplink,
                "Number": season['number'],
                "Year": None,
                "Image": None, 
                "Directors": None, 
                "Cast": None,
                "Episodes": len(season['episodes']), 
                "IsOriginal": None 
            },
            season_return.append(season_payload)
            self.episodios = 0
            for episode in season['episodes']:
                duration = self._get_duration(episode)
                deeplink = self._get_deep_link('episode', items['slug'], parentTitle)
                image = self._get_image(episode, 'episode')
                episode_payload = { 
                    "PlatformCode": self._platform_code, 
                    "Id": episode['_id'], 
                    "ParentId": id,
                    "ParentTitle": parentTitle, 
                    "Episode": episode['number'] if episode['number'] != 0 else None, 
                    "Season": episode['season'], 
                    "Title": episode['name'], 
                    "OriginalTitle": episode['name'],  
                    "Year": None, 
                    "Duration": duration,
                    "Deeplinks": { 
                    "Web": deeplink, 
                    "Android": None, 
                    "iOS": None, 
                    }, 
                    "Synopsis": episode['description'], 
                    "Image": [image], 
                    "Rating": episode['rating'] if episode['rating'] != 'Not Rated' else None, 
                    "Provider": None, 
                    "Genres": [episode['genre']], 
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
                self.episodes_payloads.append(episode_payload)
                self.episodios += 1
        ('Temporadas: ' + str(self.totalSeasons))
        print('Episodios: ' + str(self.episodios))
        return season_return


    
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

    def request(self, url, headers=None):
        """Método para hacer y validar una petición a un servidor.

        Args:
            url (str): Url a la cual realizaremos la petición.

        Returns:
            obj: Retorna un objeto tipo requests.
        """
        request_timeout = 5
        while True:
            try:
                # Request con header:
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=request_timeout
                )
                if response.status_code == 200:
                    return response
                else:
                    raise Exception(f"ERROR: {response.status_code}")
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(request_timeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...')
                time.sleep(request_timeout)
                continue

    def _get_image(self, item, _type):
        image = ''
        if _type == 'movie':
            image = 'https://api.pluto.tv/v3/images/episodes/{}/poster.jpg'.format(str(item['_id']))
        elif _type == 'serie':
            image = 'https://api.pluto.tv/v3/images/series/{}/poster.jpg'.format(str(item['_id']))
        elif _type == 'episode':
            image = 'https://api.pluto.tv/v3/images/episodes/{}/poster.jpg'.format(str(item['_id']))

        return image

