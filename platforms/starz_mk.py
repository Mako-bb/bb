import time
from pymongo.message import insert
import requests
import ast
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload         import Upload
from datetime import datetime
# from time import sleep
# import re

class Starz_mk():
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
        self.payloads = []
        self.episode_payloads = []
        # Listas de contentenido scrapeado:
        # Comparando estas listas puedo ver si el elemento ya se encuentra scrapeado.
        self.scraped = self.query_field(self.titanScraping, field='Id')   #
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")
        elements = self.get_content(self.api_url)
        self.payloads(elements)        

    def payloads(self, elements):
        elements_list = elements['playContentArray']['playContents']
        for element in elements_list:
            if element['contentType'] == 'Movie':
                self.movie_payload(element)
            elif element['contentType'] == 'Series with Season':
                self.serie_payload(element)
                
    # Payloads:
    def movie_payload(self, element):               # Payloads para movies
        payloads = []
        cleanTitle = self.clean_text(element['title'])
        deeplink = self.movie_deeplink(cleanTitle, element['contentId'])
        duration = str(int((element['runtime'] / 60))) + ' min'
        directors = self.get_directors(element['directors'])
        try:
            genre = self.get_genres(element['genres'])
        except:
            genre = None
        try:
            download = element['downloadable']
        except:
            download = False
        cast = self.get_cast(element['credits'])
        payload = { 
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": element['contentId'], #Obligatorio
            "Title": element['title'], #Obligatorio 
            "CleanTitle": cleanTitle, #Obligatorio 
            "OriginalTitle": element['title'], 
            "Type": element['contentType'], #Obligatorio 
            "Year": element['releaseYear'], #Important! 
            "Duration": duration,
            "ExternalIds": element['contentId'],  #No estoy seguro de si es
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio 
            "Android": None, 
            "iOS": None, 
            }, 
            "Synopsis": element['logLine'], 
            "Image": None,
            "Rating": element.get('ratingCode'), #Important! 
            "Provider": element['studio'],
            "Genres": genre, #Important!
            "Cast": cast, 
            "Directors": directors, #Important! 
            "Availability": None, #Important! 
            "Download": download, 
            "IsOriginal": element['original'], #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": "Subscripcion", #Obligatorio 
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
            }
        payloads.append(payload)
        self.mongo.insertMany(self.titanScraping, payloads)

    def serie_payload(self, element):               # Payloads para SERIES
        payloads = []
        cleanTitle = self.clean_text(element['title'])
        deeplink = self.serie_deeplink(cleanTitle, element['contentId'])
        try:
            directors = self.get_directors(element['directors'])
        except:
            directors = None
        try:
            genre = self.get_genres(element['genres'])
        except:
            genre = None
        try:
            download = element['downloadable']
        except:
            download = False
        cast = self.get_cast(element['credits'])
        seasons = self.get_seasons(element['childContent'], cleanTitle, directors, cast, genre, element['contentId'])
        payload = { 
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": element['contentId'], #Obligatorio
            "Seasons": seasons,
            "Title": element['title'], #Obligatorio 
            "CleanTitle": cleanTitle, #Obligatorio 
            "OriginalTitle": element['title'], 
            "Type": element['contentType'], #Obligatorio 
            "Year": element['minReleaseYear'], #Important! 
            "Duration": None, 
            "ExternalIds": element['contentId'], 
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio 
            "Android": "str", 
            "iOS": "str", 
            }, 
            "Synopsis": element['logLine'], 
            "Image": None,
            "Rating": element.get('ratingCode'), #Important! 
            "Provider": element['studio'],
            "Genres": genre, #Important!
            "Cast": cast, 
            "Directors": directors, #Important! 
            "Availability": None, #Important! 
            "Download": download, 
            "IsOriginal": element['original'], #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": "Subscripcion", #Obligatorio 
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
            }
        payloads.append(payload)
        self.mongo.insertMany(self.titanScraping, payloads)


    def get_seasons(self, element, cleanTitle, directors, cast, genre, id):  # Funcion para obtener todas las temporadas de una serie
        seasons = []
        for serie in element:
            deeplink = self.season_deeplink(serie, cleanTitle)
            season = {
                "Id": serie['contentId'], #Importante
                "Synopsis": serie['logLine'], #Importante
                "Title": serie['title'], #Importante
                "Deeplink": deeplink, #Importante
                "Number": serie['order'], #Importante
                "Year": serie['minReleaseYear'], #Importante
                "Image": None, 
                "Directors": directors, #Importante
                "Cast": cast, #Importante
                "Episodes": serie['episodeCount'], #Importante
                "IsOriginal": serie['original']
                }
            self.get_episodes(serie, cleanTitle, directors, cast, genre, serie['order'], id)
            seasons.append(season)
        return seasons

    def get_episodes(self, serie, cleanTitle, directors, cast, genre, season, id):    # Funcion para obtener todos los episodios de una temporada
        payloads = []
        childContent = serie.get('childContent')
        for element in childContent:
            episodeNumber = (element['order'] - season * 100)
            download = element.get('downloadable')
            duration = str(int((element['runtime'] / 60))) + ' min'
            deeplink = self.episode_deeplink(element, cleanTitle, season, element['contentId'])
            payload = { 
                "PlatformCode": self._platform_code, #Obligatorio 
                "Id": element['contentId'], #Obligatorio
                "ParentId": id, #Obligatorio #Unicamente en Episodios
                "ParentTitle": "str", #Unicamente en Episodios 
                "Episode": episodeNumber, #Obligatorio #Unicamente en Episodios 
                "Season": season, #Obligatorio #Unicamente en Episodios
                "Title": element['title'], #Obligatorio 
                "CleanTitle": cleanTitle, #Obligatorio 
                "OriginalTitle": element['title'], 
                "Type": element['contentType'], #Obligatorio 
                "Year": element['releaseYear'], #Important! 
                "Duration": duration, 
                "ExternalIds": element['contentId'], 
                "Deeplinks": { 
                "Web": deeplink, #Obligatorio 
                "Android": "str", 
                "iOS": "str", 
                }, 
                "Synopsis": element['logLine'], 
                "Image": None,
                "Rating": element.get('ratingCode'), #Important! 
                "Provider": element['studio'],
                "Genres": genre, #Important!
                "Cast": cast, 
                "Directors": directors, #Important! 
                "Availability": None, #Important! 
                "Download": download, 
                "IsOriginal": element['original'], #Important! 
                "IsAdult": None, #Important! 
                "IsBranded": None, #Important! (ver link explicativo)
                "Packages": "Subscripcion", #Obligatorio 
                "Country": None, 
                "Timestamp": datetime.now().isoformat(), #Obligatorio 
                "CreatedAt": self._created_at, #Obligatorio
                }
            payloads.append(payload)
        self.mongo.insertMany(self.titanScrapingEpisodios, payloads)
    
    # Directores, cast y genres:
    def get_cast(self, cast):              # Funcion para obtener los miembros del cast
        actores = []
        for actor in cast:
            actor = actor['name']
            actores.append(actor)
        return actores

    def get_directors(self, directores):    # Funcion para obtener los directores, que pueden ser uno o varios
        direct = []
        for director in directores:
            director = director['fullName']
            direct.append(director)
        return direct
    
    def get_genres(self, genres):            # Funcion para obtener los generos, que pueden ser uno o varios
        genr = []
        for genre in genres:
            for description in genre.values():
                genr.append(description)
        return genr

    # Deeplinks:
    def movie_deeplink(self, title, id):       # Funcion para generar del Deeplink de las movies
        deeplink = 'https://www.starz.com/ar/es/movies/' + title + '-' + str(id)
        return deeplink

    def serie_deeplink(self, title, id):        # Funcion para generar del Deeplink de las series
        deeplink = 'https://www.starz.com/ar/es/series/' + title + '/' + str(id)
        return deeplink     

    def season_deeplink(self, serie, cleanTitle):     # Funcion para general el Deeplink de las temporadas
        deeplink = 'https://www.starz.com/ar/es/series/' + cleanTitle + '/season-' + str(serie['order']) + '/' + str(serie['contentId'])
        return deeplink

    def episode_deeplink(self, element, cleanTitle, season, id): # Funcion para generar el Deeplink de las series
        episodeNumber = (element['order'] - season * 100)
        deeplink = ('https://www.starz.com/ar/es/series/' + cleanTitle + '/season-' + str(season) + '/episode-' 
        + str(episodeNumber) + '/' + str(id))
        return deeplink

    # Utilidades generales:
    def clean_text(self, text):         # Funcion para pasar un texto al formato utilizado en las requests
        cleanText = text.lower()
        cleanText = cleanText.replace(':', '')
        cleanText = cleanText.replace(' ', '-') 
        cleanText = cleanText.replace('¡', '') 
        cleanText = cleanText.replace('!', '')
        cleanText = cleanText.replace('ó', 'o')
        cleanText = cleanText.replace('á', 'a')
        cleanText = cleanText.replace('é', 'e')
        cleanText = cleanText.replace('í', 'i')
        cleanText = cleanText.replace('ú', 'u')      
        cleanText = cleanText.replace("'", '') 
        return cleanText

    def get_content(self, uri):
        dict_content = self.request(uri)
        return dict_content
        
    def request(self, uri):
        print(uri)
        response = self.session.get(uri)
        print(response)
        if (response.status_code == 200): 
            dict_content = response.json()
            return dict_content