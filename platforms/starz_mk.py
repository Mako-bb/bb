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
            
        # Para empezar, utilizo el metodo query_field para validar si el elemento a scrapear 
        # esta o no ingresado ya en la base de datos
        
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

# Desde _scraping hago la request. Ademas voy a validar si no estan ingresados los datos,
# utilizando el metodo query_field, y luego de scrapear los datos, desde aca hago los inserts en la DB

    def _scraping(self, testing=False):
        self.payloads = []  
        data_dict = self.request(self.api_url)  
        contents = self.get_contents(data_dict)
        # Listas de contentenido scrapeado:
        # Comparando estas listas puedo ver si el elemento ya se encuentra scrapeado.
        self.scraped = self.query_field(self.titanScraping, field='Id')   #
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        for n, content in enumerate(contents):
            print(f"\n----- Progreso ({n + 1}/{len(contents)}) -----\n")  
            if content['contentId'] in self.scraped:
                print(content['title'] + ' ya esta scrapeado')
                continue
            else:
                #self.scraped.append(content['contentId'])
                if content['contentType'] == 'Movie':
                    self.movie_payload(content)
                elif content['contentType'] == 'Series with Season':
                    self.serie_payload(content)
                    
    # Payload para el tipo Movie. Recibe un diccionario con la metadata
    def serie_payload(self, content):  
        cleanTitle = self.clean_text(content['title']) 
        deeplink = self.get_deeplink(content, 'Serie', cleanTitle)
        genres = self.get_genres(content['genres'])
        cast = self.get_cast(content['credits'])
        directors = self.get_directors(content['directors'])
        seasons = self.get_seasons()
        serie_payload = {
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": content['contentId'], #Obligatorio
            "Seasons": 'falta',
            "Title": content['title'], #Obligatorio 
            "CleanTitle": _replace(content['title']), #Obligatorio 
            "OriginalTitle": content['title'], 
            "Type": 'serie', #Obligatorio 
            "Year": content['minReleaseYear'], #Important! 
            "ExternalIds": None, 
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio 
            "Android": None, 
            "iOS": None, 
            }, 
            "Synopsis": content['logLine'], 
            "Image": None, 
            "Rating": content['ratingCode'], #Important! 
            "Provider": None, 
            "Genres": genres, #Important!     
            "Cast": cast, 
            "Directors": directors, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": content['original'], #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": [{'Type':'subscription-vod',
                          'HD': content['HD']}],
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }
        self.payloads.append(serie_payload)            
        
    # Payload para el tipo Movie. Recibe un diccionario con la metadata
    def movie_payload(self, content):
        print('Movie: ' + content['title'])
        genres = self.get_genres(content['genres'])
        cast = self.get_cast(content['credits'])
        cleanText = self.clean_text(content['title'])
        deeplink = self.get_deeplink(content, 'Movie', cleanText)
        payload = { 
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": content['contentId'], #Obligatorio
            "Title": content['title'], #Obligatorio 
            "CleanTitle": cleanText,
            "OriginalTitle": content['title'], 
            "Type": content['contentType'], #Obligatorio 
            "Year": content['releaseYear'], #Important!
            "Duration": content['runtime'] / 60,
            "ExternalIds": None,  
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio 
            "Android": '', 
            "iOS": '', 
            }, 
            "Synopsis": content['logLine'], 
            "Image": None,
            "Rating": content['ratingCode'], #Important! 
            "Provider": None,
            "Genres": genres, #Important!
            "Cast": cast, 
            "Directors": '', #Important! 
            "Availability": '', #Important! 
            "Download": content['downloadable'], 
            "IsOriginal": content['original'], #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            # "Packages": "Free", #Obligatorio 
            "Packages": [{'Type':'subscription-vod',
                          'HD': content['HD']}], 
            "Country": '', 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
            }
        self.payloads.append(payload)
        
    # Metodo para obtener el deeplink, recibe un dict 'content'
    def get_deeplink(self, content, type, cleanText):
        if type == 'Movie':
            deeplink = 'https://www.starz.com/ar/es/movies/' + cleanText + '-' + str(content['contentId'])
        elif type == 'Serie':
            deeplink = 'https://www.starz.com/ar/es/series/' + cleanText + '/' + str(content['contentId'])
        print(deeplink)
        return deeplink

    # Metodo para crear un clean text
    def clean_text(self, text):         
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
    
    # Metodo para obtener una lista con los generos, que pueden ser uno o varios            
    def get_genres(self, genres):            
        genr = []
        for genre in genres:
            for description in genre.values():
                genr.append(description)
        return genr            
    
    # Metodo para obtener los miembros del cast
    def get_cast(self, cast):              
        actores = []
        for actor in cast:
            actor = actor['name']
            actores.append(actor)
        return actores
    
    # Metodo para obtener los directores, que pueden ser uno o varios
    def get_directors(self, directores):    
        direct = []
        for director in directores:
            director = director['fullName']
            direct.append(director)
        return direct
    
    # Con get_content 'limpio' la metadata para que me devuelva el dict en donde esta la 
    # informacion que necesito   
    def get_contents(self, contents):
        content = contents['playContentArray']['playContents']
        return content
       
    # Desde request hago la request a la api que le ingreso como parametro a la funcion    
    def request(self, uri):
        response = self.session.get(uri)
        contents = response.json()
        return contents
    
    