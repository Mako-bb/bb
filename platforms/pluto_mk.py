import time
from pymongo.message import insert
import requests
import ast
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload         import Upload
# from time import sleep
# import re

class Pluto_mk():
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
    
    def _scraping(self, testing=False):
        uri = self.api_url
        contents = self.request(uri)
        contents = contents['categories']
        for content in contents:
            for item in content['items']:
                    if (item['type']) == 'movie':
                        self.movie_payload(item)
                    elif (item['type']) == 'series':
                        self.serie_payload(item)
    
    def serie_payload(self, item):
        deeplink = self.get_deeplink(item, 'serie')
        image = self.get_image(item, 'serie')
        payload = {
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": item['_id'], #Obligatorio
            "Seasons": [ #Unicamente para series
            {
            "Id": "str", #Importante
            "Synopsis": "str", #Importante
            "Title": "str", #Importante, E.J. The Wallking Dead: Season 1
            "Deeplink": "str", #Importante
            "Number": "int", #Importante
            "Year": "int", #Importante
            "Image": "list", 
            "Directors": "list", #Importante
            "Cast": "list", #Importante
            "Episodes": "int", #Importante
            "IsOriginal": "bool" 
            },
            ],
            "Title": "str", #Obligatorio 
            "CleanTitle": "_replace(str)", #Obligatorio 
            "OriginalTitle": "str", 
            "Type": "str", #Obligatorio 
            "Year": "int", #Important! 
            "Duration": "int", 
            "ExternalIds": "list", 
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio 
            "Android": "str", 
            "iOS": "str", 
            }, 
            "Synopsis": "str", 
            "Image": image, 
            "Rating": "str", #Important! 
            "Provider": "list", 
            "Genres": "list", #Important!  "Cast": "list", 
            "Directors": "list", #Important! 
            "Availability": "str", #Important! 
            "Download": "bool", 
            "IsOriginal": "bool", #Important! 
            "IsAdult": "bool", #Important! 
            "IsBranded": "bool", #Important! (ver link explicativo)
            "Packages": "list", #Obligatorio 
            "Country": "list", 
            "Timestamp": "str", #Obligatorio 
            "CreatedAt": "str", #Obligatorio
        }
        print(payload)
        
    def movie_payload(self, item):
        deeplink = self.get_deeplink(item, 'movie')
        duration = self.get_duration(item)
        image = self.get_image(item, 'movie')
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
            "Timestamp": "falta", #Obligatorio 
            "CreatedAt": "falta", #Obligatorio
            }
        #self.mongo.insert(self.titanScraping, payload)
        
    def get_image(self, item, type):
        if type == 'movie':
            image = 'https://api.pluto.tv/v3/images/episodes/' + str(item['_id']) + '/poster.jpg'
        if type == 'serie':
            image = 'https://api.pluto.tv/v3/images/series/' + str(item['_id']) + '/poster.jpg'
        return image
    
    def get_duration(self, item):
        duration = str(int((item['duration']) / 60000)) + ' min'
        return duration
        
    def get_deeplink(self, item, type):
        if type == 'movie':
            deeplink = 'https://pluto.tv/on-demand/movies/' + item['slug']

        if type == 'serie':
            deeplink = 'https://pluto.tv/on-demand/series/' + item['slug']
        return deeplink
          
        
    def request(self, uri):
        response = self.session.get(uri)
        contents = response.json()
        return contents