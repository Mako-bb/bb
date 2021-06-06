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
        self.season_url = self._config['season_api_url']

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
        seasons = self.get_seasons(item['_id'], item['slug'])
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
        #p(payload)
    
    def get_seasons(self, id, parentTitle):
        seasons = []
        uri = self.season_url + str(id) + '/seasons?deviceType=web' 
        items = self.request(uri)
        seasons = items['seasons']
        for season in seasons:
            for episode in season['episodes']:
                duration = self.get_duration(episode)
                deeplink = self.get_deeplink(episode, 'episode', season, parentTitle)
                print(deeplink)
                episode_payload = { 
                    "PlatformCode": self._platform_code, #Obligatorio 
                    "Id": episode['_id'], #Obligatorio
                    "ParentId": id, #Obligatorio #Unicamente en Episodios
                    "ParentTitle": parentTitle, #Unicamente en Episodios 
                    "Episode": episode['number'], #Obligatorio #Unicamente en Episodios 
                    "Season": episode['season'], #Obligatorio #Unicamente en Episodios
                    "Title": episode['name'], #Obligatorio 
                    "CleanTitle": episode['slug'], #Obligatorio 
                    "OriginalTitle": episode['slug'], 
                    "Type": episode['type'], #Obligatorio 
                    "Year": None, #Important! 
                    "Duration": duration,
                    "ExternalIds": "list",
                    "Deeplinks": { 
                    "Web": "str", #Obligatorio 
                    "Android": "str", 
                    "iOS": "str", 
                    }, 
                    "Synopsis": "str", 
                    "Image": "list", 
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
        
    def get_deeplink(self, item, type, season = None, parentTitle = None):
        if type == 'movie':
            deeplink = 'https://pluto.tv/on-demand/movies/' + item['slug']
        elif type == 'serie':
            deeplink = 'https://pluto.tv/on-demand/series/' + item['slug']
        elif type == 'episode':
            deeplink = 'https://pluto.tv/on-demand/series/' + parentTitle + '/episode/' + str(item['slug'])
        return deeplink
          
                       # deeplink = self.get_deeplink(episode, 'episode', season, parentTitle) 
    def request(self, uri):
        response = self.session.get(uri)
        contents = response.json()
        return contents