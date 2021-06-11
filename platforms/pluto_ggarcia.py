from handle.payload import Payload
import time
import requests
from handle.replace         import _replace
from common import config
from handle.mongo import mongo
from time import sleep
import re
from datetime               import datetime
from updates.upload         import Upload
import pandas
import numpy

class Pluto_gg():
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
        self.api_series = self._config['api_series']
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
            print('se ingresaron todos los contenidos correctamente')
            

        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self.request_series
            self._scraping(testing=True)
           
                    
    def _scraping(self, testing=False):
        """Método que realiza el scraping y controla los duplicados """
        self.movies = []
        self.series = []
        self.payloads = []
        self.payload_episodes = []
        self.scraped = []
        for categorie in self._get_categories():#
            items_list = categorie['items']
            for _item in items_list:
                if _item['type'] == 'movie':
                    payload = self.get_payloads(_item)
                    if payload in self.payloads:
                        print('-------------------------' + payload['Title'] + 'ya esta scrapeado')
                    else:
                        self.payloads.append(payload)
                        self.movies.append(payload)
                    #self.scraped.append(_item)
                    #self.mongo.insert(self.titanScraping, payload)
                        print(f"Se scrapeó {payload['Title']} correctamente")
                        print('Cantidad de ' + payload['Type'] +  ' scrapeadas')
                        print(len(self.movies))
                        print('---------------------------')   
                elif _item['type'] == 'series':
                    serie_payload = self.get_serie_payload(_item)
                    if serie_payload != None:
                        if serie_payload in self.payloads:
                            print('Esta serie ya fue scrapeadad')
                        else:    
                            #self.series.append(serie_payload)
                            self.payloads.append(serie_payload)
                            self.series.append(serie_payload) 
                            print(f"Se ingreso {serie_payload['Title']} correctamente")
                            print(len(self.series))
                            #print(serie_payload['Title'] + 'fue scrapeada correctamente')
                            #print('---------------------------')
                    else:
                        print('Error = 404')
                        continue 
        print('------------------------')
        print('Total de peliculas: ')
        print( len(self.movies))
        print('------------------------')
        print('Total de series: ')
        print(len(self.series))
        print('------------------------')       
        if self.payloads:
            for payload in self.payloads:
                self.mongo.insert(self.titanScraping, payload)
        else: 
            print('------------------Nada mas para ingresar-----------------')
        if self.payload_episodes:
            for episode_payload in self.payload_episodes:
                self.mongo.insert(self.titanScrapingEpisodios, episode_payload)
        else: 
            Upload(self._platform_code, self._created_at, testing=True)
            print('------------------Nada mas para ingresar-----------------')
            print("Fin")
            self.session.close()

    def _get_categories(self):
        """Método que trae una lista de categorias """
        list_categories = self.request().json()
        dict_contents = list_categories['categories']
        return dict_contents

  
    def request(self): 
       """metodo para hacer una peticion a un servidor"""

       uri = self.api_url
       response = self.session.get(uri)
       return response  

    def request_series(self, _item):
        """Método que hace requests y devielve un diccionario
        Esta funcion rompe en una parte, casi al final de extraer todos los contenidos ###revisar """
        uri_series = self.api_series + _item['slug']
        response = self.session.get(uri_series)
        if response.status_code != 200:
            #try:
                #request_timeout = 5
                #response = self.session.get(uri_series, timeout=request_timeout)
            #except:
            return None

        dict_seasons = response.json()
        return dict_seasons            

    def get_seasons(self, serie, _item, id):
        season_payloads = []
        name = _item['name']
        total_seasons = 0
        if serie:
            for season in serie['seasons']:
                total_seasons += 1
                synopsis = serie['summary']
                deeplink = self.get_deeplink(_item, season=season, parentTitle=_item['slug'])
                season_payload = {
                    "Id": None, #Importante
                    "Synopsis": synopsis, #Importante
                    "Title": name, #Importante, E.J. The Wallking Dead: Season 1
                    "Deeplink": deeplink, #Importante
                    "Number": season['number'], #Importante
                    "Year": None, #Importante
                    "Image": None, 
                    "Directors": None, #Importante
                    "Cast": None, #Importante
                    "Episodes": len(season['episodes']), #Importante
                    "IsOriginal": None 
                },
                self.payloads.append(season_payload)
                self.episodios = 0
                
                for episode in season['episodes']:
                    duration = self.get_duration(episode)
                    deeplink = self.get_deeplink(episode, season=season, parentTitle=_item['slug']) 
                    image = self.get_image(_item, episode=episode)
                    parentTitle = _item['slug']
                    episode_payload = { 
                        "PlatformCode": self._platform_code, #Obligatorio 
                        "Id": episode['_id'], #Obligatorio
                        "ParentId": id, #Obligatorio #Unicamente en Episodios
                         "ParentTitle": parentTitle, #Unicamente en Episodios 
                        "Episode": episode['number'] if episode['number'] != 0 else None, #Obligatorio #Unicamente en Episodios 
                        "Season": episode['season'], #Obligatorio #Unicamente en Episodios
                        "Title": episode['name'], #Obligatorio 
                        "CleanTitle": episode['slug'], #Obligatorio 
                        "OriginalTitle": episode['name'], 
                        "Type": episode['type'], #Obligatorio 
                        "Year": None, #Important! 
                        "Duration": duration,
                        "ExternalIds": deeplink,
                        "Deeplinks": { 
                        "Web": deeplink, #Obligatorio 
                        "Android": None, 
                        "iOS": None, 
                        }, 
                        "Synopsis": episode['description'], 
                        "Image": [image], 
                        "Rating": episode['rating'], #Important! 
                        "Provider": None, 
                        "Genres": [episode['genre']], #Important! 
                        "Directors": None, #Important! 
                        "Availability": None, #Important! 
                        "Download": None, 
                        "IsOriginal": None, #Important! 
                        "IsAdult": None, #Important! 
                        "IsBranded": None, #Important! (ver link explicativo)
                        "Packages": [{'Type':'free-vod'}], #Obligatorio 
                        "Country": None, 
                        "Timestamp": datetime.now().isoformat(), #Obligatorio 
                        "CreatedAt": self._created_at, #Obligatorio
                        }
                    if episode_payload in self.payload_episodes:
                        continue
                    else:
                        self.payload_episodes.append(episode_payload)
                        self.episodios += 1
            return season_payloads 
        else:
            return None  

    def get_serie_payload(self, _item):
        serie = self.request_series(_item)
        if serie:
            seasons = self.get_seasons(serie, _item, _item['_id'])
            deeplink = self.get_deeplink(_item, serie['slug'])
            #image = self.get_image(_item)
            serie_payload = {
                "PlatformCode": self._platform_code, #Obligatorio 
                "Id": _item['_id'], #Obligatorio
                "Seasons": len(seasons),
                "Title": _item['name'], #Obligatorio  
                "CleanTitle": _replace(_item['name']), #Obligatorio 
                "OriginalTitle": _item['name'], 
                "Type": 'serie', #Obligatorio 
                "Year": None, #Important! 
                "Duration": None, 
                "ExternalIds": None, 
                "Deeplinks": { 
                "Web": deeplink, #Obligatorio 
                "Android": None, 
                "iOS": None, 
                }, 
                "Synopsis": _item['description'], 
                #"Image": [image], 
                "Rating": _item['rating'], #Important! 
                "Provider": None, 
                "Genres": [_item['genre']], #Important!  "Cast": "list", 
                "Directors": None, #Important! 
                "Availability": None, #Important! 
                "Download": None, 
                "IsOriginal": None, #Important! 
                "IsAdult": None, #Important! 
                "IsBranded": None, #Important! (ver link explicativo)
                # "Packages": "Free", #Obligatorio 
                "Packages": [{'Type':'free-vod'}],
                "Country": None, 
                "Timestamp": datetime.now().isoformat(), #Obligatorio 
                "CreatedAt": self._created_at, #Obligatorio
            }
            return serie_payload
        else:
            return None
    

   

    def get_payloads(self, _item):
        """Metodo que genera payloads por contenido """ 
      
        deeplink = self.get_deeplink(_item, 'movie')
        duration = self.get_duration(_item)
        image = self.get_image(_item)
        #print('Movie: ' + _item['name'])
        payload = { 
            "PlatformCode": self._platform_code, #Obligatorio 
            "Id": _item['_id'], #Obligatorio
            "Title": _item['name'], #Obligatorio 
            "CleanTitle": _replace(_item['name']), #Obligatorio 
            "OriginalTitle": _item['name'], 
            "Type": _item['type'], #Obligatorio 
            "Year": None, #Important! 
            "Duration": duration,
            "ExternalIds": None,  #No estoy seguro de si es
            "Deeplinks": { 
            "Web": deeplink, #Obligatorio 
            "Android": None, 
            "iOS": None, 
            }, 
            "Synopsis": _item['summary'], 
            "Image": [image],
            "Rating": _item['rating'], #Important! 
            "Provider": None,
            "Genres": [_item['genre']], #Important!
            "Cast": None, 
            "Directors": None, #Important! 
            "Availability": None, #Important! 
            "Download": None, 
            "IsOriginal": None, #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            # "Packages": 'Free', #Obligatorio 
            "Packages": [{'Type':'free-vod'}],
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
            }
      #  self.payloads.append(payload)
        return payload
      
    def get_image(self, _item, episode=None):
        if _item['type'] == 'movie':
            image = 'https://api.pluto.tv/v3/images/episodes/' + str(_item['_id']) + '/poster.jpg'
        elif _item['type'] == 'serie':
            image = ['https://api.pluto.tv/v3/images/series/' + str(_item['_id']) + '/poster.jpg'] 
        elif episode:
            image = _item['featuredImage']
        return image
    
    def get_deeplink(self, _item,  season = None, parentTitle = None):
        if _item['type'] == 'movie':
            deeplink = 'https://pluto.tv/on-demand/movies/' + _item['slug']
        elif _item['type'] == 'series':
            deeplink = 'https://pluto.tv/on-demand/series/' + _item['slug']
        elif _item['type'] == 'episode':
            deeplink = 'https://pluto.tv/on-demand/series/' + parentTitle + '/episode/' + str(_item['slug'])
        elif _item['type'] == 'season':
            deeplink = 'https://pluto.tv/on-demand/series/' + parentTitle + '/season/' + str(season)
        return deeplink

    def get_duration(self, _item):
        """Método que convierte la duración en horas(?)"""
        duration = int((_item['duration']) / 60000)
        return duration