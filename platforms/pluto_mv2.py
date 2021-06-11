from os import replace
from pprint                 import pp
import time
import requests
from requests.models        import Response
from handle.replace         import _replace
from common                 import config
from handle.mongo           import mongo
from updates.upload         import Upload
from datetime               import datetime
import pandas
# from time import sleep
# import re
 sdasda
class Pluto_mv22():
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
        self.api_season =    self._config['api_season']
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
        self.episodes = []

        self.scraped = self.query_field(self.titanScraping, field='Id')   #
        self.scraped_episodes = self.query_field(self.titanScrapingEpisodios, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")
        print(f"{self.titanScrapingEpisodios} {len(self.scraped_episodes)}")
        
        contents = self.get_contents()

        for n, items in enumerate(contents):
            print(f"\n----- Progreso ({n}/{len(contents)}) -----\n")
            try:
                if items['type'] == 'movie':                #MOVIE  #bastante ok
                    self.get_payload(items)
                else:                                       #SERIE
                    uri_epi = self.api_season.format(items['_id']) #PORQUE NO ESTA EN AMARILLO? WTF,anda
                    response_epi = self.request(uri_epi)    #Hago el Request aca.
                    list_epi = response_epi.json()
                    self.get_payload(list_epi) 
            except:
                print(items)
                print("Arreglar.")

        self.mongo.insertMany(self.titanScraping, self.payloads)
        self.mongo.insertMany(self.titanScrapingEpisodios, self.episodes)


        Upload(self._platform_code, self._created_at, testing=True)
        print("Scraping finalizado")
        self.session.close()


    def request(self, url):
        response = self.session.get(url)
        if response.status_code == 200:
            return response
        
    def get_payload(self,content):
        
        if content['type'] == 'movie':
            self.movie_payload(content)
        elif content['type'] == 'series':
            self.serie_payload(content)        

    def movie_payload(self, element):
        duration = self.get_duration(element['duration'])
        
        deeplink = self.movie_deeplink(str(element['slug']))
        image = self.get_image(element['_id'], element['type'])
        genre = str(element['genre']).split(" & ")
        print(element['name'])
        payload = { 
            "PlatformCode": str(self._platform_code),#Obligatorio 
            "Id":       str(element['_id']),         #Obligatorio
            "Title":    str(element['name']),        #Obligatorio 
            "CleanTitle":   str(element['name']),    #Obligatorio 
            "OriginalTitle":None, 
            "Type":     'movie',        #Obligatorio 
            "Year":     None,                        #Important! 
            "Duration": int(duration),
            "ExternalIds": None, #                   
            "Deeplinks": { 
            "Web": str(deeplink),                    #Obligatorio 
            "Android":  None, 
            "iOS":      None, 
            }, 
            "Synopsis": str(element['summary']), 
            "Image":    [image],
            "Rating":   str(element['rating']),  #Important! 
            "Provider":     None,
            "Genres":       genre,  #Important!
            "Cast":         None, 
            "Directors":    None,   #Important! 
            "Availability": None,   #Important! 
            "Download":     None, 
            "IsOriginal":   None,   #Important! 
            "IsAdult":      None,   #Important! 
            "IsBranded":    None,   #Important! (ver link explicativo)
            "Packages":     [{'Type':'free-vod'}],     #Obligatorio 
            "Country":      None, 
            "Timestamp":    str(datetime.now().isoformat()),#Obligatorio 
            "CreatedAt":    str(self._created_at),          #Obligatorio
            }
        self.payloads.append(payload)   
        
    def serie_payload(self, element):
        
        deeplink = self.serie_deeplink(str(element['slug']))
        seasons = self.get_season(element)
        genre = str(element['genre']).split(" & ")
        image = self.get_image(element['_id'], element['type'])
        print(element['name'])

        payloads = []
        payload = { 
            "PlatformCode": str(self._platform_code),#Obligatorio #
            "Id":           str(element['_id']),         #Obligatorio #  
            "Seasons":      str(seasons),                                 ######SEASON
            "Title":        str(element['name']),        #Obligatorio #
            "CleanTitle":   str(element['name']),      #Obligatorio #
            "OriginalTitle":None,               
            "Type":         'serie',                #Obligatorio #
            "Year":         None,                   #Important!  #
            "Duration":     None,                                #
            "ExternalIds":  None,             
            "Deeplinks": { 
            "Web":  str(deeplink),                    #Obligatorio#
            "Android":  None, 
            "iOS":      None, 
            }, 
            "Synopsis": str(element['summary']),                  #
            "Image":    [image],                                #######IMAGEN
            "Rating":   str(element['rating']),      #Important! 
            "Provider":     None,
            "Genres":       genre,       #           Important!     ##SPLIT GENRE
            "Cast":         None, 
            "Directors":    None,               #Important! 
            "Availability": None,               #Important! 
            "Download":     None, 
            "IsOriginal":   None, #Important! 
            "IsAdult":      None, #Important! 
            "IsBranded":    None, #Important! (ver link explicativo)
            "Packages":     [{'Type':'free-vod'}],                   #Obligatorio 
            "Country":      None, 
            "Timestamp": str(datetime.now().isoformat()),   #Obligatorio 
            "CreatedAt": str(self._created_at),             #Obligatorio
            }
        payloads.append(payload)  

    def get_season(self,element):    
        seasons_dict = []
        seasons = element['seasons']
        for season in seasons:
            #image = self.get_image(season['_id'], episode['type']) 
            deeplink = self.seasson_deeplink(element['slug'],season['number'])
            season_payload = {
                "Id":       str(element['_id']),        #Importante
                "Synopsis": str(element['description']),#Importante
                "Title":    str(element['name']),       #Importante
                "Deeplink": str(deeplink),              #Importante
                "Number":   season['number'],           #Importante
                "Year":     None,   #Importante
                "Image":    None,                       #no hay image
                "Directors":None,   #Importante
                "Cast":     None,   #Importante
                "Episodes": len(season['episodes']),    #Importante
                "IsOriginal":None 
            }
            seasons_dict.append(season_payload)
            
            for episode in  season['episodes']:
                            
                duration = self.get_duration(episode['duration'])           ####
                deeplink = self.episode_deeplink(element['slug'], season['number'],episode['slug'])                          ####funcionan los self? no entiendo
                image = self.get_image(episode['_id'], episode['type'])
                genre = str(element['genre']).split(" & ")

                episode_payload = { 
                        "PlatformCode": str(self._platform_code),    #Obligatorio 
                        "Id": str(episode['_id']),                   #Obligatorio
                        "ParentId": str(episode['_id']),                  #Obligatorio #Unicamente en Episodios
                        "ParentTitle": str(episode['name']),         #Unicamente en Episodios 
                        "Episode": , #Obligatorio #Unicamente en Episodios 
                        "Season": int(episode['season']),            #Obligatorio #Unicamente en Episodios
                        "Title": episode['name'],               #Obligatorio 
                        "CleanTitle": episode['name'],  #Obligatorio 
                        "OriginalTitle": None, 
                        "Type": episode['type'],                #Obligatorio 
                        "Year": None,           #Important! 
                        "Duration": duration,
                        #"ExternalIds": deeplink,
                        "Deeplinks": { 
                        "Web": deeplink,        #Obligatorio 
                        "Android": None, 
                        "iOS": None, 
                        }, 
                        "Synopsis": episode['description'], 
                        "Image": [image], 
                        "Rating": episode['rating'],    #Important! 
                        "Provider": None, 
                        "Genres": genre,   #Important! 
                        "Directors": None,      #Important! 
                        "Availability": None,   #Important! 
                        "Download": None, 
                        "IsOriginal": None,     #Important! 
                        "IsAdult": None,        #Important! 
                        "IsBranded": None,      #Important! (ver link explicativo)
                        "Packages": [{'Type':'free-vod'}],      #Obligatorio 
                        "Country": None, 
                        "Timestamp": datetime.now().isoformat(),#Obligatorio 
                        "CreatedAt": self._created_at,          #Obligatorio
                        }
                        
                self.episodes.append(episode_payload)
                          
    #Deeplinks:
    def movie_deeplink(self, slug):
        deeplink = "https://pluto.tv/on-demand/movies/{}".format(slug)
        return deeplink
    def serie_deeplink(self, slug):
        deeplink = "https://pluto.tv/on-demand/series/{}/".format(slug)
        return deeplink
    
    def seasson_deeplink(self, slug, number_season ):                                              #HACER XD
        deeplink = "https://pluto.tv/on-demand/series/{}/season/{}/".format(slug, number_season )
        return deeplink
    def episode_deeplink(self,slug,number_season, episode_slug):
        deeplink = "https://pluto.tv/on-demand/series/{}/season/{}/episode/{}".format(slug,number_season, episode_slug)
        return deeplink

    #Image
    def get_image(self, id, type):
        if type == 'movie' or 'episode':
            image = 'https://api.pluto.tv/v3/images/episodes/{}/poster.jpg'.format(str(id))
        elif type == 'serie':
            image = 'https://api.pluto.tv/v3/images/series/{}}/poster.jpg'.format(str(id)) 
        return image

    def get_contents(self):
        """Método que trae los contenidos en forma de diccionario.

        Returns:
            list: Lista de diccionarios
        """
        content_list = []
        uri = self.api_url
        response = self.request(uri)
        list_categories = response['categories']
        for categories in list_categories:
            content_list += categories['items']

        return content_list 