from logging import getLogRecordFactory
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
# from time import sleep
# import re


class StarzPQ():
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

        self.all_titles_url = self._config['all_titles_url']

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
        self.scraped = [] #self.query_field(self.titanScraping, field='Id')
        self.scraped_episodes = [] #self.query_field(self.titanScrapingEpisodes, field='Id')

        payloads_episodes = []
        payloads = self.get_content(payloads_episodes)#Hago todo en la misma funcion, busco los datos de las peliculas (return) y las episodios de las series (payloads_episodes)

        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
        
        Upload(self._platform_code, self._created_at, testing=True)
    
    def get_content(self, payloads_episodes): 
        payloads = []
        res_all_titles = self.request(self.all_titles_url) #Traigo el json con todos los datos

        for content in res_all_titles["playContentArray"]["playContents"]:

            if not content["contentId"] in self.scraped: #Consulto si esta en la bbdd
                self.scraped.append(content["contentId"])
                payload = self.get_payload(content)
                payloads.append(payload)
                if payload["Type"] == "series":
                    payload["Seasons"] = self.get_seasons(content)                    
                    self.get_payload_episodes(content["childContent"], payloads_episodes)
        
        return payloads

    def get_payload_episodes(self, seasons, payloads_episodes):
        for season in seasons:
            i = 1 #los episodios no estan numerados
            for episode in season["childContent"]:
                if not episode["contentId"] in self.scraped_episodes:
                    self.scraped_episodes.append(episode["contentId"])
                    payloads_episode = self.get_payload(episode)
                    payloads_episodes.append(payloads_episode)


    def get_payload(self, content_dict):
        """Método para crear el payload. Para titanScraping.

        Args:
            content_metadata (dict): Indica la metadata del contenido.

        Returns:
            dict: Retorna el payload.
        """
        payload_contenidos = { 
            "PlatformCode": self._platform_code,   #Obligatorio 
            "Id": str(content_dict["contentId"]),  #Obligatorio
            "Seasons": [], #Lo hago aparte
            "Crew": [ #Importante
                {
                "Role": None, 
                "Name": None
                },
            ],
            "Title": content_dict["title"], #Obligatorio 
            "CleanTitle": _replace(content_dict["title"]), #Obligatorio 
            "OriginalTitle": content_dict["titleSort"], 
            "Type": self.get_type(content_dict["contentType"]), #Obligatorio #movie o serie 
            "Year": self.get_year(content_dict), #Important! 1870 a año actual 
            "Duration": self.get_duration(content_dict), #en minutos 
            "ExternalIds": [], #consultar
            "Deeplinks": { 
                "Web": self.get_deeplinks(content_dict), #Obligatorio 
                "Android": None, 
                "iOS": None, 
            }, 
            "Synopsis": content_dict["logLine"], 
            "Image": self.get_image(content_dict["contentId"]), 
            "Rating": content_dict["ratingCode"], #Important!  "Provider": "list", 
            "Genres": self.get_genres(content_dict["genres"]), #Important! 
            "Cast": self.get_cast(content_dict), #Important! 
            "Directors": self.get_directors(content_dict), #Important! 
            "Availability": "str", #Important! 
            "Download": self.get_download(content_dict), 
            "IsOriginal": content_dict["original"], #Important! 
            "IsAdult": None, #Important! 
            "IsBranded": None, #Important! (ver link explicativo)
            "Packages": [{"Type":"subscription-vod"}], #Obligatorio 
            "Country": None, 
            "Timestamp": datetime.now().isoformat(), #Obligatorio 
            "CreatedAt": self._created_at, #Obligatorio
        }
        return payload_contenidos

    def get_seasons(self, content):
        res = []
        seasons = content["childContent"]
        for season in seasons:
            season_ok = {
                "Id": season["contentId"],                                            #Importante
                "Synopsis": season["contentId"],                                      #Importante
                "Title": season["title"],                                         #Importante, E.J. The Wallking Dead:
                "Deeplink": "str",                                      #Importante
                "Number": season["order"],                                        #Importante
                "Year": season["minReleaseYear"],                                          #Importante
                "Image": "list", 
                "Directors": self.get_directors(season),                                    #Importante
                "Cast": self.get_cast(season),                                         #Importante
                "Episodes": season["episodeCount"],                                       #Importante
                "IsOriginal": season["original"] 
                }
            res.append(season_ok)
        return res
    
    ############################## FUNCIONES PARA PAYLOAD #######################################
    def get_duration(self, content):
        if content["contentType"] == "Movie":
            return content["runtime"]//60
        else:
            return None
    
    def get_type(self, type_):
        if type_ == "Movie":
            return "movie"
        else:
            return "serie"

    def get_deeplinks(self, content): #armo la url con el titulo en minusculas y con - en cada espacio. Luego le agrego el id
        title = (content["titleSort"].replace(" ","-")).lower()
        url = title+"-"+str(content["contentId"])
        return url
    
    def get_year(self, content):
        res = None
        try:
            if content["contentType"] == "Movie":
                return int(content["releaseYear"])
            else:
                return int(content["minReleaseYear"])
        except:
            return res
    
    def get_image(self, id): #las imagenes de la season y las imagenes de los episodios son los mismos
        return ["https://stz1.imgix.net/Web_AR/contentId/"+str(id)+"/type/KEY/dimension/1536x2048/lang/es-419"]
        #Esta es para una segunda img pero hay que comprobar si para cada season o episode cambia
        #https://stz1.imgix.net/Web_AR/contentId/PONER-ID/type/STUDIO/dimension/2560x1440/lang/es-419
    
    def get_genres(self, genres):
        res = []
        for genre in genres:
            res.append(genre["description"])
        return res
    
    def get_directors(self, content):
        res = []
        try:
            for director in content["directors"]:
                res.append(director["fullName"])
        except:
            pass
        return res
    
    def get_cast(self, content):
        res = []
        try:
            for c in content["actors"]:
                res.append(c["fullName"])
        except:
            pass
        return res
    
    def get_download(self, content):
        res = None
        try:
            res = content["downloadable"]
        except:
            pass
        return res
    ########################################################################################################

    def cosas():
        payload_contenidos = { 
                                                      #Obligatorio
            "Crew": [ #Importante
                {
                "Role": str, 
                "Name": str
                },
            ],
            "Title": "str", #Obligatorio 
            "CleanTitle": "_replace(str)", #Obligatorio 
            "OriginalTitle": "str", 
            "Type": "str", #Obligatorio #movie o serie 
            "Year": "int", #Important! 1870 a año actual 
            "Duration": "int", #en minutos 
            "ExternalIds": "list",
            "Deeplinks": { 
                "Web": "str", #Obligatorio 
                "Android": "str", 
                "iOS": "str", 
            }, 
            "Synopsis": "str", 
            "Image": "list", 
            "Rating": "str", #Important!  "Provider": "list", 
            "Genres": "list", #Important! 
            "Cast": "list", #Important! 
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

        payload_episodios = { 
            
            "ParentId": "str", #Obligatorio #Unicamente en Episodios
            "ParentTitle": "str", #Unicamente en Episodios 
            "Episode": "int", #Obligatorio #Unicamente en Episodios 
            "Season": "int", #Obligatorio #Unicamente en Episodios
            "Crew": [ #Importante
                {
                    "Role": str, 
                    "Name": str
                },
            ],
            "Title": "str", #Obligatorio 
            "OriginalTitle": "str", 
            "Year": "int", #Important! 
            "Duration": "int", 
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
            "Genres": "list", #Important! 
            "Cast": "list", #Important! 
            "Directors": "list", #Important! 
            "Availability": "str", #Important! 
            "Download": "bool", 
            "IsOriginal": "bool", #Important! 
            "IsAdult": "bool", #Important! 
            "IsBranded": "bool", #Important! (ver link explicativo) "Packages": "list", #Obligatorio 
            "Country": "list", 
            "Timestamp": "str", #Obligatorio 
            "CreatedAt": "str", #Obligatorio
        }
        
    def request(self, url):
        '''
        Método para hacer una petición
        '''
        requestsTimeout = 5
        while True:
            try:
                response = self.session.get(url, timeout=requestsTimeout)
                return response.json()
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(requestsTimeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...')
                time.sleep(requestsTimeout)
                continue
    

        
"""
payload = {}
# Indica si el payload a completar es un episodio:        
payload['PlatformCode'] = self._platform_code
payload['Id'] = content_dict["contentId"]
payload['Title'] = content_dict["title"]
payload['OriginalTitle'] = content_dict["titleSort"]
payload['CleanTitle'] = _replace(content_dict["title"])
payload['Duration'] = self.get_duration(content_dict)
payload['Type'] = self.get_type(content_dict["contentType"]) 
payload['Year'] = self.get_year(content_dict)
payload['Deeplinks'] = self.get_deeplinks(content_dict)
payload['Playback'] = None #consultar
payload['Synopsis'] = content_dict["logLine"]
payload['Image'] = self.get_image(content_dict["contentId"]) #consultar si no hay a mano
payload['Rating'] = content_dict["ratingCode"]
payload['Provider'] = None
payload['Genres'] = self.get_genres(content_dict["genres"])
payload['Cast'] = None
payload['Directors'] = None
payload['Availability'] = None
payload['Download'] = None
payload['IsOriginal'] = None
payload['Seasons'] = None
payload['Seasons'] = None
payload['IsBranded'] = None
payload['IsAdult'] = None
payload['Packages'] = [{"Type":"free-vod"}]
payload['Country'] = None
payload['Crew'] = None        
payload['Timestamp'] = datetime.now().isoformat()
payload['CreatedAt'] = self._created_at
"""