import time
import requests
#import pymongo
import re
#import json
from handle.replace import _replace
from common import config
from datetime import datetime
from handle.mongo import mongo
from handle.datamanager import Datamanager
from updates.upload import Upload
class AmcPau():
    """
    Amc es una ott de Estados Unidos.

    DATOS IMPORTANTES:
    - Versión Final: Si.
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si.
    - ¿Usa BS4?: No.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? 1 min
    - ¿Cuanto contenidos trajo la ultima vez? TS:163 TSE: 960 07/10/21

    OTROS COMENTARIOS:
        Contenia series sin episodios, se modificó el script para excluirlas.

    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.skippedEpis = 0
        self.skippedTitles = 0
        ################# URLS  #################
        self._movies_url = self._config['movie_url']
        self._show_url = self._config['show_url']
        #Url para encontrar la información de los contenidos por separado
        self._format_url = self._config['format_url'] 
        self._episode_url = self._config['episode_url']
        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}
        self.payloads_series = []

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
            self.testing = True
            self._scraping()

    def _scraping(self, testing=False):
        # Definimos los links de las apis y con el Datamanager usamos la función _getJson
        episode_data = Datamanager._getJSON(self, self._episode_url)
        serie_data = Datamanager._getJSON(self, self._show_url )
        movie_data = Datamanager._getJSON(self, self._movies_url)

        self.get_payload_movies(movie_data)
        self.get_payload_series(serie_data)
        self.get_payload_episodes(episode_data)


    ########## Payload Movies ##########

    def get_payload_movies(self, content):
        payloads = []
        list_db = Datamanager._getListDB(self, self.titanScraping)
        data = content['data']['children']
        for item in data:
            if item['properties'].get('title'):
                if 'Movies' in item['properties']['title']:
                    movies_data = item
                    break
        for movie in movies_data['children']:
            payload_movies = {
                "PlatformCode":  self._platform_code,
                "Title":         self.get_title_movie(movie),
                "CleanTitle":    self.get_title_movie(movie),
                "OriginalTitle": None,
                "Type":          "movie",
                "Year":          None,
                "Duration":      None,
                "Id":            self.get_id_movie(movie),
                "Deeplinks": {
                    "Web":       self.get_deeplink_movie(movie), #no sé cómo va el replace
                    "Android":   None,
                    "iOS":       None,
                },
                "Synopsis":      self.get_syn_movie(movie),
                "Image":         self.get_image_movie(movie),
                "Rating":        None,  # Important!
                "Provider":      None,
                "Genres":        self.get_genre_movie(movie),  # Important!
                "Cast":          None,
                "Directors":     None,  # Important!
                "Availability":  None,  # Important!
                "Download":      None,
                "IsOriginal":    None,  # Important!
                "IsAdult":       None,  # Important!
                "IsBranded":     None,  # Important!
                # Obligatorio
                "Packages":      [{'Type': 'tv-everywhere'}],
                "Country":       None,
                "Timestamp":     datetime.now().isoformat(),  # Obligatorio
                "CreatedAt":     self._created_at,  # Obligatorio
            }

            Datamanager._checkDBandAppend(self, payload_movies, list_db, payloads)
        Datamanager._insertIntoDB(self, payloads, self.titanScraping)

    ########## Métodos movies ##########

    def get_title_movie(self, movies_data):
        try:
            title = movies_data['properties']['cardData']['text']['title']
        except KeyError:
            print(movies_data)
            raise
        return title


    def get_id_movie(self, movies_data):
        id = movies_data['properties']['cardData']['meta']['nid']
        return id

    
    def get_deeplink_movie(self, movies_data):
        deeplink = "https://www.amc.com" + movies_data['properties']['cardData']['meta']['permalink']
        return deeplink

    
    def get_syn_movie(self, movies_data):
        syn = movies_data['properties']['cardData']['text']['description']
        return syn


    def get_genre_movie(self, movies_data):
        genre = movies_data['properties']['cardData']['meta']['genre']
        return genre

    
    def get_image_movie(self, movies_data):
        image = movies_data['properties']['cardData']['images']
        return image


    ########## Payload series ##########

    def get_payload_series(self, content):
        
        list_db_series = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)
        data = content['data']['children']
        for item in data:
            if item['properties'].get('title'):
                if 'Shows' in item['properties']['title']:
                    series_data = item
                    break
        for serie in series_data['children']:
            payload_series =  {
                "PlatformCode":     self._platform_code, #Obligatorio   
                "Id":               self.get_id_serie(serie), #Obligatorio
                "Seasons":          [ #Unicamente para series
                    {
                    "Id":           None,           #Importante
                    "Synopsis":     None,     #Importante
                    "Title":        None,        #Importante, E.J. The Wallking Dead: Season 1
                    "Deeplink":     None,    #Importante
                    "Number":       None,       #Importante
                    "Year":         None,         #Importante
                    "Image":        None, 
                    "Directors":    None,   #Importante
                    "Cast":         None,        #Importante
                    "Episodes":     None,      #Importante
                    "IsOriginal":   None    #packages
                    },
                ],
                "Crew":          [ #Importante
                    {
                        "Role": 'str', 
                        "Name": 'str'
                    },
                ],
                "Title":         self.get_title_serie(serie), #Obligatorio      
                "CleanTitle":    self.get_title_serie(serie), #Obligatorio      
                "OriginalTitle": None,                          
                "Type":          None,     #Obligatorio  #movie o serie     
                "Year":          None,     #Important!  1870 a año actual   
                "Duration":      None,     #en minutos   
                "ExternalIds":   None,       
                "Deeplinks": {
                    "Web":       self.get_deeplink_serie(serie), #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },
                "Synopsis":      self.get_syn_serie(serie),      
                "Image":         self.get_image_serie(serie),      
                "Subtitles":     None,
                "Dubbed":        None,
                "Rating":        None,     #Important!      
                "Provider":      None,      
                "Genres":        None,    #Important!      
                "Cast":          None,    #Important!        
                "Directors":     None,    #Important!      
                "Availability":  None,     #Important!      
                "Download":      None,      
                "IsOriginal":    None,    #Important!        
                "IsAdult":       None,    #Important!   
                "IsBranded":     None,    #Important!   (ver link explicativo)
                "Packages":      [{'Type': 'tv-everywhere'}],    #Obligatorio      
                "Country":       None,
                "Timestamp":     datetime.now().isoformat(), #Obligatorio
                "CreatedAt":     self._created_at, #Obligatorio
            }
            

            Datamanager._checkDBandAppend(self, payload_series, list_db_series, self.payloads_series)
        self.copiapayloads = [{"Id":pay["Id"], "CleanTitle":pay["CleanTitle"].lower().strip()} for pay in self.payloads_series]
        Datamanager._insertIntoDB(self, self.payloads_series, self.titanScraping)



    ########## Métodos series ##########

    def get_title_serie(self, series_data):
        title = series_data['properties']['cardData']['text']['title']
        return title


    def get_id_serie(self, series_data):
        id = series_data['properties']['cardData']['meta']['nid']
        return id

    
    def get_deeplink_serie(self, series_data):
        deeplink = "https://www.amc.com" + series_data['properties']['cardData']['meta']['permalink']
        return deeplink

    
    def get_syn_serie(self, series_data):
        syn = series_data['properties']['cardData']['text']['description']
        return syn


    def get_image_serie(self, series_data):
        image =  series_data['properties']['cardData']['images']
        return image    

    
    ########## Payload Episodes ##########
                
    def get_payload_episodes(self, content):
        payloads_episodes = []
        list_db_episodes = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)
        data = content['data']['children']
        for item in data:
            if item['type']== "list":
                episodes_data = item
                break
        for serie in episodes_data['children']:
            for episode in serie['children']:
                payload_episodios = {      
                    "PlatformCode":  self._platform_code, #Obligatorio      
                    "Id":            self.get_id_episodes(episode), #Obligatorio
                    "ParentId":      self.get_parent_id(serie), #Obligatorio #Unicamente en Episodios
                    "ParentTitle":   self.get_parent_title(serie), #Unicamente en Episodios 
                    "Episode":       self.get_episode_num(episode), #Obligatorio #Unicamente en Episodios  
                    "Season":        self.get_season(episode), #Obligatorio #Unicamente en Episodios
                    "Crew":          [ #Importante
                                        {
                                            "Role": 'str', 
                                            "Name": 'str'
                                        },
                    ],
                    "Title":         self.get_title_episodes(episode), #Obligatorio      
                    "OriginalTitle": self.get_title_episodes(episode),                          
                    "Year":          None,     #Important!     
                    "Duration":      None,      
                    "ExternalIds":   None,  
                    "Deeplinks": {          
                        "Web":       self.get_deeplink_episodes(episode) ,       #Obligatorio          
                        "Android":   None,          
                        "iOS":       None,      
                    },      
                    "Synopsis":      self.get_syn_episodes(episode),      
                    "Image":         self.get_image_episodes(episode),     
                    "Subtitles":     None,
                    "Dubbed":        None,
                    "Rating":        None,     #Important!      
                    "Provider":      None,      
                    "Genres":        None,    #Important!      
                    "Cast":          None,    #Important!        
                    "Directors":     None,    #Important!      
                    "Availability":  None,     #Important!      
                    "Download":      None,      
                    "IsOriginal":    None,    #Important!      
                    "IsAdult":       None,    #Important!   
                    "IsBranded":     None,    #Important!   (ver link explicativo)
                    "Packages":     [{'Type': 'tv-everywhere'}],    #Obligatorio      
                    "Country":       None,      
                    "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                    "CreatedAt":     self._created_at, #Obligatorio 
                    }
            
            Datamanager._checkDBandAppend(self, payload_episodios, list_db_episodes, payloads_episodes)
        Datamanager._insertIntoDB(self, payloads_episodes, self.titanScraping)

############# Métodos episodes ######################

    def get_title_episodes(self, episode):
        title = episode['properties']['cardData']['text']['title']
        return title


    def get_id_episodes(self, episode):
        id = episode['properties']['cardData']['meta']['nid']
        return id   

    
    def get_deeplink_episodes(self, episode):
        deeplink = "https://www.amc.com" + episode['properties']['cardData']['meta']['permalink']
        return deeplink

    
    def get_syn_episodes(self, episode):
        syn =  episode['properties']['cardData']['text']['description']
        return syn


    def get_image_episodes(self, episode):
        try:
            image = episode['properties']['cardData']['images']
        except:
            image = None
        return image


    def get_parent_id(self, serie):
        self.counter = 0
        parent = self.get_parent_title(serie)
        for item in self.copiapayloads:
            if item['CleanTitle'] == parent.lower().strip():
                parent_id = item['Id']
                break
            else:
                parent_id = "no id ", self.counter + 1
        return parent_id

    def get_parent_title(self, serie):
        parent_title = serie['properties']['title']
        return parent_title


    def get_episode_num(self, episodes_data):
        #"S1, E1"
        episode_number = episodes_data['properties']['cardData']['text'].get('seasonEpisodeNumber')
        if episode_number:
            episode = episode_number.split(",")
            return int(episode[-1].replace('E',""))


    def get_season(self, episodes_data):
        episode_season = episodes_data['properties']['cardData']['text'].get('seasonEpisodeNumber')
        if episode_season:
            season = episode_season.split(",")
            return int(season[0].replace('S', ""))



































        # while True:
        #     # condición que rompe el bucle infinito
        #     try:
        #         if i == (len(episode_data['data']['children'][2]['children'])-1):
        #             break
        #     except:
        #         break
        #     # Ubicamos los valores dentro del Json
        #     movies = movie_data['data']['children'][4]['children']
        #     episodes = episode_data['data']['children'][2]['children']
        #     shows = serie_data['data']['children'][4]['children']
        #     # Recorremos el Json de Peliculas y definimos los contendios del Payload
        #     for movie in movies:
        #         print(movie)
        #         deeplink = (self._format_url).format(movie['properties']['cardData']['meta']['permalink'])
        #         payload_peliculas = {
        #             "PlatformCode":  self._platform_code,
        #             "Title":         movie['properties']['cardData']['text']['title'],
        #             "CleanTitle":    _replace(movie['properties']['cardData']['text']['title']),
        #             "OriginalTitle": None,
        #             "Type":          "movie",
        #             "Year":          None,
        #             "Duration":      None,

        #             "Id":            str(movie['properties']['cardData']['meta']['nid']),
        #             "Deeplinks": {

        #                 "Web":       deeplink.replace('/tve?',''),
        #                 "Android":   None,
        #                 "iOS":       None,
        #             },
        #             "Synopsis":      movie['properties']['cardData']['text']['description'],
        #             "Image":         self.get_images(movie),
        #             "Rating":        None,  # Important!
        #             "Provider":      None,
        #             "Genres":        [movie['properties']['cardData']['meta']['genre']],  # Important!
        #             "Cast":          None,
        #             "Directors":     None,  # Important!
        #             "Availability":  None,  # Important!
        #             "Download":      None,
        #             "IsOriginal":    None,  # Important!
        #             "IsAdult":       None,  # Important!
        #             "IsBranded":     None,  # Important!
        #             # Obligatorio
        #             "Packages":      [{'Type': 'tv-everywhere'}],
        #             "Country":       None,
        #             "Timestamp":     datetime.now().isoformat(),  # Obligatorio
        #             "CreatedAt":     self._created_at,  # Obligatorio
        #         }
        #         Datamanager._checkDBandAppend(
        #             self, payload_peliculas, ids_guardados, payloads_series
        #         )
        #     # Recorremos el Json de las series y definimos los valores del diccionario 
        #     for show in shows:
        #         deeplink = (self._format_url).format(show['properties']['cardData']['meta']['permalink'])
        #         payload_series = {
        #             "PlatformCode":  self._platform_code,
        #             "Id":            str(show['properties']['cardData']['meta']['nid']),
        #             'Title':         show['properties']['cardData']['text']['title'],
        #             "Type":          "serie",
        #             'OriginalTitle': None,
        #             'Year':          None,
        #             'Duration':      None,
        #             'Deeplinks': {
        #                 'Web':       deeplink.replace('/tve?',''),
        #                 'Android':   None,
        #                 'iOS':       None,
        #             },
        #             'Playback':      None,
        #             "CleanTitle":    _replace(show['properties']['cardData']['text']['title']),
        #             'Synopsis':      show['properties']['cardData']['text']['description'],
        #             'Image':         self.get_images(show),
        #             'Rating':        None,
        #             'Provider':      None,
        #             'Genres':        None,
        #             'Cast':          None,
        #             'Directors':     None,
        #             'Availability':  None,
        #             'Download':      None,
        #             'IsOriginal':    None,
        #             'IsAdult':       None,
        #             'Packages':
        #             [{'Type': 'tv-everywhere'}],
        #                 'Country':       None,
        #                 'Timestamp':     datetime.now().isoformat(),
        #                 'CreatedAt':     self._created_at
        #         }
        #         payload_epis = []
        #         for episode in episodes:
        #             if episode['properties']['title'] == show['properties']['cardData']['text']['title']:
        #                 for episode_data in episode['children']:
        #                     # un filtro para limpiar la información concatenada del json.
        #                     filtro = str(
        #                         episode_data['properties']['cardData']['text']['seasonEpisodeNumber'])
        #                     season_ = re.sub(
        #                         '[A-Z] ', "", filtro)
        #                     episode__ = season_.split(', ')
        #                     deeplink = (self._format_url).format(episode_data['properties']['cardData']['meta']['permalink'])
        #                     payload = {
        #                         "PlatformCode":  self._platform_code,
        #                         "Id":            str(episode_data['properties']['cardData']['meta']['nid']),
        #                         "ParentId":      str(show['properties']['cardData']['meta']['nid']),
        #                         "ParentTitle":   show['properties']['cardData']['text']['title'],
        #                         "Episode":       int(episode__[1].replace('E', '')),
        #                         "Season":        int(episode__[0].replace('S', '')),
        #                         'Id':            str(episode_data['properties']['cardData']['meta']['nid']),
        #                         'Title':         episode_data['properties']['cardData']
        #                         ['text']['title'],
        #                         'OriginalTitle': None,
        #                         'Year':          None,
        #                         'Duration':      None,
        #                         'Deeplinks': {
        #                             'Web':       deeplink.replace('/tve?',""),
        #                             'Android':   None,
        #                             'iOS':       None,
        #                         },
        #                         'Playback':      None,
        #                         "CleanTitle":    _replace(show['properties']['cardData']['text']['title']),
        #                         'Synopsis':      episode_data['properties']['cardData']['text']['description'],
        #                         'Image':         self.get_images(episode_data),
        #                         'Rating':        None,
        #                         'Provider':      None,
        #                         'Genres':        None,
        #                         'Cast':          None,
        #                         'Directors':     None,
        #                         'Availability':  None,
        #                         'Download':      None,
        #                         'IsOriginal':    None,
        #                         'IsAdult':       None,
        #                         'Packages':
        #                             [{'Type': 'tv-everywhere'}],
        #                         'Country':       None,
        #                         'Timestamp':     datetime.now().isoformat(),
        #                         'CreatedAt':     self._created_at
        #                     }
        #                     payload_epis.append(payload)
        #                     Datamanager._checkDBandAppend(
        #                         self, payload, ids_guardados_series, payloads, isEpi=True
        #                     )
        #                 if payload_epis:
        #                     Datamanager._checkDBandAppend(
        #                     self, payload_series, ids_guardados, payloads_series
        #                     )
        #                 break

    #     Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)
    #     Datamanager._insertIntoDB(
    #         self, payloads, self.titanScrapingEpisodios)
    #     self.sesion.close()

    #     Upload(self._platform_code, self._created_at, testing=self.testing)

    # def get_images(self,content):
    #     try:
    #         return [content['properties']['cardData']['images']]
    #     except:
    #         return None