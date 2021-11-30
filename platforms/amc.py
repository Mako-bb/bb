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

class Amc():
    """
    Amc es una ott de Estados Unidos.

    DATOS IMPORTANTES:
    - Versión Final: No.
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
        self.payloads_movies = []
        self._show_url = self._config['show_url']
        self.payloads_shows = []
        self._episode_url = self._config['episode_url']
        self.payloads_episodes = []
        #Url para encontrar la información de los contenidos por separado
        self._format_url = self._config['format_url'] 
        self._episode_url = self._config['episode_url']
        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}

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
        self.list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        self.list_db_movies_shows = Datamanager._getListDB(self, self.titanScraping)
        
        # Definimos los links de las apis y con el Datamanager usamos la función _getJson
        movie_data = Datamanager._getJSON(self, self._movies_url)
        self.get_payload_movies(movie_data)
        
        serie_data = Datamanager._getJSON(self,self._show_url )
        self.get_payload_shows(serie_data) 
        
        episode_data = Datamanager._getJSON(self, self._episode_url)
        self.get_payload_episodes(episode_data)

    def get_payload_movies(self, content):
        data = content['data']['children']
        i = 0
        for item in data:
            if item['properties'].get('title'):
                if 'Movies' in item['properties']['title']:
                    movies_data = item
                    break
        for movie in movies_data['children']:
            title = self.get_title(movie)
            id = self.get_id(movie)
            type = self.get_type(movie)
            deeplink = self.get_deeplink(movie)
            description = self.get_description(movie)
            image = self.get_images(movie)
            genre = self.get_genre(movie)
            i += 1
            self.payload(title, id, type, deeplink, description, image, genre)
        #Datamanager._insertIntoDB(self, self.payload, self.titanScraping)
        print(f'Películas encontrados: {i}')

    def get_payload_shows(self, content):
        data = content['data']['children']
        i = 0
        for item in data:
            if item['properties'].get('title'):
                if 'Shows' in item['properties']['title']:
                    shows_data = item
                    break
        for show in shows_data['children']:  
            title = self.get_title(show)    
            id = self.get_id(show)  
            type = self.get_type(show)
            deeplink = self.get_deeplink(show)
            description = self.get_description(show) 
            image = self.get_images(show)
            genre = None
            i += 1
            self.payload(title, id, type, deeplink, description, image, genre)
        print(f'Series encontradas: {i}')

    def get_payload_episodes(self, content):
        data = content['data']['children']
        i = 0
        for item in data: 
            if item['type']:
                if 'list' in item['type']:
                    shows_episodes_data = item
                    break
        for show_episode in shows_episodes_data['children']:         
            parenttitle = self.get_parent_title(show_episode)
            #print(parenttitle)
            for data in show_episode['children']:
                id = self.get_id(data)
                titleEpisode = self.get_title(data)
                deeplink = self.get_deeplink(data)
                description = self.get_description(data)    
                image = self.get_images(data)
                season = self.get_season_number(data)
                episode = self.get_episode_number(data)
                i += 1
                self.payloadEpisodes(id, parenttitle, titleEpisode, season, episode, deeplink, description, image)
        print(f'Episodios encontrados: {i}')

        #Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)

    def get_title(self, content):
        title = content['properties']['cardData']['text']['title']
        return title

    def get_id(self, content):
        id = content['properties']['cardData']['meta']['nid']
        return id

    def get_type(self, content):
        type = content['properties']['cardData']['meta']['schemaType']
        if type == 'Movie':
            type = 'movie'
        elif type == 'TVSeries':
            type = 'serie'
        return type

    def get_deeplink(self, content):
        deeplink = content['properties']['cardData']['meta']['permalink']
        return self._format_url.format(deeplink)

    def get_description(self, content):
        description = content['properties']['cardData']['text']['description']
        return description

    def get_images(self, content):
        try:
            return [content['properties']['cardData']['images']]
        except:
            return None

    def get_genre(self, content):
        genre = content['properties']['cardData']['meta']['genre']
        return genre
            
    def get_parent_title(self, content):
        parenttitle = content['properties']['title']
        return parenttitle

    def get_episode_number(self, content):    
        episodeNumber = content['properties']['cardData']['text']['seasonEpisodeNumber']
        return None #usar .split()

    def get_season_number(self, content):
        seasonNumber = content['properties']['cardData']['text']['seasonEpisodeNumber']
        return None #usar .split()

    def payload(self, title, id, type, deeplink, description, image, genre):
        payload_content = { 
        "PlatformCode":  self._platform_code,               #Obligatorio   
        "Id":            id,                                #Obligatorio
        "Seasons":       None,
        "Crew":          None,
        "Title":         title,                             #Obligatorio      
        "CleanTitle":    _replace(title),                   #Obligatorio  
        "OriginalTitle": None,                          
        "Type":          type,                              #Obligatorio  #movie o serie     
        "Year":          None,                              #Important!  1870 a año actual   
        "Duration":      None,                              #en minutos   
        "ExternalIds":   None,       
        "Deeplinks": {
            "Web":       deeplink,                          #Obligatorio          
            "Android":   None,          
            "iOS":       None,      
        },
        "Synopsis":      description,      
        "Image":         image,      
        "Subtitles":     None,
        "Dubbed":        None, 
        "Rating":        None,                              #Important!      
        "Provider":      None,      
        "Genres":        genre,                             #Important!      
        "Cast":          None,                              #Important!        
        "Directors":     None,                              #Important!      
        "Availability":  None,                              #Important!      
        "Download":      None,      
        "IsOriginal":    None,                              #Important!        
        "IsAdult":       None,                              #Important!   
        "IsBranded":     None,                              #Important!   (ver link explicativo)
        "Packages":     {"subscription-vod"},                                                   
        "Country":       None,
        "Timestamp":     datetime.now().isoformat(),        #Obligatorio 
        "CreatedAt":     self._created_at,                  #Obligatorio 
        }

        #if type == 'movie':
        #    Datamanager._checkDBandAppend(self, payload_content, self.list_db_movies_shows, self.payloads_movies)
        #elif type == 'serie':
        #    Datamanager._checkDBandAppend(self, payload_content, self.list_db_movies_shows, self.payloads_shows)
        #else:
        #   None

    def payloadEpisodes(self, id, parenttitle, titleEpisode, season, episode, deeplink, description, image):
        payload_content = {
        "PlatformCode":  self._platform_code,
        "Id":            id,
        "ParentId":      None,
        "ParentTitle":   parenttitle,         
        "Episode":       episode,
        "Season":        season,          
        'Title':         titleEpisode,
        'OriginalTitle': titleEpisode,
        'Year':          None,
        'Duration':      None,
        'Deeplinks': {
            'Web':       deeplink,
            'Android':   None,
            'iOS':       None,
        },
        'Playback':      None,
        "CleanTitle":    _replace(titleEpisode),
        'Synopsis':      description,
        'Image':         image,
        'Rating':        None,
        'Provider':      None,
        'Genres':        None,
        'Cast':          None,
        'Directors':     None,
        'Availability':  None,
        'Download':      None,
        'IsOriginal':    None,
        'IsAdult':       None,
        'Packages':      {"subscription-vod"},
        'Country':       None,
        'Timestamp':     datetime.now().isoformat(),
        'CreatedAt':     self._created_at
        }

        #Datamanager._checkDBandAppend(self, payload_content, self.list_db_episodes, self.payloads_episodes, isEpi=True)

        #Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)
        #Datamanager._insertIntoDB(self, payloads, self.titanScrapingEpisodios)
        #self.sesion.close()