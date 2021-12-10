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

        self.list_db_episodes = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)

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
        payloads_series = []
        # Definimos los links de las apis y con el Datamanager usamos la función _getJson
        episode_data = Datamanager._getJSON(self, self._episode_url)
        serie_data = Datamanager._getJSON(self,self._show_url )
        movie_data = Datamanager._getJSON(self, self._movies_url)

        self.getPayloadMovies(movie_data)
        self.getPayloadEpisodes(episode_data, serie_data)
        self.getPayloadShows(serie_data)

    ##################################################################
    ############################# MOVIES #############################

    def getTitle(self, content):
        return content['properties']['cardData']['text']['title']

    def getDescription(self, content):
        return content['properties']['cardData']['text']['description']
    
    def getId(self, content):
        return content['properties']['cardData']['meta']['nid']

    def getGenre(self, content):
        return content['properties']['cardData']['meta']['genre']

    def getDeeplink(self, content):
        return (self._format_url).format(content['properties']['cardData']['meta']['permalink'])

    ##################################################################
    ############################ EPISODES ############################
    def get_parent_title(self, content):
        try:
            return content['properties']['title']
        except:
            return None

    def get_title_episode(self, content):
        try:
            return content['properties']['cardData']['meta']['title']
        except:
            return None

    def getIdEpisode(self, content):
        try:
            return content['properties']['cardData']['meta']['nid']
        except:
            return None

    def getDescriptionEpisode(self, content):
        try:
            return content['properties']['cardData']['text']['description']
        except:
            return None

    def getEpisodeNumber(self, content):
        return 0

    def getParentTitle():
        return 0


    ##################################################################
    ############################# SHOWS ##############################
    
    def getShowTitle(self, content):
        try:
            return content['properties']['cardData']['text']['title']
        except:
            return None

    def getShowId(self, content):
        try:
            return content['properties']['cardData']['meta']['nid']
        except:
            return None
        
    def getShowPermalink(self, content):
        try:
            return content['properties']['cardData']['meta']['permalink']
        except:
            return None

    def getShowDescription(self, content):
        try:
            return content['properties']['cardData']['text']['description']
        except:
            return None

    ##################################################################

    def getPayloadMovies(self, content):
        payloads = []
        list_db = Datamanager._getListDB(self, self.titanScraping)
        data = content['data']['children']

        for item in data:
            if item['properties'].get('title'):
                if 'Movies' in item['properties']['title']:
                    movies_data = item
                    break

        for movie in movies_data['children']:
            print('Titulo: ', self.getTitle(movie))
            payloads_movie = {
                "PlatformCode":  self._platform_code,
                "Title":         self.getTitle(movie),
                "CleanTitle":    _replace(self.getTitle(movie)),
                "OriginalTitle": None,
                "Type":          "movie",
                "Year":          int(1999),
                "Duration":      None,

                "Id":            str(self.getId(movie)),
                "Deeplinks": {

                    "Web":       self.getDeeplink(movie).replace('/tve?',''),
                    "Android":   None,
                    "iOS":       None,
                },
                "Synopsis":      self.getDescription(movie),
                "Image":         self.get_images(movie),
                "Rating":        None,  # Important!
                "Provider":      None,
                "Genres":        self.getGenre(movie),  # Important!
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
            Datamanager._checkDBandAppend(self, payloads_movie, list_db, payloads)
        Datamanager._insertIntoDB(self, payloads, self.titanScraping)
        
    def get_season_from_episode(self, content):
        try:
            season = content['properties']['cardData']['text']['seasonEpisodeNumber'].split(', ')[0]
            return season
        except:
            return None

    def get_episode_number(self, content):
        try:
            season = content['properties']['cardData']['text']['seasonEpisodeNumber'].split(', ')[1]
            return season
        except:
            return None
    
    def get_parent_id_from_title(self, parent_title, show_info):
        data = show_info['data']['children']

        for item in data:
            if item['properties'].get('title'):
                if 'Shows' in item['properties']['title']:
                    shows_data = item['children']

        for show in shows_data:
            if (parent_title == self.getShowTitle(show)):
                return self.getShowId(show)

        return None



    def getPayloadEpisodes(self, content, show_info):  
        self.payloads = []
        episodes = content['data']['children'][2]['children']

        for episode in episodes:
            print("####################################################################################################################")
            parent_title = self.get_parent_title(episode)
            parent_id = self.get_parent_id_from_title(parent_title, show_info)

            print("Parent title: ", parent_title)

            for data_episode in episode['children']:
                title_episode = self.get_title_episode(data_episode)
                episode_id = self.getIdEpisode(data_episode)
                #print("           {} : {} -> {}".format(parent_id, episode_id, title_episode))
                payload_episodes = {      
                "PlatformCode":  self._platform_code, #Obligatorio      
                "Id":            str(episode_id), #Obligatorio
                "ParentId":      str(parent_id), #Obligatorio #Unicamente en Episodios
                "ParentTitle":   parent_title, #Unicamente en Episodios 
                "Episode":       self.get_episode_number(data_episode), #Obligatorio #Unicamente en Episodios  
                "Season":        self.get_season_from_episode(data_episode), #Obligatorio #Unicamente en Episodios
                "Crew":          None, 
                "Title":         title_episode, #Obligatorio      
                "OriginalTitle": parent_title,                          
                "Year":          2008,     #Important!     Año cualquiera
                "Duration":      None,      
                "ExternalIds":   None,       
                "Deeplinks": {          
                    "Web":       "str",       #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },      
                "Synopsis":      self.getDescriptionEpisode(content),      
                "Image":         None,     
                "Subtitles":     None,
                "Dubbed":        None,
                "Rating":        None,     #Important!      
                "Provider":      None,      
                "Genres":        None,    #Important!      NO TIENE GENERO EPISODES
                "Cast":          None,    #Important!        
                "Directors":     None,    #Important!      
                "Availability":  None,     #Important!      
                "Download":      None,      
                "IsOriginal":    None,    #Important!      
                "IsAdult":       None,    #Important!   
                "IsBranded":     None,    #Important!   (ver link explicativo)
                "Packages":      "hola",    #Obligatorio      
                "Country":       [],      
                "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                "CreatedAt":     self._created_at, #Obligatorio 
                }
                Datamanager._checkDBandAppend(self, payload_episodes, self.list_db_episodes, self.payloads, isEpi=True)
        Datamanager._insertIntoDB(self, self.payloads, self.titanScrapingEpisodios)    

        self.sesion.close()

        #Upload(self._platform_code, self._created_at, testing=self.testing)

    def getPayloadShows(self, content):
        payloads = []
        list_db = Datamanager._getListDB(self, self.titanScraping)
        data = content['data']['children']

        for item in data:
            if item['properties'].get('title'):
                if 'Shows' in item['properties']['title']:
                    shows_data = item['children']

        for show in shows_data:
            payload_shows = { 
                "PlatformCode":  self._platform_code, #Obligatorio   
                "Id":            self.getShowId(show), #Obligatorio
                "Seasons":       [ #Unicamente para series
                                    None
                ],
                "Crew":          [ #Importante
                                    None
                ],
                "Title":         self.getShowTitle(show), #Obligatorio      
                "CleanTitle":    _replace(self.getShowTitle(show)), #Obligatorio      
                "OriginalTitle": None,                          
                "Type":          'serie',     #Obligatorio  #movie o serie     
                "Year":          None,     #Important!  1870 a año actual   
                "Duration":      None,      
                "ExternalIds":   None,       
                "Deeplinks": {
                    "Web":       self.getShowPermalink(show),       #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },
                "Synopsis":      self.getShowDescription(show),      
                "Image":         self.get_images(show),      
                "Subtitles": None,
                "Dubbed": None,
                "Rating":        None,     #Important!      
                "Provider":      None,      
                "Genres":        [],    #Important!      NO TIENE GENERO SHOWS
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
            Datamanager._checkDBandAppend(self, payload_shows, list_db, payloads)
        Datamanager._insertIntoDB(self, payloads, self.titanScraping)    

    def get_images(self,content):
        try:
            return [content['properties']['cardData']['images']]
        except:
            return None