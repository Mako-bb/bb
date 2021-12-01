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
        list_db_episodes = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)
        # Definimos los links de las apis y con el Datamanager usamos la función _getJson
        episode_data = Datamanager._getJSON(self, self._episode_url)
        serie_data = Datamanager._getJSON(self,self._show_url )
        movie_data = Datamanager._getJSON(self, self._movies_url)

        #self.getPayloadMovies(movie_data)
        self.getPayloadEpisodes(serie_data)

    ##################################################################

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

    def getTitleEpisode(self, content):
        return content['properties']['title']

    def getIdEpisode(self, content):
        return content['properties']['cardData']['meta']['nid']

    def getDescriptionEpisode(self, content):
        return content['properties']['cardData']['text']['description']

    def getSeasonEpisode(self, content):
        season = content['properties']['cardData']['text']['seasonEpisodeNumber']
        return (season[-1:])[:-1]

    def getEpisodeNumber(self, content):
        return 0

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
        
    def getPayloadEpisodes(self, content):  
        payloads = []
        list_db = Datamanager._getListDB(self, self.titanScraping)
        series_data = content['data']['children'][3]['children']

        #for serie in series_data:
            #print(serie)

        for serie in series_data:
            print("Titulo: ", self.getTitleEpisode(self))
            print("Episorio: ")
            payload_episodes = {      
              "PlatformCode":  self._platform_code, #Obligatorio      
              "Id":            self.getIdEpisode(serie), #Obligatorio
              "ParentId":      "str", #Obligatorio #Unicamente en Episodios
              "ParentTitle":   "str", #Unicamente en Episodios 
              "Episode":       22, #Obligatorio #Unicamente en Episodios  
              "Season":        22, #Obligatorio #Unicamente en Episodios
              "Crew":          [ #Importante
                                 {
                                    "Role": "str", 
                                    "Name": "str"
                                 },
                                 ...
             ],
              "Title":         self.getTitleEpisode(content), #Obligatorio      
              "OriginalTitle": "str",                          
              "Year":          "int",     #Important!     
              "Duration":      "int",      
              "ExternalIds":   "list",       
              "Deeplinks": {          
                "Web":       "str",       #Obligatorio          
                "Android":   "str",          
                "iOS":       "str",      
              },      
              "Synopsis":      self.getDescriptionEpisode(content),      
              "Image":         "list",     
              "Subtitles": "list",
              "Dubbed": "list",
              "Rating":        "str",     #Important!      
              "Provider":      "list",      
              "Genres":        "list",    #Important!      
              "Cast":          "list",    #Important!        
              "Directors":     "list",    #Important!      
              "Availability":  "str",     #Important!      
              "Download":      "bool",      
              "IsOriginal":    "bool",    #Important!      
              "IsAdult":       "bool",    #Important!   
              "IsBranded":     "bool",    #Important!   (ver link explicativo)
              "Packages":      "list",    #Obligatorio      
              "Country":       "list",      
              "Timestamp":     "str", #Obligatorio      
              "CreatedAt":     "str", #Obligatorio 
            }
            Datamanager._checkDBandAppend(self, payload_episodes, list_db, payloads)
        Datamanager._insertIntoDB(self, payloads, self.titanScraping)    


        Datamanager._insertIntoDB(
            self, payloads, self.titanScrapingEpisodios)
        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=self.testing)

    def get_images(self,content):
        try:
            return [content['properties']['cardData']['images']]
        except:
            return None