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
        self.list_db  = Datamanager._getListDB(self, self.titanScraping)
        self.list_db_epi=Datamanager._getListDB(self, self.titanScrapingEpisodios)

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

        self.get_payload_movies(movie_data)
        self.get_payload_serie(serie_data)
        self.get_payload_episodes(episode_data)

    def get_payload_movies(self,content):
        self.payloads = []
        data = content['data']['children']
        for item in data:
            if item['properties'].get('title'):
                if 'Movies' in item['properties']['title']:
                    movies_data = item
                    break
        for movie in movies_data['children']:
            id= self.get_id(movie)
            title= self.get_title(movie)
            permalink= self.get_permalink(movie)
            sinopsis= self.get_sinopsis(movie)
            image=self.get_images(movie)
            genre=self.get_genre(movie)
            self.payload_movie(id,title,permalink,sinopsis,image,genre)
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)

    def get_payload_serie(self,content):
        self.payloads = []
        self.matchid={}
        data = content['data']['children']
        for item in data:
            if item['properties'].get('title'):
                if 'Shows' in item['properties']['title']:
                    series_data = item
                    break
        for serie in series_data['children']:
            id=self.get_id(serie)
            title=self.get_title(serie)
            self.matchid[title]=id
            permalink=self.get_permalink(serie)
            sinopsis=self.get_sinopsis(serie)
            image=self.get_images(serie)
            genre=self.get_genre(serie)
            self.payload_series(id,title,permalink,sinopsis,image,genre)
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)

    def get_payload_episodes(self,content):
        self.payloads_epis=[]
        data=content['data']['children'][2]['children']
        for serie in data:
            title=serie['properties']['title']
            for episode in serie['children']:
                id=self.get_id(episode)
                epititle=self.get_title(episode)
                serieid=self.get_serieid(title)
                permalink=self.get_permalink(episode)
                season=int(episode['properties']['cardData']['text']['seasonEpisodeNumber'].split(',')[0].replace('S',''))
                numepi=int(episode['properties']['cardData']['text']['seasonEpisodeNumber'].split(',')[1].replace('E',''))
                sinopsis=self.get_sinopsis(episode)
                image=self.get_images(episode)
            self.payload_episodes(title,id,epititle,season,numepi,permalink,sinopsis,image,serieid)
        Datamanager._insertIntoDB(self, self.payloads_epis, self.titanScrapingEpisodios)


    def payload_movie(self,id,title,permalink,sinopsis,image,genre):
        payload_contenidos = { 
            'PlatformCode':  self._platform_code, #Obligatorio   
            "Id":            id, #Obligatorio
            "Crew":          None,
            "Title":         title, #Obligatorio      
            "CleanTitle":    _replace(title), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          'movie',     #Obligatorio  #movie o serie     
            "Year":          None,     #Important!  1870 a año actual   
            "Duration":      None,     #en minutos   
            "ExternalIds":   None,    
            "Deeplinks": {
                "Web":       permalink,       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },
            "Synopsis":      sinopsis,      
            "Image":         image,      
            "Subtitles":    None,
            "Dubbed":       None,
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        genre,    #Important!      
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
        Datamanager._checkDBandAppend(self, payload_contenidos, self.list_db, self.payloads)

    def payload_series(self,id,title,permalink,sinopsis,image,genre):
        payload_contenido_series = { 
            "PlatformCode":  self._platform_code, #Obligatorio   
            "Id":            id, #Obligatorio
            "Seasons":       [ #Unicamente para series
                                None
            ],
            "Crew":          [ #Importante
                                None
            ],
            "Title":         title, #Obligatorio      
            "CleanTitle":    _replace(title), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          'serie',     #Obligatorio  #movie o serie     
            "Year":          None,     #Important!  1870 a año actual   
            "Duration":      None,      
            "ExternalIds":   None,       
            "Deeplinks": {
                "Web":       permalink,       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },
            "Synopsis":      sinopsis,      
            "Image":         image,      
            "Subtitles": None,
            "Dubbed": None,
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        genre,    #Important!      
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
        Datamanager._checkDBandAppend(self, payload_contenido_series, self.list_db, self.payloads)

    def payload_episodes(self,title,epititle,epid,season,numepi,permalink,sinopsis,image,serieid):
            payload_episodios = {      
                "PlatformCode":  self._platform_code, #Obligatorio      
                "Id":            epid, #Obligatorio
                "ParentId":      serieid, #Obligatorio #Unicamente en Episodios
                "ParentTitle":   title, #Unicamente en Episodios 
                "Episode":       numepi, #Unicamente en Episodios  
                "Season":        season, #Obligatorio #Unicamente en Episodios
                "Crew":          None, #important
                "Title":         epititle, #Obligatorio      
                "OriginalTitle": None,                          
                "Year":          None,     #Important!     
                "Duration":      None,      
                "ExternalIds":   None,     
                "Deeplinks": {          
                    "Web":       permalink,       #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },      
                "Synopsis":      sinopsis,      
                "Image":         image,     
                "Subtitles": None,
                "Dubbed": None,
                "Rating":        None,     #Important!      
                "Provider":      None,      
                "Genres":        None,    #Important!      
                "Cast":          None,    #Important!        
                "Directors":    None,    #Important!      
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
            
            Datamanager._checkDBandAppend(self, payload_episodios, self.list_db_epi, self.payloads_epis, isEpi=True)


    def get_id(self,content):
        try:
            return content['properties']['cardData']['meta']['nid']
        except:
            None

    def get_title(self,content):
        try:
            return content['properties']['cardData']['text']['title']
        except:
            None

    def get_permalink(self,content):
        try:
            return 'https://www.amc.com'+content['properties']['cardData']['meta']['permalink']
        except:
            None

    def get_sinopsis(self,content):
        try:
            return content['properties']['cardData']['text']['description']
        except:
            None

    def get_images(self,content):
        try:
            return content['properties']['cardData']['images']
        except:
            return None

    def get_genre(self,content):
        try:
            return content['properties']['cardData']['meta']['genre']
        except:
            None

    def get_serieid(self,title):
        if title in self.matchid:
            return self.matchid[title]
        else:
            None