import time
import requests
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
        Upload(self._platform_code, self._created_at, testing = self.testing)

    def get_payload_movies(self,content):
        self.payloads = []
        data = content['data']['children']
        for item in data:
            if item['properties'].get('title'):
                if 'Movies' in item['properties']['title']:
                    movies_data = item
                    break
        for movie in movies_data['children']:
            self.payload_movie(movie)
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)

    def get_payload_serie(self,content):
        self.payloads = []
        self.matchid=[]
        data = content['data']['children']
        for item in data:
            if item['properties'].get('title'):
                if 'Shows A - Z' in item['properties']['title']:
                    series_data = item
                    break
        for serie in series_data['children']:
            self.matchid.append({'title':self.get_title(serie),'id':self.get_id(serie)})
            self.payload_series(serie)
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)

    def get_payload_episodes(self,content):
        self.payloads_epis=[]
        data=content['data']['children'][2]['children']
        for serie in data:
            title=serie['properties']['title']
            for episode in serie['children']:
                self.payload_episodes(episode, title)
        Datamanager._insertIntoDB(self, self.payloads_epis, self.titanScrapingEpisodios)

    def payload_movie(self, movie):
        payload_contenidos = { 
            'PlatformCode':  self._platform_code, #Obligatorio   
            "Id":            str(self.get_id(movie)), #Obligatorio
            "Crew":          None,
            "Title":         self.get_title(movie), #Obligatorio      
            "CleanTitle":    _replace(self.get_title(movie)), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          'movie',     #Obligatorio  #movie o serie     
            "Year":          None,     #Important!  1870 a año actual   
            "Duration":      None,     #en minutos   
            "ExternalIds":   None,    
            "Deeplinks": {
                "Web":       self.get_permalink(movie),       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },
            "Synopsis":      self.get_sinopsis(movie),      
            "Image":         self.get_images(movie),      
            "Subtitles":     None,
            "Dubbed":        None,
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        self.get_genre(movie),    #Important!      
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

    def payload_series(self,serie):
        payload_contenido_series = { 
            "PlatformCode":  self._platform_code, #Obligatorio   
            "Id":            str(self.get_id(serie)), #Obligatorio
            "Seasons":       [ #Unicamente para series
                                None
            ],
            "Crew":          [ #Importante
                                None
            ],
            "Title":         self.get_title(serie), #Obligatorio      
            "CleanTitle":    _replace(self.get_title(serie)), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          'serie',     #Obligatorio  #movie o serie     
            "Year":          None,     #Important!  1870 a año actual   
            "Duration":      None,      
            "ExternalIds":   None,       
            "Deeplinks": {
                "Web":       self.get_permalink(serie),       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },
            "Synopsis":      self.get_sinopsis(serie),      
            "Image":         self.get_images(serie),      
            "Subtitles":     None,
            "Dubbed":        None,
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        self.get_genre(serie),    #Important!      
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

    def payload_episodes(self,episode,title):
            payload_episodios = {      
                "PlatformCode":  self._platform_code, #Obligatorio      
                "Id":            str(self.get_id(episode)), #Obligatorio
                "ParentId":      str(self.get_serieid(title)), #Obligatorio #Unicamente en Episodios
                "ParentTitle":   title, #Unicamente en Episodios 
                "Episode":       int(episode['properties']['cardData']['text']['seasonEpisodeNumber'].split(',')[1].replace('E','')), #Unicamente en Episodios  
                "Season":        int(episode['properties']['cardData']['text']['seasonEpisodeNumber'].split(',')[0].replace('S','')), #Obligatorio #Unicamente en Episodios
                "Crew":          None, #important
                "Title":         self.get_title(episode), #Obligatorio      
                "OriginalTitle": None,                          
                "Year":          None,     #Important!     
                "Duration":      None,      
                "ExternalIds":   None,     
                "Deeplinks": {          
                    "Web":       self.get_permalink(episode),       #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },      
                "Synopsis":      self.get_sinopsis(episode),      
                "Image":         self.get_images(episode),     
                "Subtitles":     None,
                "Dubbed":        None,
                "Rating":        None,     #Important!      
                "Provider":      None,      
                "Genres":        self.get_genre(episode),    #Important!      
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
            
            Datamanager._checkDBandAppend(self, payload_episodios, self.list_db_epi, self.payloads_epis, isEpi=True)


    def get_id(self,content):
        return str(content['properties']['cardData']['meta']['nid'])

    def get_title(self,content):
        return content['properties']['cardData']['text']['title']

    def get_permalink(self,content):
            return 'https://www.amc.com'+content['properties']['cardData']['meta']['permalink']

    def get_sinopsis(self,content):
        try:
            return content['properties']['cardData']['text']['description']
        except:
            return None

    def get_images(self,content):
        images=[]
        try:
            if len(content['properties']['cardData']['images'])==0:
                return images
            else:
                images=content['properties']['cardData']['images'].split(',')
                return images
        except:
            return None

    def get_genre(self,content):
        genre=[]
        try:
            if content['properties']['cardData']['meta']['genre']:
                genre=content['properties']['cardData']['meta']['genre'].split(',')
            else:
                return genre
        except:
            return None

    def get_serieid(self,title):
        for item in self.matchid:
            if title== item['title']:
                return str(item['id'])