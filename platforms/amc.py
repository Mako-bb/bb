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
        self._show_url = self._config['show_url']
        self._episode_url = self._config['episode_url']
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

        self.sesion.close()
    
    # Metodos que devuelven el contenido, se pueden reutilizar en esta ocasion para varios payload
    # Separados en metodos para que la correccion de errores sea mas facil y localizada
    # Ademas, con los metodos hacemos llegar la data en su "estado final" al payload

    def get_title(self, content):
        title = content['properties']['cardData']['text']['title']
        return title

    def get_id(self, content):
        id = content['properties']['cardData']['meta']['nid']
        return str(id)

    def get_deeplink(self, content):
        deeplink = content['properties']['cardData']['meta']['permalink']
        return self._format_url.format(deeplink)

    def get_description(self, content):
        description = content['properties']['cardData']['text']['description']
        return description

    def get_images(self, content):
        try:
            images = [content['properties']['cardData']['images']]
            return images
        except:
            return None

    def get_genre(self, content):
        genres = []
        try:
            genres = [content['properties']['cardData']['meta']['genre']]
            return genres
        except:
            return None
            
    def get_parent_title(self, content):
        parent_title = content['properties']['title']
        return str(parent_title)

    def get_parent_id(self, parent_title):
        # El id de la serie no se encuentra en el contenido del episodio
        # Se recorre el payload de los shows y si el titulo de algun show coincide con el parent title,
        # Se asigna el id de esa serie al parent id
        for title in self.find_id:
            if title['title'] == parent_title:
                parent_id = title['id']
                return parent_id

    # Episodios y temporadas vienen con el formato: "S1, E1", se debe splitear el contenido y quedarse la parte que se necesite
    # Luego se debe sacar la letra que sobre porque deben ser de tipo int

    def get_season_number(self, content):
        season_number = content['properties']['cardData']['text']['seasonEpisodeNumber']
        season_number = int(season_number.split(",")[0].replace("S", ""))
        return int(season_number)

    def get_episode_number(self, content):    
        episode_number = content['properties']['cardData']['text']['seasonEpisodeNumber']
        episode_number = int(episode_number.split(",")[1].replace("E", ""))
        return int(episode_number)

    # Metodos para obtener el payload de las peliculas, las series y los episodios 
    # (basado en lo que nos mostraron en introduccion al scraping)

    def get_payload_movies(self, content):
        self.payloads_movies = []
        data = content['data']['children'] # Te posicionas al inicio para scrapear el contenido
        i = 0 # Contador para la cantidad de contenido que trae
        for item in data:
            if item['properties'].get('title'):
                if 'Movies' in item['properties']['title']: 
                    movies_data = item 
                    break
        for movie in movies_data['children']: # Se empieza a recorrer todo el contenido
            # Para cada variable se hace uso del metodo correspondiente
            # type = self.get_type(movie) se puede usar un metodo para el type
            # pero decirle directamente el tipo agiliza la velocidad
            i += 1
            # Se manda toda la data de la movie al payload y ahi se llama a los metodos
            self.payloadMovies(movie)
        Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)
        print(f'Películas encontradas: {i}')

    # Se podría usar un solo payload para series y peliculas
    # Payload peliculas 
    def payloadMovies(self, movie):
        payload_content = { 
        "PlatformCode":  self._platform_code,               #Obligatorio   
        "Id":            self.get_id(movie),                                #Obligatorio
        "Seasons":       None,
        "Crew":          None,
        "Title":         self.get_title(movie),                             #Obligatorio      
        "CleanTitle":    _replace(self.get_title(movie)),                   #Obligatorio  
        "OriginalTitle": None,                          
        "Type":          'movie',                              #Obligatorio  #movie o serie     
        "Year":          None,                              #Important!  1870 a año actual   
        "Duration":      None,                              #en minutos   
        "ExternalIds":   None,       
        "Deeplinks": {
            "Web":       self.get_deeplink(movie),                          #Obligatorio          
            "Android":   None,          
            "iOS":       None,      
        },
        "Synopsis":      self.get_description(movie),      
        "Image":         self.get_images(movie),      
        "Subtitles":     None,
        "Dubbed":        None, 
        "Rating":        None,                              #Important!      
        "Provider":      None,      
        "Genres":        self.get_genre(movie),                             #Important!      
        "Cast":          None,                              #Important!        
        "Directors":     None,                              #Important!      
        "Availability":  None,                              #Important!      
        "Download":      None,      
        "IsOriginal":    None,                              #Important!        
        "IsAdult":       None,                              #Important!   
        "IsBranded":     None,                              #Important!   (ver link explicativo)
        "Packages":     {"Type": "subscription-vod"},                                                   
        "Country":       None,
        "Timestamp":     datetime.now().isoformat(),        #Obligatorio 
        "CreatedAt":     self._created_at,                  #Obligatorio 
        }

        Datamanager._checkDBandAppend(self, payload_content, self.list_db_movies_shows, self.payloads_movies)

        #print(payload_content)

    def get_payload_shows(self, content):
        self.payloads_shows = []
        self.find_id = []
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
            # type = self.get_type(show)
            self.find_id.append({
                'title': title,
                'id': id
            })
            i += 1
            self.payloadShows(show, title, id)
        Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)
        print(f'Series encontradas: {i}')

    # Payload shows
    def payloadShows(self, show, title, id):
        payload_content = { 
        "PlatformCode":  self._platform_code,               #Obligatorio   
        "Id":            id,                                #Obligatorio
        "Seasons":       None,
        "Crew":          None,
        "Title":         title,                             #Obligatorio      
        "CleanTitle":    _replace(title),                   #Obligatorio  
        "OriginalTitle": None,                          
        "Type":          'serie',                              #Obligatorio  #movie o serie     
        "Year":          None,                              #Important!  1870 a año actual   
        "Duration":      None,                              #en minutos   
        "ExternalIds":   None,       
        "Deeplinks": {
            "Web":       self.get_deeplink(show),                          #Obligatorio          
            "Android":   None,          
            "iOS":       None,      
        },
        "Synopsis":      self.get_description(show),      
        "Image":         self.get_images(show),      
        "Subtitles":     None,
        "Dubbed":        None, 
        "Rating":        None,                              #Important!      
        "Provider":      None,      
        "Genres":        None,                             #Important!      
        "Cast":          None,                              #Important!        
        "Directors":     None,                              #Important!      
        "Availability":  None,                              #Important!      
        "Download":      None,      
        "IsOriginal":    None,                              #Important!        
        "IsAdult":       None,                              #Important!   
        "IsBranded":     None,                              #Important!   (ver link explicativo)
        "Packages":     {"Type": "subscription-vod"},                                                   
        "Country":       None,
        "Timestamp":     datetime.now().isoformat(),        #Obligatorio 
        "CreatedAt":     self._created_at,                  #Obligatorio 
        }

        Datamanager._checkDBandAppend(self, payload_content, self.list_db_movies_shows, self.payloads_shows)
        #print(payload_content)

    def get_payload_episodes(self, content):
        self.payloads_episodes = []
        data = content['data']['children']
        i = 0
        for item in data: 
            if item['type']:
                if 'list' in item['type']:
                    shows_episodes_data = item
                    break
        for show_episode in shows_episodes_data['children']:         
            parent_title = self.get_parent_title(show_episode)
            for data in show_episode['children']:
                i += 1
                self.payloadEpisodes(data, parent_title)
        Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)
        print(f'Episodios encontrados: {i}')
    
    # Payload episodios
    def payloadEpisodes(self, data, parent_title):
        payload_content = {
        "PlatformCode":  self._platform_code,
        "Id":            self.get_id(data),
        "ParentId":      self.get_parent_id(parent_title),
        "ParentTitle":   parent_title,         
        "Episode":       self.get_episode_number(data),
        "Season":        self.get_season_number(data),
        "Crew":          [ #Importante
                     {
                        "Role": None, 
                        "Name": None
                     },
        ],          
        'Title':         self.get_title(data),
        'OriginalTitle': self.get_title(data),
        'Year':          None,
        'Duration':      None,
        'Deeplinks': {
            'Web':       self.get_deeplink(data),
            'Android':   None,
            'iOS':       None,
        },
        'Playback':      None,
        "CleanTitle":    _replace(self.get_title(data)),
        'Synopsis':      self.get_description(data),
        'Image':         self.get_images(data),
        'Rating':        None,
        'Provider':      None,
        'Genres':        None,
        'Cast':          None,
        'Directors':     None,
        'Availability':  None,
        'Download':      None,
        'IsOriginal':    None,
        'IsAdult':       None,
        'Packages':      {"Type": "subscription-vod"},
        'Country':       None,
        'Timestamp':     datetime.now().isoformat(),
        'CreatedAt':     self._created_at
        }
        
        Datamanager._checkDBandAppend(self, payload_content, self.list_db_episodes, self.payloads_episodes, isEpi=True)

        #print(payload_content)

        #Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)
        #Datamanager._insertIntoDB(self, payloads, self.titanScrapingEpisodios)
        #self.sesion.close()