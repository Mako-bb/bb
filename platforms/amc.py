import time
import requests
from handle.replace import _replace
from common import config
from datetime import datetime
from handle.mongo import mongo
from handle.datamanager import Datamanager
from updates.upload import Upload

#Comando para correr el Script: python main.py Amc --c US --o testing

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
    - ¿Cuanto demoró la ultima vez? Time: 0:00:34.986414 - Actualizado: 2021/11/30
    - ¿Cuanto contenidos trajo la ultima vez? TS: 91 Peliculas 83 Series | TSE: 857 - Actualizado: 2021/11/30
    """
    #Constructor, instancia variables
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.skippedEpis = 0
        self.skippedTitles = 0
        self.payloads_movies = []   #Inicia payloads movies vacio, luego de la primer ejecución obtenemos contenido
        self.payloads_shows = []    #Inicia payloads shows vacio, luego de la primer ejecución obtenemos contenido
        self.idtitle_shows = []     #Se utiliza esta lista para guardar los id y titulos de las series
        self.payloads_episodes = [] #Inicia payloads episodios vacio, luego de la primer ejecución obtenemos contenido
        self.deeplinkBase = "https://www.amc.com"

        ################# URLS YAML#################
        self._movies_url = self._config['movie_url']
        #https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/movies?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web 
        
        self._show_url = self._config['show_url']
        #https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/shows?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web
        
        self._episode_url = self._config['episode_url']
        #https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/episodes?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web
        
        self._format_url = self._config['format_url'] 
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

    #Se encarga de instanciar las DB y llamar a los métodos que hacen el scraping
    def _scraping(self):
        self.list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        self.list_db_movies_shows = Datamanager._getListDB(self, self.titanScraping)
        
        # Definimos los links de las API'S
        movie_data = Datamanager._getJSON(self, self._movies_url)   #Convierte la URL en un JSON
        self.get_payload_movies(movie_data) #Le pasa como parametro el JSON y luego la función se encarga de trabajar con el mismo

        show_data = Datamanager._getJSON(self,self._show_url) #Convierte la URL en un JSON
        self.get_payload_shows(show_data)  #Le pasa como parametro el JSON y luego la función se encarga de trabajar con el mismo
        
        episode_data = Datamanager._getJSON(self, self._episode_url)
        self.get_payload_episodes(episode_data)
        
        self.sesion.close() #Se cierra la session
        Upload(self._platform_code, self._created_at, testing=self.testing) #Sube a Misato (OJO, NO LO USAMOS TODAVÍA)

    #Se encarga de filtrar y sacar contenido de las movies
    def get_payload_movies(self, content):
        data = content['data']['children']
        for item in data:                       #Este bucle filtra hasta encontrar movies
            if item['properties'].get('title'):
                if 'Movies' in item['properties']['title']:
                    movies_data = item
                    break
        
        count = 0
        for movie in movies_data['children']:   #Se para en el contenido peliculas y las recorre para extraer la data
            title = self.get_title(movie)    
            id = self.get_id(movie)  
            type = self.get_type(movie)
            deeplink = self.get_deeplink(movie)
            description = self.get_description(movie) 
            image = self.get_image(movie)
            genre = self.get_genre(movie)
            count += 1

            self.payload_movie_and_shows(title, id, type, deeplink, description, image, genre)   #Se encarga de cargar el payload con los campos correspondientes

        Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)

    def get_payload_shows(self, content):
        data = content['data']['children']
        for item in data:                       #Este bucle filtra hasta encontrar series
            if item['properties'].get('title'):
                if 'Shows A - Z' in item['properties']['title']:
                    shows_data = item
                    break
        count = 0
        for show in shows_data['children']:   #Se para en el contenido series y las recorre para extraer la data
            title = self.get_title(show)    
            id = self.get_id(show)
            self.idtitle_shows.append({"Id": id, 
                                       "Title": title,})    #Se utiliza solo para guardar los id y titulos de las series
            type = self.get_type(show)
            deeplink = self.get_deeplink(show)
            description = self.get_description(show) 
            image = self.get_image(show)
            genre = None
            count += 1

            self.payload_movie_and_shows(title, id, type, deeplink, description, image, genre)   #Se encarga de cargar el payload con los campos correspondientes

        Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)

    def get_payload_episodes(self, content):
        data = content['data']['children']
        for item in data:                       #Este bucle filtra hasta encontrar las series y episodios
            if item['type']:
                if 'list' in item['type']:
                    shows_episodes_data = item
                    break
        count = 0
        for show_episode in shows_episodes_data['children']:   #Se para en el contenido series y las recorre para extraer la data           
            parenttitle = self.get_parent_title_episode(show_episode)
            parentid = self.get_parent_id_episode(parenttitle, count)
            for data in show_episode['children']:
                id = self.get_id(data)
                titleepisode = self.get_title(data)        
                deeplink = self.get_deeplink(data)
                description = self.get_description(data)
                image = self.get_image(data)
                season = self.get_season_episode(data)
                episode = self.get_episode_episode(data)
                count += 1

                self.payload_episodes(id, titleepisode, parentid, parenttitle, season, episode, deeplink, description, image)   #Se encarga de cargar el payload con los campos correspondientes

        Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)

    #####Los getters se encargan de extraer cada parte importante de información de episodios#####
    def get_parent_id_episode(self, parenttitle, count):
        for item in self.idtitle_shows:
            if item['Title'] == parenttitle:
                return str(item['Id'])
        return "Not match" + str(count)     #Si el titulo no matchea retorna "Not match" y el número de episodio que no encuentra           
    
    def get_parent_title_episode(self, content):
         return content['properties']['title'] 

    def get_episode_episode(self, content):    
        contentEpisode = content['properties']['cardData']['text']['seasonEpisodeNumber']
        return int(contentEpisode.split(",")[1].strip().split("E")[1])

    def get_season_episode(self, content):
        contentEpisode = content['properties']['cardData']['text']['seasonEpisodeNumber']
        return int(contentEpisode.split(",")[0].split("S")[1])

    #####Los getters se encargan de extraer cada parte importante de información de peliculas y series#####
    def get_title(self, content):
        return content['properties']['cardData']['text']['title']

    def get_id(self, content):
        return str(content['properties']['cardData']['meta']['nid'])
    
    def get_type(self, content):
        type = content['properties']['cardData']['meta']['schemaType']    
        if type == 'TVSeries':
            type = 'serie'
        elif type == 'Movie':
            type = 'movie'
        return type
    
    def get_deeplink(self, content):
        return self.deeplinkBase + content['properties']['cardData']['meta']['permalink']

    def get_description(self, content):    
        return content['properties']['cardData']['text']['description']

    def get_image(self, content):
        images = []
        if content['properties']['cardData'].get('images'):
            if content['properties']['cardData']['images'] is list:
                for image in content['properties']['cardData']['images']:
                    images.append(image)
            else:
                images.append(content['properties']['cardData']['images'])
        else:
            return None
        return images
        
    def get_genre(self, content):
        genres = []
        if content['properties']['cardData']['meta'].get('genre'):
            if content['properties']['cardData']['meta']['genre'] is list:
                for genre in content['properties']['cardData']['meta']['genre']:
                    genres.append(genre)
            else:
                genres.append(content['properties']['cardData']['meta']['genre'])
        else:
            return None
        return genres

    #Se encarga de llenar el payload con la data para peliculas y series y llamar a Datamanager
    def payload_movie_and_shows(self, title, id, type, deeplink, description, image, genre):
        payload_content = { 
        "PlatformCode":  self._platform_code,               #Obligatorio   
        "Id":            id,                                #Obligatorio
        "Crew":          None,
        "Title":         title,                             #Obligatorio      
        "CleanTitle":    _replace(title),                   #Obligatorio  _replace saca los caracteres basura del title.
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
        "Packages":     [{'Type': 'subscription-vod'}],             #Se debe colocar el Packages para que reconozca el modelo de negocio del contenido                                      
        "Country":       None,
        "Timestamp":     datetime.now().isoformat(),        #Obligatorio  Inserte fecha en formato ISO.
        "CreatedAt":     self._created_at,                  #Obligatorio  Inserta time.strftime('%Y-%m-%d') para setear fecha en createdAt.
        }
        if type == 'movie':
            Datamanager._checkDBandAppend(self, payload_content, self.list_db_movies_shows, self.payloads_movies)    #Compara el content con lo que existe en la base de datos y lo guarda en payloads
        elif type == 'serie':
            payload_content["Seasons"] = None               #Campo definido solo para series
            Datamanager._checkDBandAppend(self, payload_content, self.list_db_movies_shows, self.payloads_shows)    #Compara el content con lo que existe en la base de datos y lo guarda en payloads

    #Se encarga de llenar el payload con la data para episodios y llamar a Datamanager
    def payload_episodes(self, id, titleepisode, parentid, parenttitle, season, episode, deeplink, description, image):
        payload_content = {
        "PlatformCode":  self._platform_code,
        "Id":            id,           
        "ParentId":      parentid,                          #ID de la serie a la que corresponde el episodio
        "ParentTitle":   parenttitle,                       #TITLE de la serie a la que corresponde el episodio
        "Episode":       episode,     
        "Season":        season,                
        'Title':         titleepisode,
        'OriginalTitle': titleepisode,
        'Year':          None,
        'Duration':      None,
        'Deeplinks': {
            'Web':       deeplink,
            'Android':   None,
            'iOS':       None,
        },
        'Playback':      None,
        "CleanTitle":    _replace(titleepisode),
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
        'Packages':      [{'Type': 'subscription-vod'}],
        'Country':       None,
        'Timestamp':     datetime.now().isoformat(),
        'CreatedAt':     self._created_at,
        }            
        Datamanager._checkDBandAppend(self, payload_content, self.list_db_episodes, self.payloads_episodes, isEpi=True)     #Compara el content con lo que existe en la base de datos y lo guarda en payloads