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
    - ¿Cuanto demoró la ultima vez? Time: 0:00:03.814766 - Actualizado: 2021/11/30
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
        #Upload(self._platform_code, self._created_at, testing=self.testing) #Sube a Misato (OJO, NO LO USAMOS TODAVÍA)

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
            title = self.getTitle(movie)    
            id = self.getId(movie)  
            type = self.getType(movie)
            deeplink = self.getDeeplink(movie)
            description = self.getDescription(movie) 
            image = self.getImage(movie)
            genre = self.getGenre(movie)
            count += 1

            self.payload(title, id, type, deeplink, description, image, genre)   #Se encarga de cargar el payload con los campos correspondientes
        print("###########################################\nCantidad total de peliculas encontradas: "+ str(count) + "\n" + "###########################################\n")

        #Se debe insertar en la BD Local (OJO, TODAVIA NO LO USAMOS)
        #Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)

    def get_payload_shows(self, content):
        data = content['data']['children']
        for item in data:                       #Este bucle filtra hasta encontrar series
            if item['properties'].get('title'):
                if 'Shows A - Z' in item['properties']['title']:
                    shows_data = item
                    break
        count = 0
        for show in shows_data['children']:   #Se para en el contenido series y las recorre para extraer la data
            title = self.getTitle(show)    
            id = self.getId(show)  
            type = self.getType(show)
            deeplink = self.getDeeplink(show)
            description = self.getDescription(show) 
            image = self.getImage(show)
            genre = None
            count += 1

            self.payload(title, id, type, deeplink, description, image, genre)   #Se encarga de cargar el payload con los campos correspondientes
        print("########################################\nCantidad total de series encontradas: "+ str(count) + "\n" + "########################################\n")

        #Se debe insertar en la BD Local (OJO, TODAVIA NO LO USAMOS)
        #Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)

    def get_payload_episodes(self, content):
        data = content['data']['children']
        for item in data:                       #Este bucle filtra hasta encontrar las series y episodios
            if item['type']:
                if 'list' in item['type']:
                    shows_episodes_data = item
                    break
        count = 0
        for show_episode in shows_episodes_data['children']:   #Se para en el contenido series y las recorre para extraer la data           
            self.parenttitle = self.getParentTitle(show_episode)
            parentid = self.getParentId(show_episode)
            for data in show_episode['children']:
                id = self.getIdEpisode(data)
                titleepisode = self.getTitleEpisode(data)        
                deeplink = self.getDeeplinkEpisode(data)
                description = self.getDescriptionEpisode(data)
                image = self.getImageEpisode(data)
                season = self.getSeasonEpisode(data)
                episode = self.getEpisodeEpisode(data)
                count += 1

                self.payloadEpisodes(id, titleepisode, parentid, self.parenttitle, season, episode, deeplink, description, image)   #Se encarga de cargar el payload con los campos correspondientes
        print("############################################\nCantidad total de episodios encontrados: "+ str(count) + "\n" + "############################################\n")

        #Se debe insertar en la BD Local (OJO, TODAVIA NO LO USAMOS)
        #Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)

    #####Los getters se encargan de extraer cada parte importante de información de episodios#####
    def getParentId(self, content):
        for id in self.payloads_movies:
            if id['Title'] == self.parenttitle:
                return id['Id']
    
    def getParentTitle(self, content):
         return content['properties']['title'] 
    
    def getIdEpisode(self, content):
        return content['properties']['cardData']['meta']['nid'] 

    def getTitleEpisode(self, content):
        return content['properties']['cardData']['text']['title']     

    def getDeeplinkEpisode(self, content):
        return self.deeplinkBase + content['properties']['cardData']['meta']['permalink']
    
    def getDescriptionEpisode(self, content):
        return content['properties']['cardData']['text']['description']     
    
    def getImageEpisode(self, content):
        if content['properties']['cardData'].get('images'):
            image = content['properties']['cardData']['images']
        else:
            image = None
        return image

    def getEpisodeEpisode(self, content):    
        contentEpisode = content['properties']['cardData']['text']['seasonEpisodeNumber']
        return int(contentEpisode.split(",")[1].strip().split("E")[1])

    def getSeasonEpisode(self, content):
        contentEpisode = content['properties']['cardData']['text']['seasonEpisodeNumber']
        return int(contentEpisode.split(",")[0].split("S")[1])

    #####Los getters se encargan de extraer cada parte importante de información de peliculas y series#####
    def getTitle(self, content):
        return content['properties']['cardData']['text']['title']

    def getId(self, content):
        return content['properties']['cardData']['meta']['nid']
    
    def getType(self, content):
        type = content['properties']['cardData']['meta']['schemaType']    
        if type == 'TVSeries':
            type = 'serie'
        elif type == 'Movie':
            type = 'movie'
        return type
    
    def getDeeplink(self, content):
        return self.deeplinkBase + content['properties']['cardData']['meta']['permalink']

    def getDescription(self, content):    
        return content['properties']['cardData']['text']['description']

    def getImage(self, content):  
       return content['properties']['cardData']['images']
        
    def getGenre(self, content):
        return content['properties']['cardData']['meta']['genre']
    
    #Se encarga de llenar el payload con la data para peliculas y series y llamar a Datamanager
    def payload(self, title, id, type, deeplink, description, image, genre):
        payload_content = { 
        "PlatformCode":  self._platform_code,               #Obligatorio   
        "Id":            id,                                #Obligatorio
        "Seasons":       None,
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
        "Packages":     {"subscription-vod"},               #Se debe colocar el Packages para que reconozca el modelo de negocio del contenido                                      
        "Country":       None,
        "Timestamp":     datetime.now().isoformat(),        #Obligatorio  Inserte fecha en formato ISO.
        "CreatedAt":     self._created_at,                  #Obligatorio  Inserta time.strftime('%Y-%m-%d') para setear fecha en createdAt.
        }
        if type == 'movie':
            Datamanager._checkDBandAppend(self, payload_content, self.list_db_movies_shows, self.payloads_movies)    #Compara el content con lo que existe en la base de datos y lo guarda en payloads
        elif type == 'serie':
            Datamanager._checkDBandAppend(self, payload_content, self.list_db_movies_shows, self.payloads_shows)    #Compara el content con lo que existe en la base de datos y lo guarda en payloads
        print(payload_content)

    #Se encarga de llenar el payload con la data para episodios y llamar a Datamanager
    def payloadEpisodes(self, id, titleepisode, parentid, parenttitle, season, episode, deeplink, description, image):
        payload_content = {
        "PlatformCode":  self._platform_code,
        "Id":            None,           
        "ParentId":      parentid,                          #ID de la serie a la que corresponde el episodio
        "ParentTitle":   parenttitle,                       #TITLE de la serie a la que corresponde el episodio
        "Episode":       episode,     
        "Season":        season,    
        'Id':            id,            
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
        'Packages':      {"subscription-vod"},
        'Country':       None,
        'Timestamp':     datetime.now().isoformat(),
        'CreatedAt':     self._created_at
        }            
        Datamanager._checkDBandAppend(self, payload_content, self.list_db_episodes, self.payloads_episodes, isEpi=True)     #Compara el content con lo que existe en la base de datos y lo guarda en payloads
        print(payload_content)