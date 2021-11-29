import time
import requests
import re
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
    - ¿Cuanto demoró la ultima vez? -
    - ¿Cuanto contenidos trajo la ultima vez? TS:163 TSE: 960 07/10/21

    OTROS COMENTARIOS:
        Contenia series sin episodios, se modificó el script para excluirlas.

    """
    #Constructor, instancia variables
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.skippedEpis = 0
        self.skippedTitles = 0
        self.payloads_movies = []   #Inicia payloads movies vacio, luego de la primer ejecución obtenemos contenido
        self.payloads_shows = []    #Inicia payloads shows vacio, luego de la primer ejecución obtenemos contenido
        self.payloads_episodes = [] #Inicia payloads shows vacio, luego de la primer ejecución obtenemos contenido

        ################# URLS YAML#################
        self._movies_url = self._config['movie_url']
        #https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/movies?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web 
        
        self._show_url = self._config['show_url']
        #https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/shows?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web
        
        self._episode_url = self._config['episode_url']
        #https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/amc/url/episodes?cacheHash=6fbe285914ba1b125a543cb2a78a8e5d2b8bf1962737c01b1fd874e87f8dbdb9&device=web
        
        self._format_url = self._config['format_url'] 
        self.testing = False
        self.session = requests.session()
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
        
        self.session.close() #Se cierra la session
        #Upload(self._platform_code, self._created_at, testing=self.testing) #Sube a Misato -POR EL MOMENTO NO LO HACEMOS-

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

        #Se debe insertar en la BD Local cuando funcione correctamente
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

        #Se debe insertar en la BD Local cuando funcione correctamente
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
            for data in show_episode['children']:
                id = data['properties']['cardData']['meta']['nid']  
                titleepisode = data['properties']['cardData']['text']['title']          
                #VER DEEPLINK
                deeplink = self._format_url.format(data['properties']['cardData']['meta']['permalink'])
                description = data['properties']['cardData']['text']['description']       
                
                if data['properties']['cardData'].get('images'):
                    image = data['properties']['cardData']['images']
                else:
                    image = None

                season = data['properties']['cardData']['text']['seasonEpisodeNumber']
                count += 1

                self.payloadEpisodes(id, titleepisode, season, deeplink, description, image)   #Se encarga de cargar el payload con los campos correspondientes
        print("############################################\nCantidad total de episodios encontrados: "+ str(count) + "\n" + "############################################\n")

        #Se debe insertar en la BD Local cuando funcione correctamente
        #Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)

    #Los getters se encargan de extraer cada parte importante de información
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
        deeplink = content['properties']['cardData']['meta']['permalink']
        return self._format_url.format(deeplink)

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
    def payloadEpisodes(self, id, titleepisode, season, deeplink, description, image):
        payload_content = {
        "PlatformCode":  self._platform_code,
        "Id":            None,          #Falta
        "ParentId":      None,          #Falta ID de la serie a la que corresponde el episodio
        "ParentTitle":   None,          #Falta TITLE de la serie a la que corresponde el episodio
        "Episode":       season[5],     #Me paro en la posición donde se encuentra el número de episodio - Tipo de dato int -USAR MÉTODOS DE STRING-
        "Season":        season[1],     #Me paro en la posición donde se encuentra el número de temporada - Tipo de dato int -USAR MÉTODOS DE STRING-
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
        print(payload_content)  #Solo para poder ver