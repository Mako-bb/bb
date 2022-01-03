import time
import requests
import hashlib
from bs4                import BeautifulSoup as BS
from handle.replace     import _replace
from common             import config
from datetime           import datetime
from handle.mongo       import mongo
from handle.datamanager import Datamanager
from updates.upload     import Upload

#Comando para correr el Script: python main.py Shoutfactorytv --c US --o testing

class Shoutfactorytv():
    """
    Shoutfactorytv es una ott de Estados Unidos.

    DATOS IMPORTANTES:
    - Versión Final: No.
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: No.
    - ¿Usa BS4?: Si.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? 
    - ¿Cuanto contenidos trajo la ultima vez?
    """
    #Constructor, instancia variables
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.payloads_movies        = []                                                                    #Inicia payloads movies vacio, luego de la primer ejecución obtenemos contenido
        self.payloads_shows         = []                                                                    #Inicia payloads shows vacio, luego de la primer ejecución obtenemos contenido
        self.payloads_episodes      = []                                                                    #Inicia payloads episodios vacio, luego de la primer ejecución obtenemos contenido
        self.deeplink_base          = "https://www.shoutfactorytv.com"                                      #Se necesita para concatenar
        self.search_page            = "https://www.shoutfactorytv.com/videos?utf8=✓&commit=submit&q="       #Se necesita para concatenar
        self.url                    = self._config['url']                                                   #URL YAML 
        self.testing                = False
        self.sesion                 = requests.session()
        self.headers                = {"Accept": "application/json", "Content-Type": "application/json; charset=utf-8"}

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
        
        response_url = requests.get(self.url)                                                               
        if(self.verify_status_code(response_url)):                                                          
            self.get_payload_movies(response_url)                                                           
            self.get_payload_shows(response_url)                                                              
            #self.get_payload_episodes()

        self.sesion.close()
        #Sube a Misato (OJO, NO LO USAMOS TODAVÍA)
        #Upload(self._platform_code, self._created_at, testing=self.testing)

    #Se encargar de extraer información para llenar los payloads de peliculas
    def get_payload_movies(self, response):   
        links_categories_movies = self.get_categories(response, 0)                                          #Obtiene la lista con todos los links de categorias para peliculas limpias 
    
        count = 0
        for link_categorie in links_categories_movies:                                                      #Recorre cada categoria de la lista de categorias
            response_categorie = requests.get(link_categorie)                                   

            if (self.verify_status_code(response_categorie)):                                   
                soup_link_categorie = BS(response_categorie.text, 'lxml')                                   #Se trae todo el contenido de las categorias
                content_categorie = soup_link_categorie.find_all("div", class_='img-holder')                #Se queda con los tags que contienen la lista de peliculas
              
                for item in content_categorie:                                                              #Recorre cada pelicula
                    list_links = item.find_all("a")                                                         #Obtiene una lista con todos los links de las peliculas sin limpiar

                    for item in list_links:
                        deeplink = self.deeplink_base + item['href']                                        #Acá obtiene el deeplink               
                        link_search_page = self.search_page + deeplink.split("/")[3]                        #Link de la pelicula para buscar en el search
                        response_link_search = requests.get(link_search_page)                               
                        
                        count =+ 1
                        if (self.verify_status_code(response_link_search)):                                
                            soup_link_search = BS(response_link_search.text, 'lxml')                        #Se trae todo el contenido de la búsqueda con el link en el search
                            content_link_search = soup_link_search.find_all("article", class_='post')       #Se queda con los tags que contienen la información de las peliculas

                            for content in content_link_search:
                                list_movies = content.find_all("h2")                                        #Se queda con los tags que contienen los titulos de las peliculas

                                for item in list_movies:
                                    if(str(item.text).upper()                             
                                        == str(link_search_page.split("=")[3]).replace("-", " ").upper()):  #Comprueba que el titulo de la pelicula de la pagina search sea igual al del link
                                        title = content.h2.text.strip()                                     #Acá obtiene el titulo
                                        image = content.img['src'].strip()                                  #Acá obtiene el link de la imagen
                                        duration = int(content.time.text.split(" ")[1].split(":")[0])       #Acá obtiene la duración                        
                                        #Acá obtiene la descripción
                                        description = content.p.text.replace("\n", "").strip()

                                        #Acá generamos un string id_hash mediante un encodee en utf-8 y el hash hexadecial
                                        id_hash = str(title) + str(image) + str(duration)
                                        id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()

                                        #Se encarga de cargar el payload de las peliculas con los campos correspondientes
                                        self.payload_movie(id_hash, deeplink, title, image, duration, description)                                            
                        else:
                            break
            else:
                break   
        print("\n###############################################")
        print("Cantidad de contenido encontrado: " + str(count))
        print("#################################################\n")
        print("Procesando la información...")
        time.sleep(5)

        #Se encarga de insertar en al DB titanScraping el payload de peliculas 
        #Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)

    #Se encargar de extraer información para llenar los payloads de series
    def get_payload_shows(self, response):
        links_categories_shows = self.get_categories(response, 1)                                           #Obtiene la lista con todos los links de categorias para series limpias

        count = 0
        for link_categorie in links_categories_shows:                                                       #Recorre cada categoria de la lista de categorias
            response_categorie = requests.get(link_categorie)                                   

            if (self.verify_status_code(response_categorie)):                                   
                soup_link_categorie = BS(response_categorie.text, 'lxml')                                   #Se trae todo el contenido de las categorias
                content_categorie = soup_link_categorie.find_all("div", class_='img-holder')                #Se queda con los tags que contienen la lista de series
              
                for item in content_categorie:                                                              #Recorre cada serie
                    list_links = item.find_all("a")                                                         #Obtiene una lista con todos los links de las series sin limpiar

                    for item in list_links:                                                                 #Recorre cada link de serie
                        deeplink = self.deeplink_base + item['href']                                        #Acá obtiene el deeplink                  
                        response_link = requests.get(deeplink)                                              

                        count =+ 1
                        if (self.verify_status_code(response_link)):                                
                            soup_link = BS(response_link.text, 'lxml')                                      #Se trae todo el contenido de la serie
                            content_link = soup_link.find_all("div", class_='s2 info-wrap')                 #Se queda con los tags que contienen la información de las series

                            for content in content_link:                                                     
                                title = content.h1.text.strip()                                             #Acá obtiene el titulo        
                                image = content.img['src'].strip()                                          #Acá obtiene el link de la imagen
                                description = content.p.text.strip()                                        #Acá obtiene la descripción

                                #Acá generamos un string id_hash mediante un encodee en utf-8 y el hash hexadecial
                                id_hash = str(title) + str(image)
                                id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()

                                #Se encarga de cargar el payload de las series con los campos correspondientes
                                self.payload_shows(id_hash, deeplink, title, image, description)                                            
                        else:
                            break
            else:
                break
        print("\n###############################################")
        print("Cantidad de contenido encontrado: " + str(count))
        print("#################################################\n")
        print("Procesando la información...")
        time.sleep(5)

        #Se encarga de insertar en al DB titanScraping el payload de series 
        #Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)

    #Comprobación del estado de la respuesta a la petición HTTP
    def verify_status_code(self, response):     
        if response.status_code == 200:
            return True
        else:
            print("Código de estado " + str(response.status_code) + " -> ¡Fallo!")                                #Cualquier respuest que no sea 200 entra por acá
            return False    

    #Getter que extrae los links de categorias de las peliculas y series    
    def get_categories(self, response, position):   
        soup_categories = BS(response.text, 'lxml')                                                         #Se trae todo el contenido de la plataforma
        categories = soup_categories.find_all("div", class_='divRow')                                       #Se queda con los tags que contienen las categorias en una lista
        list_categories = categories[position].find_all("a")                                                #Obtiene una lista con todos los links de las categorias

        links_categories = []         
        for item in list_categories:        
             links_categories.append(self.deeplink_base + item['href'])                                      #Agrega los links de las categorías en una lista
        return links_categories

    #Se encarga de llenar el payload de peliculas con toda la información necesaria
    def payload_movie(self, id_hash, deeplink, title, image, duration, description):
        payload_movie = {
            "PlatformCode":  self._platform_code,                                                            #Obligatorio
            "Id":            id_hash,                                                                        #Obligatorio
            "Title":         title,                                                                          #Obligatorio      
            "CleanTitle":    _replace(title),                                                                #Obligatorio      
            "OriginalTitle": None,                                                                       
            "Type":          "movie",                                                                        #Obligatorio      
            "Year":          None,                                                                           #Important!     
            "Duration":      duration,                                                   
            "ExternalIds":   None,                                                   
            "Deeplinks": {                                                       
                "Web":       deeplink,                                                                       #Obligatorio          
                "Android":   None,                                                       
                "iOS":       None,                                                   
            },                                                   
            "Synopsis":      description,                                                   
            "Image":         image,                                                   
            "Rating":        None,                                                                           #Important!      
            "Provider":      None,                                                   
            "Genres":        None,                                                                           #Important!      
            "Cast":          None,                                                   
            "Directors":     None,                                                                           #Important!      
            "Availability":  None,                                                                           #Important!      
            "Download":      None,                                                                                 
            "IsOriginal":    None,                                                                           #Important!      
            "IsAdult":       None,                                                                           #Important!   
            "IsBranded":     None,                                                                           #Important!   
            "Packages":      [{"Type":"subscription-vod"}],                                                  #Obligatorio      
            "Country":       None,                                                   
            "Timestamp":     datetime.now().isoformat(),                                                     #Obligatorio      
            "CreatedAt":     self._created_at                                                                #Obligatorio
            }
        #Compara el payload_movie con lo que existe en la DB y lo guarda en payloads_movies
        #Datamanager._checkDBandAppend(self, payload_movie, self.list_db_movies_shows, self.payloads_movies)
        print(payload_movie)

    #Se encarga de llenar el payload de series con toda la información necesaria
    def payload_shows(self, id_hash, deeplink, title, image, description):
        payload_show = {
            "PlatformCode":  self._platform_code,                                                           #Obligatorio      
            "Id":            id_hash,                                                                       #Obligatorio
            "Seasons":       None,                                                                          #DEJAR EN NONE (se va a hacer al final cuando Samuel diga)
            "Title":         title,                                                                         #Obligatorio      
            "CleanTitle":    _replace(title),                                                               #Obligatorio      
            "OriginalTitle": None,                                                                      
            "Type":          "serie",                                                                       #Obligatorio      
            "Year":          None,                                                                          #Important!     
            "Duration":      None,                                                  
            "ExternalIds":   None,                                                  
            "Deeplinks": {                                                      
                "Web":       deeplink,                                                                      #Obligatorio          
                "Android":   None,                                                      
                "iOS":       None,                                                  
            },                                                  
            "Synopsis":      description,                                                  
            "Image":         image,                                                  
            "Rating":        None,                                                                          #Important!      
            "Provider":      None,                                                  
            "Genres":        None,                                                                          #Important!      
            "Cast":          None,                                                  
            "Directors":     None,                                                                          #Important!      
            "Availability":  None,                                                                          #Important!      
            "Download":      None,                                                                  
            "IsOriginal":    None,                                                                          #Important!      
            "IsAdult":       None,                                                                          #Important!   
            "IsBranded":     None,                                                                          #Important!   
            "Packages":      [{"Type":"Subscription-vod"}],                                                 #Obligatorio      
            "Country":       None,                                                  
            "Timestamp":     datetime.now().isoformat(),                                                    #Obligatorio      
            "CreatedAt":     self._created_at                                                               #Obligatorio
        }
        #Compara el payload_show con lo que existe en la DB y lo guarda en payload_shows
        #Datamanager._checkDBandAppend(self, payload_show, self.list_db_movies_shows, self.payload_shows)
        print(payload_show)