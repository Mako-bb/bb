import time
import requests
import hashlib
from bs4                            import BeautifulSoup as BS
from handle.replace                 import _replace
from common                         import config
from datetime                       import datetime
from handle.mongo                   import mongo
from handle.datamanager             import Datamanager
from updates.upload                 import Upload

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
        self.payloads_movies        = []                                                                    
        self.payloads_shows         = []                                                                    
        self.payloads_episodes      = []                                                                    
        self.url_base               = "https://www.shoutfactorytv.com"                                      
        self.url_search             = "https://www.shoutfactorytv.com/videos?utf8=✓&commit=submit&q="
        self.url_search_page        = "https://www.shoutfactorytv.com/videos?page="
        self.url                    = self._config['url']                                                    
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

    #Se encarga de instanciar las listas de la DB y llamar a los métodos que hacen el Scraping
    def _scraping(self):
        self.list_db_movies_shows = Datamanager._getListDB(self, self.titanScraping)
        self.list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        
        response_url = self.verify_status_code(self.url)                                                               
        if response_url.status_code == 200:                                                          
            print("Código de estado " + str(response_url.status_code))   
            #self.get_payload_movies(response_url)                                                           
            self.get_payload_shows(response_url)                                                              
        else:
            print("Código de estado " + str(response_url.status_code) + " -> ¡Fallo!")

        self.sesion.close()
        #Sube a Misato
        #Upload(self._platform_code, self._created_at, testing=self.testing)

    #Se encarga de extraer información de peliculas
    def get_payload_movies(self, response):   
        links_categories_movies = self.get_categories(response, 0)                                          #Obtiene la lista con todos los links de categorias para peliculas limpias 
        
        count = 0
        list_title = []
        for link_categorie in links_categories_movies:                                                      #Recorre cada categoria de la lista de categorias
            response_categorie = self.verify_status_code(link_categorie)                                   

            if response_categorie.status_code == 200:
                print("Código de estado " + str(response_categorie.status_code))                                   
                soup_link_categorie = BS(response_categorie.text, 'lxml')                                   #Se trae todo el contenido de las categorias
                content_categorie = soup_link_categorie.find_all("div", class_='img-holder')                #Se queda con los tags que contienen la lista de peliculas
              
                for item in content_categorie:                                                              #Recorre cada pelicula
                    list_links = item.find_all("a")                                                         #Obtiene una lista con todos los links de las peliculas sin limpiar

                    for item in list_links:
                        deeplink = self.url_base + item['href']                                             #Acá obtiene el deeplink               

                        if int(len(item['href'].split("/"))) == 3:                                          #Link de la pelicula para buscar en el search
                            link_search_page = self.url_search + item['href'].split("/")[2]                
                        elif int(len(item['href'].split("/"))) == 4:
                            link_search_page = self.url_search + item['href'].split("/")[3]

                        response_link_search = self.verify_status_code(link_search_page)                              
                        
                        if response_link_search.status_code == 200:                                
                            print("Código de estado " + str(response_link_search.status_code))
                            soup_link_search = BS(response_link_search.text, 'lxml')                        #Se trae todo el contenido de la búsqueda con el link en el search
                            content_link_search = soup_link_search.find_all("article", class_='post')       #Se queda con los tags que contienen la información de las peliculas

                            for content in content_link_search:
                                content_link_movie = content.find("a")['href']                              #Se queda con los tags que contienen parte del link de las peliculas

                                if content_link_movie not in list_title:                                    #Acá corrobora que el link de la pelicula no este duplicado
                                    if content_link_movie == item['href']:                                  #Acá corrobora que sea igual al link de la pagina principal de la categoria                                                          
                                        list_title.append(content_link_movie)                               #Agrega a la lista para comparar     
                                        
                                        title = content.h2.text.strip()                                     #Acá obtiene el titulo
                                        
                                        image = content.img['src'].strip()                                  #Acá obtiene el link de la imagen
                                        
                                        duration = int(content.time.text.split(" ")[1].split(":")[0])       #Acá obtiene la duración                        
                                        
                                        description = content.p.text.replace("\n", "").strip()              #Acá obtiene la descripción

                                        id_hash = str(deeplink) + str(title)        
                                        id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()          #Acá generamos hash para el id (no es posible acceder conseguirlo desde la plataforma)
                                        count += 1

                                        self.payload_movies(id_hash,                                        #Se encarga de cargar el payload de las peliculas con los campos correspondientes
                                                            deeplink, 
                                                            title, 
                                                            image, 
                                                            duration, 
                                                            description)
                                        break  
                                                                                                            #Cuando consigue que los links sean iguales corta, así se reduce el tiempo de ejecución
                        else:
                            print("Código de estado " + str(response_link_search.status_code) + " -> ¡Fallo!")
                            continue
            else:
                print("Código de estado " + str(response_categorie.status_code) + " -> ¡Fallo!")
                continue   
        print(f"\033[33m\n##################################### \033[0m")
        print(f"\033[33mCantidad de contenido encontrado: \033[0m" + str(count))
        print(f"\033[33m#####################################\n \033[0m")
        
        #Se encarga de insertar en al DB local el payload de peliculas 
        #Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)

    #Se encarga de extraer información de series
    def get_payload_shows(self, response):        
        links_categories_shows = self.get_categories(response, 1)                                           #Obtiene la lista con todos los links de categorias para series limpias
        
        count = 0
        list_title = []      
        for link_categorie in links_categories_shows:                                                       #Recorre cada categoria de la lista de categorias
            response_categorie = self.verify_status_code(link_categorie)                                   

            if response_categorie.status_code == 200: 
                print("Código de estado " + str(response_categorie.status_code))                                  
                soup_link_categorie = BS(response_categorie.text, 'lxml')                                   #Se trae todo el contenido de las categorias
                content_categorie = soup_link_categorie.find_all("div", id='tab1')                          #Se queda con los tags que contienen la lista de series
                
                for item in content_categorie:                                                              #Recorre cada serie
                    list_links = item.find_all("a")                                                         #Obtiene una lista con todos los links de las series sin limpiar

                    for link in list_links:                                                                 #Recorre cada link de serie
                        deeplink = self.url_base + link['href']                                             #Acá obtiene el deeplink                                                                                                                       
                        deeplink_search_show = self.url_search_page + "1&q=" + link['href'].split("/")[2].replace("-series", "")   #Acá obtiene el deeplink de la serie para buscar en el search
                        response_link = self.verify_status_code(deeplink)                        
                        
                        if response_link.status_code == 200:
                            print("Código de estado " + str(response_link.status_code))
                            soup_link = BS(response_link.text, 'lxml')                                      #Se trae todo el contenido de la serie
                            content_link = soup_link.find("div", class_='s1')                               #Se queda con los tags que contienen la información de las series
                        
                            if content_link.img:                                                            #Acá corrobora que exista el atributo title                                                                         
                                if content_link.img['title'] not in list_title:                             #Acá corrobora que el titulo de la serie no este duplicado
                                    list_title.append(content_link.img['title'].strip())                    #Agrega a la lista para comparar     
                                    
                                    title = content_link.img['title'].strip()                               #Acá obtiene el titulo                                                                     

                                    if content_link.img.get('src'):                     
                                        image = content_link.img['src'].strip()                             #Acá obtiene el link de la imagen                        
                                    else:                   
                                        image = None
                                    
                                    content_descr = soup_link.find("div", id='info-slide-content')          #Se queda con los tags que contienen la información de la descripción de series                                                                                                     
                                    if content_descr:                                                       #Acá obtiene la descripción              
                                        if content_descr.div != None:   
                                            description = content_descr.div.text.strip()    
                                        elif content_descr.p != None:   
                                            description = content_descr.p.text.strip()  
                                    else:   
                                        description = None  

                                    id_hash = str(title) + str(deeplink)                                                                                  
                                    id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()              #Acá generamos hash para el id (no es posible acceder conseguirlo desde la plataforma)                          
                                    count += 1

                                    self.payload_shows(id_hash,                                             #Se encarga de cargar el payload de las series con los campos correspondientes
                                                        deeplink,   
                                                        title,  
                                                        image,          
                                                        description)

                                    print(f"\033[33m\n##################################### \033[0m")
                                    print(f"\033[33mComienza Scraping de episodios... \033[0m")
                                    print(f"\033[33m#####################################\n \033[0m")
                                    self.get_payload_episodes(soup_link,                                    #Acá obtiene el contenido para extraer información de los episodios y el deeplink                                                                                                   
                                                                deeplink_search_show, 
                                                                id_hash, 
                                                                title)
                            else:                                                                                                         
                                title_optional = str(link['href'].split("/")[2].replace("-", " ")).title()  #Titulo que se obtiene desde el link

                                if title_optional not in list_title:
                                    list_title.append(title_optional) 

                                    title = title_optional                                                  #Acá obtiene el titulo
                                    image = None                                                            #Acá setea el link de la imagen en None
                                    description = None                                                      #Acá setea la descripción en None

                                    id_hash = str(title) + str(deeplink)    
                                    id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()              #Acá generamos hash para el id (no es posible acceder conseguirlo desde la plataforma)                          
                                    count += 1   
                                    
                                    self.payload_shows(id_hash,                                             #Se encarga de cargar el payload de las series con los campos correspondientes
                                                        deeplink,   
                                                        title, 
                                                        image,          
                                                        description)

                                    print(f"\033[33m\n##################################### \033[0m")
                                    print(f"\033[33mComienza Scraping de episodios... \033[0m")
                                    print(f"\033[33m#####################################\n \033[0m")
                                    self.get_payload_episodes(soup_link,                                    #Acá obtiene el contenido para extraer información de los episodios y el deeplink                                                                                                   
                                                                deeplink_search_show, 
                                                                id_hash, 
                                                                title)     
                        else:
                            print("Código de estado " + str(response_link.status_code) + " -> ¡Fallo!")
                            continue
            else:
                print("Código de estado " + str(response_categorie.status_code) + " -> ¡Fallo!")
                continue
        print(f"\033[33m\n##################################### \033[0m")
        print(f"\033[33mCantidad de contenido encontrado: \033[0m" + str(count))
        print(f"\033[33m#####################################\n \033[0m")
        
        #Se encarga de insertar en al DB local el payload de series 
        #Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)

    #Se encarga de extraer información de episodios 
    def get_payload_episodes(self, soup_link, deeplink_search_show, parent_id, parent_title):
        content_episodes_page = soup_link.find("div", id='tab0')                                            #Obtiene todo el contenido de la serie

        count = 0
        for content_episode in content_episodes_page.find_all("a"):                                         #Recorre todos el contenido que contiene el tag 
            deeplink = self.url_base + content_episode['href']                                              #Acá obtiene el deeplink

            title = content_episode.img['title'].strip()                                                    #Acá obtiene el titulo           

            image = content_episode.img['src'].strip()                                                      #Acá obtiene el link de la imagen           

            season = content_episode.find_all('span')[1].text.strip().split(",")[0].split(":")[1].strip()   #Acá obtiene el número de temporada            
            
            episode = content_episode.find_all('span')[1].text.strip().split(",")[1].split(":")[1].strip()  #Acá obtiene el número de episodio            

            id_hash = str(title) + str(deeplink)    
            id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()                                      #Acá generamos hash para el id (no es posible acceder conseguirlo desde la plataforma)                          
            
            print("\nDATA:")
            print(deeplink)
            print(title)
            print(image)
            print(season)
            print(episode)
            print(id_hash)
            print(parent_id)
            print(parent_title)

            deeplink_search_episode = deeplink_search_show + content_episode['href']                        #Acá guarda el link para ingresar en el search
            
            response_deeplink_search_episode = self.verify_status_code(deeplink_search_episode)            
            if response_deeplink_search_episode.status_code == 200:                                                 
                soup_link_search = BS(response_deeplink_search_episode.text, 'lxml')                        #Extrae todo el contenido del link del episodio en el search
                content_link_search_episode = soup_link_search.find_all("article", page='1')                

                for content in content_link_search_episode:                                                 
                    if content.a['href'] == content_episode['href']:                                        #Nos aseguramos de extraer la información que corresponde a ese episodio 
                        duration = int(content.time.text.split(" ")[1].split(":")[0])                       #Acá obtiene la duración

                        description = content.p.text.strip()                                                #Acá obtiene la descripción
                        count += 1

                        print("\nMÁS DATA:")
                        print(duration)
                        print(description)
                        print("\n")

                        '''self.payload_episodes(id_hash,                                                         #Se encarga de cargar el payload de los episodios con los campos correspondientes
                                            deeplink,   
                                            title, 
                                            image,
                                            duration,          
                                            description,
                                            season, 
                                            episode,
                                            parent_id,
                                            parent_title)'''
                    else:
                        duration = None                                                                     #Acá setea la duración en None
                        description = None                                                                  #Acá setea la descripción en None
                        count += 1
                        
                        '''self.payload_episodes(id_hash,                                                         #Se encarga de cargar el payload de los episodios con los campos correspondientes
                                            deeplink,   
                                            title, 
                                            image,
                                            duration,          
                                            description,
                                            season,
                                            episode,
                                            parent_id,
                                            parent_title)'''
            else:
                print("Código de estado " + str(response_deeplink_search_episode.status_code) + " -> ¡Fallo!")
                continue
        print(f"\033[33m\n################################################ \033[0m")
        print(f"\033[33mCantidad de episodios encontrados en esta serie: \033[0m" + str(count))
        print(f"\033[33m################################################\n \033[0m")

        #Se encarga de insertar en al DB local el payload de episodios 
        #Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScraping)

    #Comprobación del estado de la respuesta a la petición HTTP
    def verify_status_code(self, link):        
        response_link = requests.get(link)
        
        if response_link.status_code == 200:
            return response_link
        else:            
            count = 0
            while count != 10 and response_link.status_code != 200:                                         #Comprueba que el código de estado sea 200, de lo contrario intenta 10 veces obtener la response
                response_link = requests.get(link)
                count += 1
                print("Intentando ingresar al sitio... Código de estado " + str(response_link.status_code))
            return response_link

    #Extrae los links de categorias de las peliculas y series    
    def get_categories(self, response, position):   
        soup_categories = BS(response.text, 'lxml')                                                         #Se trae todo el contenido de la plataforma
        categories = soup_categories.find_all("div", class_='divRow')                                       #Se queda con los tags que contienen las categorias en una lista
        list_categories = categories[position].find_all("a")                                                #Obtiene una lista con todos los links de las categorias
        
        links_categories = []         
        for item in list_categories:        
             links_categories.append(self.url_base + item['href'])                                          #Agrega los links de las categorías en una lista
        return links_categories

    #Se encarga de llenar el payload de peliculas con toda la información
    def payload_movies(self, id_hash, deeplink, title, image, duration, description):
        payload_movie = {
            "PlatformCode":  self._platform_code,                                                           #Obligatorio
            "Id":            id_hash,                                                                       #Obligatorio
            "Title":         title,                                                                         #Obligatorio      
            "CleanTitle":    _replace(title),                                                               #Obligatorio      
            "OriginalTitle": None,                                                                      
            "Type":          "movie",                                                                       #Obligatorio      
            "Year":          None,                                                                          #Important!     
            "Duration":      duration,                                                  
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
        #Compara el payload_movie con lo que existe en la DB y lo guarda en payloads_movies
        #Datamanager._checkDBandAppend(self, payload_movie, self.list_db_movies_shows, self.payloads_movies)
        #print(payload_movie)

    #Se encarga de llenar el payload de series con toda la información
    def payload_shows(self, id_hash, deeplink, title, image, description):
        payload_show = {
            "PlatformCode":  self._platform_code,                                                           #Obligatorio      
            "Id":            id_hash,                                                                       #Obligatorio
            "Seasons":       None,                                                                          #Dejar en None (se va a hacer al final cuando Samuel diga)
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
        #print(payload_show)

    #Se encarga de llenar el payload de series con toda la información
    def payload_episodes(self, id_hash, deeplink, title, image, duration, description, season, episode, parent_id, parent_title):
        payload_episode = {
            "PlatformCode":  self._platform_code,                                                           #Obligatorio      
            "Id":            id_hash,                                                                       #Obligatorio
            "ParentId":      parent_id,                                                                     #Obligatorio #Unicamente en Episodios
            "ParentTitle":   parent_title,                                                                  #Unicamente en Episodios 
            "Episode":       episode,                                                                       #Obligatorio #Unicamente en Episodios  
            "Season":        season,                                                                        #Obligatorio #Unicamente en Episodios
            "Title":         title,                                                                         #Obligatorio           
            "OriginalTitle": None,                                                                                                         
            "Year":          None,                                                                          #Important!     
            "Duration":      duration,                                                                               
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
            "CreatedAt":     self._created_at,                                                              #Obligatorio
        }
        #Compara el payload_episode con lo que existe en la DB y lo guarda en payload_episodes
        #Datamanager._checkDBandAppend(self, payload_episode, self.self.list_db_episodes, self.payloads_episodes)
        #print(payload_episode)