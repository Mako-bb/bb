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
        print("\x1b[1;36;40mInicia el Scraping >>> \x1b[0m")
        self.list_db_movies_shows = Datamanager._getListDB(self, self.titanScraping)
        self.list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        
        response_url = self.verify_status_code(self.url)                                                               
        if response_url.status_code == 200:                                                          
            print("\x1b[1;32;40mCódigo de estado >>> \x1b[0m" + str(response_url.status_code))   
            self.get_payload_movies(response_url)                                                           
            self.get_payload_shows(response_url)                                                              
        else:
            print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_url.status_code))

        self.sesion.close()
        #Sube a Misato
        #Upload(self._platform_code, self._created_at, testing=self.testing)

    #Se encarga de extraer información de peliculas
    def get_payload_movies(self, response):   
        print(f"\033[33mComienza Scraping de peliculas <<< \033[0m")
        links_categories_movies = self.get_categories(response, 0)                                          #Obtiene la lista con todos los links de categorias para peliculas limpias 
        
        count = 0
        list_links_movies = []
        for link_categorie in links_categories_movies:                                                      #Recorre cada categoria de la lista de categorias
            response_categorie = self.verify_status_code(link_categorie)
            print("\x1b[1;35;40mCategoria >>> \x1b[0m" + link_categorie)

            if response_categorie.status_code == 200:
                print("\x1b[1;32;40mCódigo de estado >>> \x1b[0m" + str(response_categorie.status_code))                                   
                soup_link_categorie = BS(response_categorie.text, 'lxml')                                   #Se trae todo el contenido de las categorias
                content_categorie = soup_link_categorie.find_all("div", class_='img-holder')                #Se queda con los tags que contienen la lista de peliculas
              
                for item in content_categorie:                                                              #Recorre cada contenido de peliculas
                    list_links = item.find_all("a")                                                         #Obtiene una lista con todos los links de las peliculas sin limpiar

                    for link in list_links:
                        ###ACÁ OBTIENE EL DEEPLINK PARA LAS PELICULAS###
                        deeplink = self.url_base + link['href']                                                       
                        link_search_page = self.clear_link_search_movie(link['href'])                       #Link de la pelicula para buscar en el search
                        response_link_search = self.verify_status_code(link_search_page)                              
                        
                        if response_link_search.status_code == 200:                                
                            print("\x1b[1;32;40mCódigo de estado >>> \x1b[0m" + str(response_link_search.status_code))
                            soup_link_search = BS(response_link_search.text, 'lxml')                        #Se trae todo el contenido de la búsqueda con el link en el search
                            content_link_search = soup_link_search.find_all("article", class_='post')       #Se queda con los tags que contienen la información de las peliculas

                            for content in content_link_search:
                                link_movie = content.find("a")['href']                                      #Se queda con los tags que contienen parte del link de las peliculas

                                if link['href'] not in list_links_movies:                                   #Acá corrobora que el link de la pelicula no este duplicado
                                    if link['href'] in link_movie:                                          #Nos aseguramos de extraer la información que corresponde a esa pelicula              
                                        list_links_movies.append(link['href'])                              #Agrega a la lista para comparar     
                                        
                                        ###ACÁ OBTIENE LA DATA PARA LAS PELICULAS###
                                        title = content.h2.text.strip()                                     
                                        image = content.img['src'].strip()                                  
                                        duration = int(content.time.text.split(" ")[1].split(":")[0])                               
                                        description = content.p.text.replace("\n", "").strip()              
                                        id_hash = self.generate_id_hash(title, deeplink)                    
                                        count += 1

                                        self.payload_movies(id_hash, deeplink, 
                                                            title, image, 
                                                            duration, description)

                                        print(f"\033[33mPelicula encontrada >>> \033[0m" + title)          
                                        break                                                               #Cuando consigue que los links sean iguales corta, así se reduce el tiempo de ejecución                                                                                                   
                                    else:
                                        print("\x1b[1;31;40m¡Pelicula no encontrada! >>> \x1b[0m" + deeplink)
                                else:
                                    print("\x1b[1;31;40m¡Pelicula repetida! >>> \x1b[0m" + deeplink)
                                    break
                        else:
                            print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_link_search.status_code))
            else:
                print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_categorie.status_code))
        print(f"\033[33mCantidad de peliculas encontradas >>> \033[0m" + str(count))
        
        #Se encarga de insertar en al DB local el payload de peliculas 
        #Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)

    #Se encarga de extraer información de series
    def get_payload_shows(self, response):
        print(f"\033[33mComienza Scraping de series <<< \033[0m")        
        links_categories_shows = self.get_categories(response, 1)                                           #Obtiene la lista con todos los links de categorias para series limpias
        
        count = 0
        count_total_episodes = 0
        list_title = []      
        for link_categorie in links_categories_shows:                                                       #Recorre cada categoria de la lista de categorias
            response_categorie = self.verify_status_code(link_categorie)
            print("\x1b[1;35;40mCategoria >>> \x1b[0m" + link_categorie)                                 

            if response_categorie.status_code == 200: 
                print("\x1b[1;32;40mCódigo de estado >>> \x1b[0m" + str(response_categorie.status_code))                                  
                soup_link_categorie = BS(response_categorie.text, 'lxml')                                   #Se trae todo el contenido de las categorias
                content_categorie = soup_link_categorie.find_all("div", id='tab1')                          #Se queda con los tags que contienen la lista de series
                
                for item in content_categorie:                                                              #Recorre cada serie
                    list_links = item.find_all("a")                                                         #Obtiene una lista con todos los links de las series sin limpiar

                    for link in list_links:
                        ###ACÁ OBTIENE EL DEEPLINK PARA LAS SERIES###                                       
                        deeplink = self.url_base + link['href']                                                                                                                                                                
                        deeplink_search_show = self.url_search_page + "1&q=" + link['href'].split("/")[2].replace("-series", "")   #Acá obtiene el deeplink de la serie para buscar en el search
                        response_link = self.verify_status_code(deeplink)                        
                        
                        if response_link.status_code == 200:
                            print("\x1b[1;32;40mCódigo de estado >>> \x1b[0m" + str(response_link.status_code))
                            soup_link = BS(response_link.text, 'lxml')                                      #Se trae todo el contenido de la serie
                            content_link = soup_link.find("div", class_='s1')                               #Se queda con los tags que contienen la información de las series
                        
                            if content_link.img:                                                            #Acá corrobora que exista el atributo title                                                                         
                                title = content_link.img['title']                                                            
                                if title not in list_title:                                                 #Acá corrobora que el titulo de la serie no este duplicado
                                    list_title.append(title.strip())                                        #Agrega a la lista para comparar     
                                    
                                    ###ACÁ OBTIENE LA DATA PARA LAS SERIES###
                                    title = title.strip()                                                                                                                        
                                    image = self.clear_image_show(content_link)                             
                                    content_descr = soup_link.find("div", id='info-slide-content')
                                    description = self.clear_description_show(content_descr)                                                                                                
                                    id_hash = self.generate_id_hash(title, deeplink)                        
                                    count += 1
                                else:
                                    print("\x1b[1;31;40m¡Serie repetida! >>> \x1b[0m" + deeplink)
                            else:                                                                                                         
                                title_optional = str(link['href'].split("/")[2].replace("-", " ")).title()  #Titulo que se obtiene desde el link cuando no es accesible desde la pagina

                                if title_optional not in list_title:
                                    list_title.append(title_optional) 

                                    ###ACÁ OBTIENE LA DATA PARA LAS SERIES###
                                    title = title_optional                                                  
                                    image = None                                                            
                                    description = None                                                      
                                    id_hash = self.generate_id_hash(title, deeplink)                        
                                    count += 1   
                                else:
                                    print("\x1b[1;31;40m¡Serie repetida! >>> \x1b[0m" + deeplink)

                            self.payload_shows(id_hash, deeplink,   
                                                                title, image,          
                                                                description)
    
                            print(f"\033[33mSerie encontrada >>> \033[0m" + title)
    
                            count_episodes = self.get_payload_episodes(soup_link, deeplink_search_show, 
                                                        id_hash, title)
                            count_total_episodes += count_episodes                                          #Incrementa la cantidad de episodios por cada serie            
                        else:
                            print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_link.status_code))   
            else:
                print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_categorie.status_code))
        print(f"\033[33mCantidad de series encontradas >>> \033[0m" + str(count))
        print(f"\033[33mCantidad de episodios encontrados en todas las series >>> \033[0m" + str(count_total_episodes))
        
        #Se encarga de insertar en al DB local el payload de series 
        #Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)

    #Se encarga de extraer información de episodios 
    def get_payload_episodes(self, soup, show, parent_id, parent_title):        
        print(f"\033[33mComienza Scraping de episodios <<< \033[0m")

        tab_increment = 0
        count = 0
        while True:
            content_episodes_page = soup.find("div", id='tab' + str(tab_increment))
            if content_episodes_page != None:                                                                       #Obtiene todo el contenido de la serie   

                for content_episode in content_episodes_page.find_all("a"):                                         #Recorre todos el contenido que contiene el tag 
                    ###ACÁ OBTIENE LA DATA PARA LOS EPISODIOS###
                    deeplink = self.url_base + content_episode['href']                                              
                    title = self.clear_title_episode(content_episode)                                               
                    image = content_episode.img['src'].strip()                                                      
                    season = content_episode.find_all('span')[1].text.split(",")[0].split(":")[1].strip()              
                    episode = content_episode.find_all('span')[1].text.split(",")[1].split(":")[1].strip()                  
                    id_hash = self.generate_id_hash(title, deeplink)                                                

                    deeplink_search_episode = show + content_episode['href']                                        #Acá guarda el link del episodio para ingresar en el search                    
                    response_deeplink_search_episode = self.verify_status_code(deeplink_search_episode)            
                    
                    if response_deeplink_search_episode.status_code == 200:                                                 
                        soup_link_search = BS(response_deeplink_search_episode.text, 'lxml')                        #Extrae todo el contenido del link del episodio en el search
                        content_link_search_episode = soup_link_search.find_all("article", page='1')                

                        for content in content_link_search_episode:                                                 
                            if content.a['href'] in content_episode['href']:                                        #Nos aseguramos de extraer la información que corresponde a ese episodio 
                                ###ACÁ OBTIENE LA DATA PARA LOS EPISODIOS###
                                duration = int(content.time.text.split(" ")[1].split(":")[0])                       
                                description = content.p.text.strip()                                              
                                count += 1
                            else:
                                ###ACÁ OBTIENE LA DATA PARA LOS EPISODIOS###
                                duration = None                                                                     
                                description = None                                                                  
                                count += 1
                         
                            self.payload_episodes(id_hash, deeplink, title, 
                                                image, duration, description, 
                                                season, episode, parent_id, 
                                                parent_title)

                            print(f"\033[33mEpisodio encontrado >>> \033[0m" + title)
                    else:
                        print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_deeplink_search_episode.status_code))
                tab_increment += 1
            else:
                break
        print(f"\033[33mCantidad de episodios encontrados en: \033[0m" + parent_title + " = " + str(count))
        return count                                                                                                #Cantidad de episodios en una serie

        #Se encarga de insertar en al DB local el payload de episodios 
        #Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScraping)

    #Comprobación del estado de la respuesta a la petición HTTP (intenta 10 veces)
    def verify_status_code(self, link):        
        response_link = requests.get(link)
        
        if response_link.status_code == 200:
            return response_link
        else:            
            count = 0
            while count != 10 and response_link.status_code != 200:                                        
                response_link = requests.get(link)
                count += 1
                print("\x1b[1;32;40mIntentando ingresar al sitio, código de estado >>> \x1b[0m" + str(response_link.status_code))
            return response_link

    #Extrae los links de todas categorias (peliculas y series)    
    def get_categories(self, response, position):   
        soup_categories = BS(response.text, 'lxml')                                                         
        categories = soup_categories.find_all("div", class_='divRow')                                       
        list_categories = categories[position].find_all("a")                                                
        
        links_categories = []         
        for item in list_categories:        
             links_categories.append(self.url_base + item['href'])                                          
        return links_categories

    #Genera el id hash
    def generate_id_hash(self, title, deeplink):
        id_hash = str(title) + str(deeplink)    
        id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()
        return id_hash

    #Filtra y limpia el link de la pelicula para buscar en el search (lupa de la pagina)
    def clear_link_search_movie(self, link):
        if int(len(link.split("/"))) == 3:                                          
            link_search_page = self.url_search + link.split("/")[2]                
        elif int(len(link.split("/"))) == 4:
            link_search_page = self.url_search + link.split("/")[3]
        return link_search_page

    #Filtra y limpia el link de la imagen de cada serie
    def clear_image_show(self, content):
        if content.img.get('src'):                     
            image = content.img['src'].strip()                                                     
        else:                   
            image = None
        return image

    #Filtra y limpia la descripción de cada serie
    def clear_description_show(self, content):        
        if content:                                                                     
            if content.div != None:   
                description = content.div.text.strip()    
            elif content.p != None:   
                description = content.p.text.strip()  
        else:   
            description = None
        return description  

    #Filtra y limpia el titulo de cada episodio
    def clear_title_episode(self, content):        
        title = content.img['title']
        
        if ":" not in title:
            title = title.strip()
        elif len(title.split(":")) == 2:
            if "-" in title.split(":")[1]:
                title = title.split(":")[1].split("-")[1].strip()
            else:
                title = title.split(":")[1].strip()
        elif len(title.split(":")) == 3:
            if "Comic-Con" in title.split(":")[1]:
                title = title.strip()
            elif "-" in title.split(":")[2]:
                title = title.split(":")[2].split("-")[1].strip()                           
            else:
                title = title.split(":")[2].strip()
        return title

    #Se encarga de llenar el payload de peliculas
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

    #Se encarga de llenar el payload de series
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

    #Se encarga de llenar el payload de series
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