import time
import requests
import hashlib
from tkinter                        import N
from bs4                            import BeautifulSoup as BS
from handle.replace                 import _replace
from common                         import config
from datetime                       import datetime
from handle.mongo                   import mongo
from handle.datamanager             import Datamanager
from updates.upload                 import Upload

#Comando para correr el Script: 
#python main.py Shoutfactorytv --c US --o testing

class Shoutfactorytv():
    """
    Shoutfactorytv es una ott de Estados Unidos.

    DATOS IMPORTANTES:
    - Versión Final: Si.
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: No.
    - ¿Usa BS4?: Si.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? Tiempo: 1:54:32.778170 ~ Fecha: 25/01/2022
    - ¿Cuanto contenidos trajo la ultima vez? Peliculas: 1408 | Series: 118 | Episodios: 5376 ~ Fecha: 25/01/2022
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
        self.skippedEpis            = 0
        self.skippedTitles          = 0
        self.payloads_movies        = []                                                                    
        self.payloads_shows         = []                                                                    
        self.payloads_episodes      = []                                                                    
        #URLs necesarias para concatenar en el Scraping
        self.url_base               = "https://www.shoutfactorytv.com"                                      
        self.url_search             = "https://www.shoutfactorytv.com/videos?utf8=✓&commit=submit&q="
        self.url_search_page        = "https://www.shoutfactorytv.com/videos?page="
        self.url                    = self._config['url']                                                    
        self.testing                = False
        self.sesion                 = requests.session()
        self.headers                = {"Accept": "application/json", "Content-Type": "application/json; charset=utf-8"}
        #Lista necesaria para corroborar repetidos en el Scraping de peliculas, series y episodios
        self.list_titles            = []

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode": self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']

            self.scraping()

        if type == 'scraping':
            self.scraping()

        if type == 'testing':
            self.testing = True
            self.scraping()

    #Se encarga de instanciar las listas de la DB y llamar a los métodos que hacen el Scraping
    def scraping(self):
        print("\x1b[1;36;40m************************** \x1b[0m")
        print("\x1b[1;36;40m*** Inicia el Scraping *** \x1b[0m")
        print("\x1b[1;36;40m************************** \x1b[0m")
        
        #Se instancian las listas de peliculas, series y episodios que se encuentran en la DB Local
        self.list_db_movies_shows = Datamanager._getListDB(self, self.titanScraping)
        self.list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        
        response_url = self.verify_status_code(self.url)                                                               
        if response_url.status_code == 200:                                                          
            print("\x1b[1;32;40mCódigo de estado >>> \x1b[0m" + str(response_url.status_code))   
            #Se encarga de extraer toda la data de las peliculas
            self.get_payload_movies(response_url)
            #Se encarga de extraer toda la data de las series, dentro llama a get_payload_episodes()
            #para extraer la data de los episodios                                                   
            self.get_payload_shows(response_url)                                                              
        else:
            print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_url.status_code))
                 
        print("\x1b[1;36;40m************************** \x1b[0m")
        print("\x1b[1;36;40m***  Fin del Scraping  *** \x1b[0m")
        print("\x1b[1;36;40m************************** \x1b[0m")    

        self.sesion.close()
        
        #print(f"\033[33mComienza a subirse el Scraping a Misato <<< \033[0m")
        
        #Sube a Misato el contenido Scrapeado que se encuentra en la DB Local
        #Upload(self._platform_code, self._created_at, testing=self.testing)

    #Se encarga de extraer información de peliculas
    def get_payload_movies(self, response):   
        print(f"\033[33mComienza Scraping de peliculas <<< \033[0m")
        
        #Lista con los links de las categorias de peliculas (parametro 'POSITION' en 0)
        POSITION = 0
        links_categories_movies = self.get_links_categories(response, POSITION)

        #Lista con parte del link de todas las peliculas en todas las categorias
        links_movies = self.get_links_movies_shows(links_categories_movies)
                                                       
        #Recorre la lista de links de peliculas
        for link in links_movies:
            ###ACÁ OBTIENE EL DEEPLINK PARA LAS PELICULAS###
            deeplink = self.url_base + link                                                       
            #Link de la pelicula para buscar en el search
            deeplink_search_movie = self.clear_link_search(link)
            response_link = self.verify_status_code(deeplink_search_movie)                                
            
            if response_link.status_code == 200:                                
                print("\x1b[1;32;40mCódigo de estado >>> \x1b[0m" + str(response_link.status_code))
                #Se trae todo el contenido de la búsqueda con el link en el search 
                #y se queda con los tags que contienen la data para cada pelicula
                soup_link_search = BS(response_link.text, 'lxml')                        
                content = soup_link_search.find("article", class_='post')
                
                #Titulo que se obtiene desde el tag 'img', 
                #o bien desde el link cuando no es accesible desde la pagina
                title = self.clear_title_movies_shows(content, link)

                #Corrobora que el titulo de la pelicula no este duplicado
                if title not in self.list_titles:                                                     
                    #Agrega a la lista para comparar que no esten repetidos
                    self.list_titles.append(title)                                        
                    
                    #Nos aseguramos de extraer la información que corresponde a esa pelicula             
                    if link in content.a['href']:                                                                     
                        ###ACÁ OBTIENE LA DATA PARA LAS PELICULAS###
                        title = content.img['title'].strip()                                                         
                        image =  self.clear_image(content)                                  
                        duration = self.clear_duration_movies_episodes(content)                               
                        description = self.clear_description_movies_episodes(content)         
                        id_hash = self.generate_id_hash(title, deeplink)                                          
                        
                        print(f"\033[33mPelicula encontrada >>> \033[0m" + title)
                        
                        self.payload_movies(id_hash, deeplink, 
                                            title, image, 
                                            duration, description)                                                                                                                                                                         
                    else:
                        print("\x1b[1;31;40m¡Pelicula no encontrada! >>> \x1b[0m" + deeplink)
                else:
                    print("\x1b[1;31;40m¡Pelicula repetida! >>> \x1b[0m" + deeplink)            
            else:
                print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_link.status_code))
            
        #Se encarga de insertar en la DB Local el payload de peliculas 
        Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)

    #Se encarga de extraer información de series
    def get_payload_shows(self, response):
        print(f"\033[33mComienza Scraping de series <<< \033[0m")        
        
        #Lista con los links de las categorias de series (parametro 'POSITION' en 1)
        POSITION = 1
        links_categories_shows = self.get_links_categories(response, POSITION)                                          
        
        #Lista con parte del link de todas las series en todas las categorias
        links_shows = self.get_links_movies_shows(links_categories_shows)
        
        #Recorre la lista de links de series
        for link in links_shows:
            ###ACÁ OBTIENE EL DEEPLINK PARA LAS SERIES###                                       
            deeplink = self.url_base + link                                                                                                                                                              
            response_link = self.verify_status_code(deeplink)               
 
            if response_link.status_code == 200:
                print("\x1b[1;32;40mCódigo de estado >>> \x1b[0m" + str(response_link.status_code))
                #Se trae todo el contenido de la búsqueda con el link en el search 
                #y se queda con los tags que contienen la data para cada serie
                soup_link_search = BS(response_link.text, 'lxml')                                      
                content = soup_link_search.find("div", class_='s1')                               
                
                #Titulo que se obtiene desde el tag 'img', 
                #o bien desde el link cuando no es accesible desde la pagina
                title = self.clear_title_movies_shows(content, link)    
 
                #Corrobora que el titulo de la serie no sea una pelicula ya obtenida, o bien una serie repetida
                if title not in self.list_titles:
                    #Agrega a la lista para comparar que no esten repetidos
                    self.list_titles.append(title)                                    
                    ###ACÁ OBTIENE LA DATA PARA LAS SERIES###
                    title = title                                                                                                                        
                    image = self.clear_image(content)                             
                    content_description = soup_link_search.find("div", id='info-slide-content')
                    description = self.clear_description_shows(content_description)
                    id_hash = self.generate_id_hash(title, deeplink)          

                    print(f"\033[33mSerie encontrada >>> \033[0m" + title)

                    seasons = self.get_payload_episodes(soup_link_search, id_hash, 
                                                        title, deeplink)

                    #Corrobora que la serie tenga al menos una temporada con uno o más episodios,
                    #de lo contrario no la inserta
                    if seasons[0].get("Episodes") > 0:                                                  
                        
                        self.payload_shows(id_hash, deeplink,   
                                            title, image,          
                                            description, seasons)

                        #Se encarga de insertar en la DB Local el payload de series 
                        Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)
                        
                        #Se encarga de insertar en la DB Local el payload de episodios 
                        Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)
                    else:
                        print("\x1b[1;31;40m¡Serie no contiene episodios, es posible que se haya insertado como pelicula! >>> \x1b[0m" + deeplink)                                     
                else:
                    print("\x1b[1;31;40m¡Serie fue insertada como pelicula, o bien es una serie repetida! >>> \x1b[0m" + deeplink)                                                                                                                                                                   
            else:
                print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_link.status_code))   
                                 
    #Se encarga de extraer información de episodios 
    def get_payload_episodes(self, soup_show, parent_id, parent_title, parent_deeplink):        
        print(f"\033[33mComienza Scraping de episodios <<< \033[0m")

        #Se inicializan variables que son necesarias para controlar que se inserten
        #correctamente los episodios en cada temporada
        count_season = 1
        count_episodes_for_season = 0
        seasons_and_episodes = []
        list_seasons = []
        tab_increment = 0
        
        #El ciclo se va a ejecutar mientras encuentre una temporada 
        while True:
            #Obtiene todo los episodios de la serie en cada temporada 
            #(la variable 'tab_increment' se va incrementando para conseguir la temporada)
            content_episodes = soup_show.find("div", id='tab' + str(tab_increment))                         
            if content_episodes != None:                                                                  
                
                #Recorre todo el contenido que contiene la temporada
                for content_episode in content_episodes.find_all("a"):                                 
                    ###ACÁ OBTIENE LA DATA PARA LOS EPISODIOS###                                          
                    title = content_episode.img['title'].strip()                                       
                    deeplink = self.url_base + content_episode['href']
  
                    #Corrobora que el titulo del episodio no sea una pelicula ya obtenida                                                        
                    if title not in self.list_titles:                                                
                        ###ACÁ OBTIENE LA DATA PARA LOS EPISODIOS###
                        title = self.clear_title_episodes(content_episode)                                   
                        image = self.clear_image(content_episode)                                                      
                        season = int(content_episode.find_all('span')[1].text.split(",")[0].split(":")[1].strip())                                   
                        episode = int(content_episode.find_all('span')[1].text.split(",")[1].split(":")[1].strip())                  
                        id_hash = self.generate_id_hash(title, deeplink)                                                

                        #Link del episodio para buscar en el search                    
                        deeplink_search_episode = self.clear_link_search(content_episode['href'])                
                        response_link = self.verify_status_code(deeplink_search_episode)                            
                        
                        if response_link.status_code == 200:                                                 
                            #Se trae todo el contenido de la búsqueda con el link en el search 
                            #y se queda con los tags que contienen la data para cada episodio,
                            #esto lo hace siempre seteado en page='1'
                            soup_link_search = BS(response_link.text, 'lxml')            
                            content = soup_link_search.find("article", page='1')                        
                                                 
                            #Corrobora que exista el contenido
                            if content:
                                
                                #Nos aseguramos de extraer la información que corresponde a ese episodio
                                if content_episode['href'] in content.a['href']:                                
                                    ###ACÁ OBTIENE LA DATA PARA LOS EPISODIOS###
                                    duration = self.clear_duration_movies_episodes(content)                
                                    description = self.clear_description_movies_episodes(content)
                                
                                    print(f"\033[33mEpisodio encontrado >>> \033[0m" + title + " - " + "S" +str(season) + " " + "E" + str(episode))  
                                
                                else:
                                    duration = None
                                    description = None

                                    print("\x1b[1;31;40m¡Episodio no encontrado! >>> \x1b[0m" + deeplink)
                            else:
                                duration = None
                                description = None

                                print("\x1b[1;31;40m¡Episodio no encontrado! >>> \x1b[0m" + deeplink)
                            
                            self.payload_episodes(id_hash, deeplink, title, 
                                                image, duration, description, 
                                                season, episode, parent_id, 
                                                parent_title)

                            #Ataja inconsistencias de la plataforma a la hora de mostrar las temporadas y sus episodios
                            if season != count_season:                                                 
                                seasons_and_episodes.append({"Season": count_season,
                                                            "Episodes": count_episodes_for_season})
                                count_season = season
                                count_episodes_for_season = 1
                            else:    
                                count_episodes_for_season += 1                              
                        else:
                            print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_link.status_code))
                    else:
                        print("\x1b[1;31;40m¡Episodio fue insertado como pelicula! >>> \x1b[0m" + deeplink)                      
                
                seasons_and_episodes.append({"Season": count_season,
                                            "Episodes": count_episodes_for_season})             

                #Recorre la lista de temporadas y episodios para insertar en el campo
                #que corresponde en el payload de series
                for content in seasons_and_episodes:
                    ###ACÁ OBTIENE LA DATA PARA LAS SERIES###
                    number_season = content.get("Season")
                    name_season = parent_title + " Season " + str(number_season)
                    id_hash = self.generate_id_hash(name_season, parent_deeplink)
                    number_episodes = content.get("Episodes")
                    
                    list_seasons.append({
                                "Id":         id_hash,                                                      #Importante
                                "Synopsis":   None,                                                         #Importante
                                "Title":      name_season,                                                  #Importante, E.J. The Wallking Dead: Season 1
                                "Deeplink":   parent_deeplink,                                              #Importante
                                "Number":     number_season,                                                #Importante
                                "Year":       None,                                                         #Importante
                                "Image":      None,                                                     
                                "Directors":  None,                                                         #Importante
                                "Cast":       None,                                                         #Importante
                                "Episodes":   number_episodes,                                              #Importante
                                "IsOriginal": None,   
                                })
               
                count_episodes_for_season = 0
                seasons_and_episodes = []
                tab_increment += 1

                #Corrobora que exista otra temporada siguiente que contenga episodios
                #y le asigna a 'count_season' el número de la temporada siguiente,
                #de lo contrario la setea en None.
                try:
                    new_count_season = int(soup_show.find("div", id='tab' + str(tab_increment)).find_all('span')[1].text.split(",")[0].split(":")[1].strip())
                    count_season = new_count_season
                except:
                    count_season = None
            else:
                break
        
        return list_seasons

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

    #Extrae los links de todas las categorias de peliculas y series
    def get_links_categories(self, response, position):   
        soup_categories = BS(response.text, 'lxml')                                                         
        categories = soup_categories.find_all("div", class_='divRow')                                       
        list_categories = categories[position].find_all("a")                                         
        
        links_categories = []         
        for item in list_categories:        
             links_categories.append(self.url_base + item['href'])                                          
        return links_categories

    #Extrae parte del link de las peliculas y series de todas las categorias
    def get_links_movies_shows(self, links):        
        list_links = []
        
        for link_categorie in links:                                                      
            response_categorie = self.verify_status_code(link_categorie)
            print("\x1b[1;35;40mCategoria >>> \x1b[0m" + link_categorie)    
            
            if response_categorie.status_code == 200:
                print("\x1b[1;32;40mCódigo de estado >>> \x1b[0m" + str(response_categorie.status_code))                                   
                soup_link_categorie = BS(response_categorie.text, 'lxml')                                          
                content_categorie = soup_link_categorie.find_all("div", class_='img-holder')                

                for content in content_categorie:                                                             
                    content_links = content.find_all("a")

                    for item in content_links:
                        list_links.append(item['href'])
            else:
                print("\x1b[1;31;40mCódigo de estado >>> \x1b[0m" + str(response_categorie.status_code))
        return list_links

    #Filtra y limpia el link de las peliculas, series y episodios para buscar en el search (lupa de la pagina)
    def clear_link_search(self, link):
        if len(link.split("/")) == 3:                                          
            link_search_page = self.url_search + "1&q=" + link.split("/")[2].replace("-series", "")                
        elif len(link.split("/")) == 4:
            link_search_page = self.url_search + "1&q=" + link.split("/")[3].replace("-series", "")
        return link_search_page

    #Genera el id hash
    def generate_id_hash(self, title, deeplink):
        id_hash = str(title) + str(deeplink)    
        id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()
        return str(id_hash)

    #Filtra y limpia el link de la imagen
    def clear_image(self, content):
        try:                     
            image = content.img['src'].strip()                                                     
        except:                   
            image = None
        return image

    #Filtra y limpia el titulo de las peliculas y series
    def clear_title_movies_shows(self, content, link):                                                             
        try:                                                
            title = content.img['title'].strip()                                             
        except:                       
            title = str(link.split("/")[2].replace("-", " ")).title()
        return title

    #Filtra y limpia la descripción de las peliculas y episodios
    def clear_description_movies_episodes(self, content):
        try:
            description = content.p.text.replace("\n", "").strip()
        except:
            description = None
        return description

    #Filtra y limpia la duración de las peliculas y episodios
    def clear_duration_movies_episodes(self, content):
        try:
            duration = int(content.time.text.split(" ")[1].split(":")[0])
        except:
            duration = None
        return duration
    
    #Filtra y limpia la descripción de las series
    def clear_description_shows(self, content):                                                                                     
        try:
            if content.div:
                description = content.div.text.replace("\r\n", "").strip()      
            else: 
                description = content.p.text.replace("\r\n", "").strip()  
        except:
            description = None
        return description  

    #Filtra y limpia el titulo de los episodios
    def clear_title_episodes(self, content):        
        title = content.img['title']
        
        if ":" not in title:
            title = title
        elif len(title.split(":")) == 2:
            if "-" in title.split(":")[1]:
                if len(title.split(":")[1].split("-")) == 3:
                    title = title.split(":")[1].split("-")[2]
                else:
                    title = title.split(":")[1].split("-")[1]
            else:
                title = title.split(":")[1]
        elif len(title.split(":")) == 3:
            if "Comic-Con" in title.split(":")[1]:
                title = title
            elif "-" in title.split(":")[2]:
                title = title.split(":")[2].split("-")[1]                           
            else:
                title = title.split(":")[2]
        return title.strip()

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
        Datamanager._checkDBandAppend(self, payload_movie, self.list_db_movies_shows, self.payloads_movies)

    #Se encarga de llenar el payload de series
    def payload_shows(self, id_hash, deeplink, title, image, description, seasons):
        payload_show = {
            "PlatformCode":  self._platform_code,                                                           #Obligatorio      
            "Id":            id_hash,                                                                       #Obligatorio
            "Seasons":       seasons,                                                                       
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
        Datamanager._checkDBandAppend(self, payload_show, self.list_db_movies_shows, self.payloads_shows)

    #Se encarga de llenar el payload de episodios
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
        Datamanager._checkDBandAppend(self, payload_episode, self.list_db_episodes, self.payloads_episodes, isEpi=True)