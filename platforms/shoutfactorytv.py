from ast import Break
import requests
import hashlib
import time
import pymongo 
import re
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from bs4                    import BeautifulSoup as BS
from handle.datamanager     import Datamanager
from updates.upload         import Upload

#Funcionamiento:
#Obtener la data de la manera mas completa se hace tanto para movies como series de la misma manera
#Movies: se obtiene la ultima parte de la url de la pelicula a scrapear, por ej, shoutfactorytv.com/bloodfist/"5c1a73e0d80ed51332007496", 
#una vez obenida hacemos un soup a una url en especifico, esta: 'shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q='
#y esa ultima parte se la pasamos a esa url, nos quedaria 'shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=5c1a73e0d80ed51332007496' 
#que sirve como si buscaramos en la lupa de la pagina, haciendo soup a esa url
#la pagina te devuelve solo la pelicula que querias buscar y nada mas, con toda la info disponible

#Series: muy parecido a movies, pero esta vez nos metemos a cada serie en vez de sacar su url solamente,
#una vez dentro de cada serie, sacamos la url de cada epi y nos quedamos con la ultima parte de este
#y hacemos lo mismo que con las movies, buscamos el id que esta en la url y hacemos soup a como si lo buscaramos en la lupa 

# DATOS SCRAPEADOS:
# MOVIES: titulo, deeplink, image, descripcion, duracion, id, categoria
# SERIES: titulo, deeplink, image, descripcion, categoria, id, FALTA SEASONS
# EPISODES: titulo, deeplink, image, parent_title, parent_id, temporada, episodio, descripcion, duracion, id

# Tiene pocos metodos porque todavia me falta separarlo en mas, igualmente el script se ejecuta muy linealmente 
# y la manera de obtener la data de los distintos payloads varia en algunas cosas

class Shoutfactorytv():

    """
    DATOS IMPORTANTES:
    - Versión Final: No.
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: No.
    - ¿Usa BS4?: Si.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? Aprox 1h 40min
    - ¿Cuanto contenidos trajo la ultima vez? TITANSCRAPING: APROX 1546; TITANSCRAPINGEPISODE: APROX 5375
    """

    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url            = self._config['start_url']
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
        self.currentSession         = requests.session()
        #self._format_url            = self._config['format_url'] 
        #self._episode_url           = self._config['episode_url']
        self.testing                = False
        self.payloads_db            = Datamanager._getListDB(self, self.titanScraping)
        self.payloads_epi_db        = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        self.headers                = {"Accept":"application/json",
                                        "Content-Type":"application/json; charset=utf-8"}

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
        self.scraping_content()

    def scraping_content(self):
        self.list_db_movies_shows = Datamanager._getListDB(self, self.titanScraping)
        self.list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        visited_deeplinks = []
        url = 'https://www.shoutfactorytv.com' # URL principal de la pagina
        response = requests.get(url)
        content = response.text
        soup = BS(content, 'lxml') # Hacemos soup a la URL principal
        #print(soup.prettify())
        categories = soup.find_all("div",{"class":"divRow"})
        movies_categories = categories[0].find_all("a") # Encontrar todos los links de las categorias movies
        series_categories = categories[1].find_all("a") # Encontrar todos los links de las categorias series
        
        self.scraping_category_movies(movies_categories, url, visited_deeplinks)
        self.scraping_category_series(series_categories, url, visited_deeplinks)
        self.scraping_extra_content(url, visited_deeplinks)
    
    def scraping_category_movies(self, movies_categories, url, visited_deeplinks):
        print(f"\x1b[1;32;40m SCRAPEANDO MOVIES \x1b[0m")
        for category in movies_categories: # Empezamos scrapeo de categorias movies
            deeplink_category = url + category["href"]
            response = requests.get(deeplink_category)
            content = response.text
            soup = BS(content, 'lxml')
            movies = soup.find("div",{"id":"main"})
            movies_category = [movies.find_all("h2")[-1].text]
            print(f"\x1b[1;32;40m SCRAPEANDO CATEGORIA MOVIES: {movies_category[0]} \x1b[0m")
            movies_href = movies.find_all("a") # Encontramos todos los href de las movies dentro de la categoria
            for movie_href in movies_href: # Empezamos scrapeo de cada movie
                if movie_href["href"] != "#": # Hay dos tag <a> que su href es "#", esos no son movie
                    movie_deeplink = url + movie_href["href"] # Se crea el deeplink con la url de la pagina y el href de la pelicula
                    if movie_deeplink not in visited_deeplinks: # Si el deeplink no fue visitado todavia, que ejecute el metodo scraping movies
                        self.scraping_movies(movie_deeplink, visited_deeplinks, movies_category)
            Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)
    
    # Metodo que obtiene la info de cada pelicula 
    def scraping_movies(self, movie_deeplink, visited_deeplinks, movies_category): 
        movie_id = movie_deeplink.split("/")[-1]
        search_movie = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + movie_id 
        response = requests.get(search_movie) # al hacer esta requests podemos ver la duracion de la movie ya que entrando en la movie directamente no te la muestra
        content = response.text
        soup = BS(content, 'lxml')
        search_movie_soup = soup.find("div",{"class":"video-container"})
        movies = search_movie_soup.find_all("article")
        for movie in movies: # Busca la movie que queremos y si la encuentra asigna todas las variables necesarias
            if movie_id == (movie.a["href"].split("/")[-1]):
                movie_title = movie.h2.text
                if int(movie.time.text.split(':')[2]) > 29:
                    movie_duration = int(movie.time.text.split(':')[1]) + 1
                else:
                    movie_duration = int(movie.time.text.split(':')[1])
                try:
                    movie_image = movie.img["src"]
                except:
                    movie_image = None
                movie_description = movie.p.text.strip()
                visited_deeplinks.append(movie_deeplink)
                self.payload_movies(movie_id, movie_title, movie_duration, movie_deeplink, movie_description, movie_image, movies_category)
                break
    
    def scraping_category_series(self, series_categories, url, visited_deeplinks):
        print(f"\x1b[1;32;40m SCRAPEANDO SERIES \x1b[0m")
        for category in series_categories: # Empezamos scrapeo de categorias series
            deeplink_category = url + category["href"]
            response = requests.get(deeplink_category)
            content = response.text
            soup = BS(content, 'lxml')
            series = soup.find("div",{"id":"main"})
            series_category = [series.find("h2").text.replace("Series", "").strip()] # Todas las categorias dicen Series, se lo sacamos
            print(f"\x1b[1;32;40m SCRAPEANDO CATEGORIA SERIES: {series_category[0]} \x1b[0m")
            series_href = series.find_all("a") # Encontramos todos los href de las movies dentro de la categoria
            for serie in series_href: # Empezamos scrapeo de cada serie
                serie_deeplink = url + serie["href"] # Se crea el deeplink con la url de la pagina y el href de la serie
                if serie_deeplink not in visited_deeplinks: # Si el deeplink no fue visitado todavia, que ejecute el metodo scraping series
                    self.scraping_series(series_category, url, visited_deeplinks, serie_deeplink)

    # Metodo que obtiene la info de cada serie y luego de sus episodes 
    def scraping_series(self, series_category, url, visited_deeplinks, serie_deeplink):
        serie_episodes = []
        response = requests.get(serie_deeplink)
        content = response.text
        soup = BS(content, 'lxml')
        serie_content = soup.find("div",{"id":"main"})
        try:
            serie_image = serie_content.img["src"]
        except:
            serie_image = None
        serie_title = serie_content.img["title"]
        serie_id = str(serie_deeplink) + str(serie_title)    
        serie_id = hashlib.md5(serie_id.encode('utf-8')).hexdigest()
        serie_description = soup.find("p").text.strip()
        episodes_container = soup.find_all("div",{"class":"holder"})
        visited_deeplinks.append(serie_deeplink)
        # Hasta este punto se pudo sacar toda la info posible de la serie
        episodes = self.scraping_episodes(url, serie_episodes, episodes_container, visited_deeplinks, serie_title, serie_id)
        if episodes != 0: # Hay 3 series que sus epis son solo movies que ya estaban scrapeadas y quedan vacias
            self.payload_series(serie_title, serie_deeplink, serie_id, serie_description, serie_image, series_category)
            Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)
            Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)
            # Si la serie tiene episodes que no han sido scrapeados, se hace el payload y se inserta
        else:
            print("\x1b[1;31;40m  SERIE SIN EPISODES \x1b[0m")

    # Metodo que obtiene los episodes de cada serie
    # Su funcionamiento es mas complicado que los anteriores debido a los errores de la pagina
    def scraping_episodes(self, url, serie_episodes, episodes_container, visited_deeplinks, serie_title, serie_id):
        episodes_href = episodes_container[1].find_all("a") # Encontramos todos los episodes de la serie
        serie_episodes_count = 0
        for episode in episodes_href:
            try:
                episode_id = episode["href"].split("/")[-1] # Agarramos el id de cada episode
            except:
                break
            # Debido a que hay episodes que no tienen season ni episode number en su titulo, es mejor sacarlo desde la serie
            season_episode_number = episode.find_all("span")[1].text.strip()
            season_number = season_episode_number.split(",")[0].split(":")[1].strip()
            episode_number = season_episode_number.split(",")[1].split(":")[1].strip()
            if episode_id and season_number and episode_number:
                id_season_episode = episode_id + " " + season_number + " " + episode_number
                serie_episodes.append(id_season_episode)
                # Agregamos el id, el season y el episode number a una lista para luego dividirla y quedarnos con lo que nos sirve
            else:
                serie_episodes.append(episode_id)
        for episodes in serie_episodes: 
            episode_id = episodes.split(" ")[0]
            episode_search = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + episode_id
            response = requests.get(episode_search) # al hacer esta requests podemos ver la duracion del episode y tarda mucho menos que hacer una requests al episode en si
            content = response.text
            soup = BS(content, 'lxml')
            episode_container = soup.find("div",{"class":"video-container"})
            episode_content = episode_container.find("article")
            episode_deeplink = url + episode_content.a["href"]
            if episode_deeplink not in visited_deeplinks: #  Verificamos que no haya sido visitado el episode con el visited_deeplinks
                if episode_id == (episode_content.a["href"].split("/")[-1]):
                    # Con la lista que hicimos anteriormente, nos quedamos con lo que nos interesa
                    season_number = int(episodes.split(" ")[1])
                    episode_number = int(episodes.split(" ")[2])
                    # Podemos pasarle el parent_title y parent_id directamente
                    parent_title = serie_title
                    parent_id = serie_id
                    episode_title_raw = episode_content.h2.text #Esta variable es para usarla en el metodo get_episode_title
                    # Se llama al metodo que limpia el title del episodio
                    episode_title = self.get_episode_title(episode_title_raw, season_number, episode_number, episodes, parent_title)
                    try:
                        episode_image = episode_content.img["src"]
                    except:
                        episode_image = None
                    if int(episode_content.time.text.split(':')[2]) > 29:
                        episode_duration = int(episode_content.time.text.split(':')[1]) + 1
                    else:
                        episode_duration = int(episode_content.time.text.split(':')[1])
                    episode_description = episode_content.p.text.strip()
                    visited_deeplinks.append(episode_deeplink)
                    serie_episodes_count = serie_episodes_count + 1
                    self.payload_episodes(episode_id, parent_id, parent_title, episode_number, season_number, episode_title, episode_duration, episode_deeplink, episode_description, episode_image)
            else:
                print("\x1b[1;31;40m  EPISODE YA ESTABA SCRAPEADO \x1b[0m") # Por lo general son movies que aparecen como episodes
        if len(serie_episodes) == serie_episodes_count:
            print(f"\x1b[1;32;40m TODOS LOS EPISODIOS SCRAPEADOS: {serie_episodes_count}/{len(serie_episodes)} \x1b[0m")
        else:
            print(f"\x1b[1;31;40m  HUBO EPISODIOS NO SCRAPEADOS: {serie_episodes_count}/{len(serie_episodes)} \x1b[0m")
        return serie_episodes_count

    # Metodo que obtiene contenido que no esta en ninguna categoria, son un par de peliculas y series
    def scraping_extra_content(self, url, visited_deeplinks):
        print(f"\033[33m SCRAPEANDO CONTENIDO EXTRA \033[0m")
        extra_count = 0
        i = 1
        for pages in range(999):
            extra_deeplinks = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + f'&page={i}'
            response = requests.get(extra_deeplinks) # Este request es como si buscaramos en la lupa de la pagina, donde esta todo el contenido
            content = response.text
            soup = BS(content, 'lxml')
            all_content = soup.find("div",{"class":"video-container"})
            extra_content = all_content.find_all("article")
            while all_content.find("p").text != "No videos were found.": # Significa que llegamos al final de TODA la pagina
                for extra in extra_content:
                    extra_deeplink = url + extra.a["href"]
                    if extra_deeplink not in visited_deeplinks:
                        if int(extra.time.text.split(':')[2]) > 29:
                            extra_duration = int(extra.time.text.split(':')[1]) + 1
                        else:
                            extra_duration = int(extra.time.text.split(':')[1])
                        extra_title = extra.h2.text
                        if extra_duration != 0 and ".mp4" not in extra_title and "Promo" not in extra_title and "Trailer" not in extra_title and "Factory TV" not in extra_title and "New in" not in extra_title  and "New In" not in extra_title: # Limpiamos trailers, promos, cortos, contenido que no corresponde
                            response = requests.get(extra_deeplink)
                            content = response.text
                            soup = BS(content, 'lxml')
                            aside = soup.find("aside",{"id":"sidebar"})
                            if aside != None: 
                                if aside.find("h2").text == "Series": # Si se cumple el if, es que encontramos la serie a la que pertenece el episode
                                    serie_deeplink = url + aside.a["href"]
                                    if serie_deeplink not in visited_deeplinks:
                                        series_category = None
                                        self.scraping_series(series_category, url, visited_deeplinks, serie_deeplink) # Se encuentra la serie padre y se llama al metodo
                                else:
                                    if ": " in extra_title and " - " in extra_title: # Hay unos pocos episodios "sueltos" que no estan en su serie correspondiente, o hay otros que estan dos veces, uno dentra de la serie y otro fuera, es dificil diferir cual es cual
                                        break
                                    else:
                                        movie_deeplink = extra_deeplink
                                        movies_category = None
                                        self.scraping_movies(movie_deeplink, visited_deeplinks, movies_category) # Se scrapea la movie de la misma manera que las que estan en las categorias
                            else:
                                break
                i = i + 1
                break
        print(f"CONTENIDOS QUE NO SE ENCONTRARON EN LAS CATEGORIAS: {extra_count}")

    #Metodo que se encarga de limpiar el title de cualquier episode ya que vienen de muchas maneras
    def get_episode_title(self, episode_title_raw, season_number, episode_number, episodes, parent_title): #metodo creado porque es muy largo el conseguir el titulo de los epis de manera correcta
        if ' - ' in episode_title_raw:
            if ':' in episode_title_raw and f'S{season_number}' in episode_title_raw and f'E{episode_number}' in episode_title_raw:
                episode_title_list = episode_title_raw.split(":")[1:]
                episode_title = ":".join(episode_title_list)
                episode_title_list = episode_title.split(' - ')[1:]
                if len(episode_title_list) == 1:
                    episode_title = "".join(episode_title_list).strip()
                else: 
                    episode_title = " - ".join(episode_title_list)
            elif ': ' in episode_title_raw and 'S' in episode_title_raw and 'E' in episode_title_raw:
                episode_title_list = episode_title_raw.split(':')[1:]
                episode_title = ":".join(episode_title_list).strip()
                try:
                    season_number = int(episode_title.split(' - ')[0].strip().split(' ')[0].replace('S', ''))
                    episode_number = int(episode_title.split(' - ')[0].strip().split(' ')[1].replace('E', ''))
                except ValueError:
                    season_number = int(episodes.split(" ")[1])
                    episode_number = int(episodes.split(" ")[2])
                if f'S{season_number}' in episode_title and f'E{episode_number}' in episode_title:
                    episode_title_list = episode_title.split(' - ')[1:]
                    if len(episode_title_list) == 1:
                        episode_title = "".join(episode_title_list)
                    else: 
                        episode_title = " - ".join(episode_title_list)
            else:
                episode_title_list = episode_title_raw.split(":")[1:]
                episode_title = ":".join(episode_title_list).strip()
        elif ':' in episode_title_raw:
            if 'The Jerry Lewis Show: 1957-62 TV Specials' in episode_title_raw: #excepcion para esta serie por los titulos de sus epis
                episode_title_list = episode_title_raw.split(":")[2:]
                episode_title = ":".join(episode_title_list).strip()
            elif ' – ' in episode_title_raw:
                if ':' in episode_title_raw and f'S{season_number}' in episode_title_raw and f'E{episode_number}' in episode_title_raw:
                    episode_title_list = episode_title_raw.split(":")[1:]
                    episode_title = ":".join(episode_title_list)
                    episode_title_list = episode_title.split(' – ')[1:]
                    if len(episode_title_list) == 1:
                        episode_title = "".join(episode_title_list)
                    else: 
                        episode_title = " – ".join(episode_title_list)
                else:
                    episode_title_list = episode_title_raw.split(":")[1:]
                    episode_title = ":".join(episode_title_list).strip()
            else:
                episode_title_list = episode_title_raw.split(":")[1:]
                episode_title = ":".join(episode_title_list).strip()
        else:
            episode_title = episode_title_raw
        if f"S{season_number} E{episode_number}" in episode_title:
            episode_title = parent_title + f": S{season_number} E{episode_number}"
        return episode_title

    #PAYLOADS
    def payload_movies(self, movie_id, movie_title, movie_duration, movie_deeplink, movie_description, movie_image, movies_category):
        payload_movie = {
            "PlatformCode":  self._platform_code, #Obligatorio      
            "Id":            movie_id, #Obligatorio
            "Seasons":       None,
            "Title":         movie_title, #Obligatorio      
            "CleanTitle":    _replace(movie_title), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          "movie",     #Obligatorio      
            "Year":          None,     #Important!     
            "Duration":      movie_duration,      
            "ExternalIds":   None,      
            "Deeplinks": {          
                "Web":       movie_deeplink,       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },      
            "Synopsis":      movie_description,      
            "Image":         movie_image,      
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        movies_category,    #Important!      
            "Cast":          None,      
            "Directors":     None,    #Important!      
            "Availability":  None,     #Important!      
            "Download":      None,      
            "IsOriginal":    None,    #Important!      
            "IsAdult":       None,    #Important!   
            "IsBranded":     None,    #Important!   
            "Packages":      [{"Type":"subscription-vod"}],    #Obligatorio      
            "Country":       None,      
            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
            "CreatedAt":     self._created_at, # self._created_at #Obligatorio
        }
        #print(payload_movie)
        Datamanager._checkDBandAppend(self, payload_movie, self.list_db_movies_shows, self.payloads_movies)

    def payload_series(self, serie_title, serie_deeplink, serie_id, serie_description, serie_image, series_category):
        payload = {
            "PlatformCode":  self._platform_code, #Obligatorio      
            "Id":            serie_id,            #Obligatorio
            "Seasons":       [ #Unicamente para series
                    {
                      "Id": "str",           #Importante
                      "Synopsis": "str",     #Importante
                      "Title": "str",        #Importante, E.J. The Wallking Dead: Season 1
                      "Deeplink":  "str",    #Importante
                      "Number": "int",       #Importante
                      "Year": "int",         #Importante
                      "Image": "list", 
                      "Directors": "list",   #Importante
                      "Cast": "list",        #Importante
                      "Episodes": "int",      #Importante
                      "IsOriginal": "bool"    
                    }
            ],
            "Title":         serie_title,         #Obligatorio      
            "CleanTitle":    _replace(serie_title), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          "serie",            #Obligatorio      
            "Year":          None,               #Important!     
            "Duration":      None,      
            "ExternalIds":   None,      
            "Deeplinks": {          
                "Web":       serie_deeplink,     #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },      
            "Synopsis":      serie_description,      
            "Image":         serie_image,      
            "Rating":        None,               #Important!      
            "Provider":      None,      
            "Genres":        series_category,    #Important!      
            "Cast":          None,      
            "Directors":     None,               #Important!      
            "Availability":  None,               #Important!      
            "Download":      None,      
            "IsOriginal":    None,               #Important!      
            "IsAdult":       None,               #Important!   
            "IsBranded":     None,               #Important!   
            "Packages":      [{"Type":"Subscription-vod"}],            #Obligatorio      
            "Country":       None,      
            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
            "CreatedAt":     self._created_at, #self._created_at   #Obligatorio
        }
        #print(payload)
        Datamanager._checkDBandAppend(self, payload, self.list_db_movies_shows, self.payloads_shows)

    def payload_episodes(self, episode_id, parent_id, parent_title, episode_number, season_number, episode_title, episode_duration, episode_deeplink, episode_description, episode_image):
        payload_epi = {
            "PlatformCode":  self._platform_code, #Obligatorio      
            "Id":            episode_id, #Obligatorio
            "ParentId":      parent_id, #Obligatorio #Unicamente en Episodios
            "ParentTitle":   parent_title, #Unicamente en Episodios 
            "Episode":       episode_number, #Obligatorio #Unicamente en Episodios  
            "Season":        season_number, #Obligatorio #Unicamente en Episodios
            "Title":         episode_title, #Obligatorio           
            "OriginalTitle": None,                                
            "Year":          None,     #Important!     
            "Duration":      episode_duration,      
            "ExternalIds":   None,      
            "Deeplinks": {          
                "Web":       episode_deeplink,       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },      
            "Synopsis":      episode_description,      
            "Image":         episode_image,      
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        None,    #Important!      
            "Cast":          None,      
            "Directors":     None,    #Important!      
            "Availability":  None,     #Important!      
            "Download":      None,      
            "IsOriginal":    None,    #Important!      
            "IsAdult":       None,    #Important!   
            "IsBranded":     None,    #Important!   
            "Packages":      [{"Type":"Subscription"}],    #Obligatorio      
            "Country":       None,      
            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
            "CreatedAt":     self._created_at, #self._created_at, #Obligatorio
        }
        #print(payload_epi)
        Datamanager._checkDBandAppend(self, payload_epi, self.list_db_episodes, self.payloads_episodes, isEpi=True)