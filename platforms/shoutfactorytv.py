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

"""
Funcionamiento:
Obtener la data de la manera mas completa se hace tanto para movies como series de la misma manera
Movies: se obtiene la ultima parte de la url de la pelicula a scrapear, por ej, shoutfactorytv.com/bloodfist/"5c1a73e0d80ed51332007496", 
una vez obenida hacemos un soup a una url en especifico, esta: 'shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q='
y esa ultima parte se la pasamos a esa url, nos quedaria 'shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=5c1a73e0d80ed51332007496' 
basicamente eso es como si buscaramos en la lupa de la pagina para que nos traiga UNICAMENTE el contenido que queremos, haciendo soup a esa url
la pagina te devuelve solo la pelicula que querias buscar y nada mas, con toda la info disponible

Series: muy parecido a movies, pero esta vez nos metemos a cada serie en vez de sacar su url solamente,
una vez dentro de cada serie, sacamos la url de cada epi y nos quedamos con la ultima parte de este
y hacemos lo mismo que con las movies, buscamos el id que esta en la url y hacemos soup a la url como si lo buscaramos en la lupa 

"""

class Shoutfactorytv():

    """
    DATOS IMPORTANTES:
    - Versión Final: Si. 
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: No.
    - ¿Usa BS4?: Si.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? Aprox 1h 48min
    - ¿Cuanto contenidos trajo la ultima vez? TS: Aprox 1556; TSE: Aprox 5462
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
        id_list = [] #Lista donde se guardan los ids para despues verificar cuales contenidos ya fueron visitados
        url = 'https://www.shoutfactorytv.com' #URL principal de la pagina
        soup = self.try_soup(url)
        categories = soup.find_all("div",{"class":"divRow"})
        movies_categories = categories[0].find_all("a") #Encontrar todos los links de las categorias movies
        series_categories = categories[1].find_all("a") #Encontrar todos los links de las categorias series
        
        #Llamamos a los metodos de movies, series, episodes y contenido extra
        self.scraping_category_movies(movies_categories, url, id_list)
        self.scraping_category_series(series_categories, url, id_list)
        self.scraping_extra_content(url, id_list)

        self.currentSession.close()

        Upload(self._platform_code, self._created_at, testing=self.testing)
    
    #Metodo que se encarga de obtener las pelis de cada categoria
    def scraping_category_movies(self, movies_categories, url, id_list):
        print(f"\x1b[1;32;40m SCRAPEANDO MOVIES \x1b[0m")
        for category in movies_categories: 
            deeplink_category = url + category["href"]
            soup = self.try_soup(deeplink_category)
            movies = soup.find("div",{"id":"main"})
            movies_category = [movies.find_all("h2")[-1].text]
            if "Shout! Originals" in movies_category[0] or "Shout! Studios" in movies_category[0]:
                is_original = True
            else:
                is_original = False
            print(f"\x1b[1;32;40m SCRAPEANDO CATEGORIA MOVIES: {movies_category[0]} \x1b[0m")
            movies_href = movies.find_all("a") #Encontramos todos los href de las movies dentro de la categoria
            for movie_href in movies_href: #Empezamos scrapeo de cada pelicula
                if movie_href["href"] != "#": #Hay dos tag <a> que su href es "#", esos no son pelicula
                    movie_deeplink = url + movie_href["href"] #Se crea el deeplink con la url de la pagina y el href de la pelicula
                    movie_id = movie_deeplink.split("/")[-1]
                    if movie_id not in id_list: #Verificamos que no haya sido visitada la movie con el id_list
                        self.scraping_movies(movie_deeplink, movies_category, movie_id, id_list, is_original)
            Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)
    
    #Metodo que se encarga de obtener la info de cada pelicula 
    def scraping_movies(self, movie_deeplink, movies_category, movie_id, id_list, is_original):
        #Con la variable movie_search podemos hacer que el soup traiga solo la peli que queremos con toda la info posible
        movie_search = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + movie_id
        soup = self.try_soup(movie_search)
        search_movie_soup = soup.find("div",{"class":"video-container"})
        movies = search_movie_soup.find_all("article")
        for movie in movies: #Deberia encontrar solo la peli que queremos, pero por las dudas se hace que recorra todo el contenido que encuentra
            if movie_id == (movie.a["href"].split("/")[-1]):
                movie_title = movie.h2.text
                if "[" in movie_title and "]" in movie_title:
                    if "Language Version" in movie_title:
                        movie_dubbed = [movie_title.split("[")[-1].split("-")[0]]
                    elif "Edit" in movie_title:
                        movie_dubbed = None
                    else:
                        movie_dubbed = None
                        movie_title = movie_title.split(" [")[0]
                else:
                    movie_dubbed = None
                movie_duration = self.get_duration(movie)
                movie_image = self.get_image(movie)
                movie_description = movie.p.text.strip()
                id_list.append(movie_id)
                self.payload_movies(movie_id, movie_title, movie_duration, movie_deeplink, movie_description, movie_image, movie_dubbed, movies_category, is_original)
    
    def scraping_category_series(self, series_categories, url, id_list):
        print(f"\x1b[1;32;40m SCRAPEANDO SERIES \x1b[0m")
        for category in series_categories: #Empezamos scrapeo de categorias series
            deeplink_category = url + category["href"]
            soup = self.try_soup(deeplink_category)
            series = soup.find("div",{"id":"main"})
            series_category = [series.find("h2").text.replace("Series", "").strip()] #Todas las categorias dicen Series, se lo sacamos
            if "Shout! Originals" in series_category[0]:
                is_original = True
            else:
                is_original = False
            print(f"\x1b[1;32;40m SCRAPEANDO CATEGORIA SERIES: {series_category[0]} \x1b[0m")
            series_href = series.find_all("a") #Encontramos todos los href de las series dentro de la categoria
            for serie in series_href: #Empezamos scrapeo de cada serie
                serie_deeplink = url + serie["href"] #Se crea el deeplink con la url de la pagina y el href de la serie
                serie_title = serie.img["title"]
                serie_id = self.id_hash(serie_deeplink, serie_title)
                if serie_id not in id_list: #Verificamos que no haya sido visitada la serie con el id_list
                    extra_content = False
                    self.scraping_series(series_category, url, serie_deeplink, serie_title, serie_id, extra_content, id_list, is_original)

    #Metodo que obtiene la info de cada serie y luego de sus episodes 
    def scraping_series(self, series_category, url, serie_deeplink, serie_title, serie_id, extra_content, id_list, is_original):
        serie_episodes = []
        soup = self.try_soup(serie_deeplink)
        serie_content = soup.find("div",{"id":"main"})
        serie_image = self.get_image(serie_content)
        serie_description = soup.find("p").text.strip()
        episodes_container = soup.find_all("div",{"class":"holder"})
        id_list.append(serie_id)
        #Hasta este punto se pudo sacar toda la info posible de la serie
        episodes_seasons = self.get_episodes_from_serie(url, serie_episodes, episodes_container, serie_title, serie_id, extra_content, id_list, serie_deeplink, is_original)
        if episodes_seasons[0] != 0: #Hay 3 series que sus epis son solo movies que ya estaban scrapeadas y quedan vacias
            self.payload_series(serie_title, serie_deeplink, serie_id, episodes_seasons[1], serie_description, serie_image, series_category, is_original)
            Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)
            Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)
        else:
            print("\x1b[1;31;40m >>> Serie sin episodios, se saltea <<< \x1b[0m")
            #No se inserta la serie en la base de datos si esta vacia

    #Metodo que obtiene los episodes de cada serie
    def get_episodes_from_serie(self, url, serie_episodes, episodes_container, serie_title, serie_id, extra_content, id_list, serie_deeplink, is_original):
        episodes_href = episodes_container[1].find_all("a") #Encontramos todos los episodes de la serie
        season_list = [] #Cuenta la cantidad de seasons de la serie
        for episode in episodes_href:
            try:
                episode_id = episode["href"].split("/")[-1] #Agarramos el id de cada episode
            except:
                break
            #Debido a que hay episodes que no tienen season ni episode number en su titulo, es mejor sacarlo desde la serie
            season_episode_number = episode.find_all("span")[1].text.strip()
            season_number = season_episode_number.split(",")[0].split(":")[1].strip()
            season_list.append(season_number)
            episode_number = season_episode_number.split(",")[1].split(":")[1].strip()
            id_season_episode = episode_id + " " + season_number + " " + episode_number
            serie_episodes.append(id_season_episode)
            #Agregamos el id, el season y el episode number a una lista para luego dividirla y quedarnos con lo que nos sirve
        return self.scraping_episodes(serie_episodes, url, id_list, serie_title, serie_id, extra_content, season_list, serie_deeplink, is_original)

    #Metodo que scrapea los episodes de cada serie
    def scraping_episodes(self, serie_episodes, url, id_list, serie_title, serie_id, extra_content, season_list, serie_deeplink, is_original):
        serie_episodes_count = 0 #Cuenta la cantidad de episodios de la serie
        for episodes in serie_episodes: 
            episode_id = episodes.split(" ")[0]
            episode_search = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + episode_id
            soup = self.try_soup(episode_search)
            episode_container = soup.find("div",{"class":"video-container"})
            episode_content = episode_container.find("article")
            episode_deeplink = url + episode_content.a["href"]
            if episode_id not in id_list: #Verificamos que no haya sido visitado el episode con el id_list
                if episode_id == (episode_content.a["href"].split("/")[-1]):
                    #Con la lista que hicimos anteriormente, nos quedamos el numero de temporada y de episode
                    season_number = int(episodes.split(" ")[1])
                    episode_number = int(episodes.split(" ")[2])
                    #Podemos pasarle el parent_title y parent_id directamente
                    parent_title = serie_title
                    parent_id = serie_id
                    #Esta variable (episode_title_raw) tiene el titulo entero del episodio (del tipo: parenttitle: s1 e1 - episodetitle)
                    episode_title_raw = episode_content.h2.text 
                    #Se llama al metodo que limpia el title del episodio
                    if extra_content == False:
                        episode_title = self.get_episode_title(episode_title_raw, season_number, episode_number, episodes, parent_title)
                    else:
                        episode_title = episode_title_raw
                    episode_image = self.get_image(episode_content)
                    episode_duration = self.get_duration(episode_content)
                    episode_description = episode_content.p.text.strip()
                    id_list.append(episode_id)
                    serie_episodes_count += 1
                    self.payload_episodes(episode_id, parent_id, parent_title, episode_number, season_number, episode_title, episode_duration, episode_deeplink, episode_description, episode_image, is_original)
            else:
                print("\x1b[1;31;40m >>> Episodio ya estaba scrapeado! <<< \x1b[0m") #Por lo general son movies que aparecen como episodes
        
        if serie_episodes_count != 0:
            season_list = self.get_season_episodes(season_list, parent_title, serie_deeplink, is_original)
        
        #Solo para saber la cantidad de episodios de cada serie que se scrapearon
        if len(serie_episodes) == serie_episodes_count:
            print(f"\x1b[1;32;40m >>> Todos los episodios scrapeados: {serie_episodes_count}/{len(serie_episodes)} <<< \x1b[0m")
        else:
            print(f"\x1b[1;31;40m >>> Hubo episodios no scrapeados: {serie_episodes_count}/{len(serie_episodes)} <<< \x1b[0m")
        return serie_episodes_count, season_list

    """
    Metodo que obtiene contenido que no esta en ninguna categoria, son un par de peliculas y series
    Lo que hace es recorrer toda la pagina en busca de contenido que no esta en las categorias
    Se encarga tambien de diferir que contenido es una movie y cual un episode
    """
    def scraping_extra_content(self, url, id_list):
        print("\033[33m SCRAPEANDO CONTENIDO EXTRA \033[0m")
        extra_count = 0
        i = 1
        for pages in range(999):
            extra_deeplinks = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + f'&page={i}'
            soup = self.try_soup(extra_deeplinks)
            all_content = soup.find("div",{"class":"video-container"})
            extra_content = all_content.find_all("article")
            while all_content.find("p").text != "No videos were found.": #Significa que llegamos al final de TODA la pagina y ya no hay mas contenido
                for extra in extra_content:
                    extra_deeplink = url + extra.a["href"]
                    extra_id = extra_deeplink.split("/")[-1]
                    if extra_id not in id_list: #Verificamos que no haya sido visitado el contenido extra con el id_list
                        extra_duration = self.get_duration(extra)
                        extra_title = extra.h2.text
                        #Limpiamos trailers, promos, cortos, contenido que no corresponde a movies o episodes, son muchas "and" pero es la unica manera que no traiga cualquier cosa
                        if (extra_duration != 0 and ".mp4" not in extra_title and "Promo" not in extra_title and "Trailer" not in extra_title and 
                            "Factory TV" not in extra_title and "New in" not in extra_title and "New In" not in extra_title and "New This Month" not in extra_title): 
                            soup = self.try_soup(extra_deeplink)
                            aside = soup.find("aside",{"id":"sidebar"})
                            if aside.find("h2"):
                                if aside.find("h2").text == "Series": #Si se cumple el if, es que encontramos la serie a la que pertenece el episode
                                    #De esta manera scrapeamos la serie directamente y ya quedan todos los epis scrapeados de entrada
                                    serie_deeplink = url + aside.a["href"]
                                    serie_title = aside.img["title"]
                                    serie_id = self.id_hash(serie_deeplink, serie_title)
                                    if serie_id not in id_list: #Verificamos que no haya sido visitado la serie extra con el id_list
                                        series_category = None
                                        extra_content = True
                                        is_original = None
                                        self.scraping_series(series_category, url, serie_deeplink, serie_title, serie_id, extra_content, id_list, is_original) #Se encuentra la serie padre y se llama al metodo
                                        extra_count += 1
                                else:
                                    if ": " in extra_title: #Todos los episodios sueltos tienen ": ", por lo que no se scrapean debido a que seria mucho mas codigo por unos cuantos episodios
                                        break
                                    else:
                                        movie_deeplink = extra_deeplink
                                        movie_id = movie_deeplink.split("/")[-1]
                                        movies_category = None
                                        is_original = None
                                        self.scraping_movies(movie_deeplink, movies_category, movie_id, id_list, is_original) #Se scrapea la movie de la misma manera que las que estan en las categorias
                                        Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)
                                        extra_count += 1
                            else:
                                break
                i += 1
                break
        print(f"\033[33m >>> Contenidos extra: {extra_count} <<< \033[0m")

    """
    METODOS QUE SIRVEN PARA CONSEGUIR DATOS ESPECIFICOS
    No hice metodos para TODOS los datos del payload ya que algunos datos se sacan en solo 1 linea de codigo
    Hice los metodos para obtener los datos que abarcan varias lineas
    """

    #Metodo (largo xd) que se encarga de limpiar el title de cualquier episode de manera correcta ya que vienen de muchas maneras
    def get_episode_title(self, episode_title_raw, season_number, episode_number, episodes, parent_title): 
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
            if 'The Jerry Lewis Show: 1957-62 TV Specials' in episode_title_raw: #harcodeado porque tiene un formato muy complicado
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
    
    #Metodo que se encarga de generar ids hasheados ?)
    def id_hash(self, deeplink, title):
        id_hash = str(deeplink) + str(title)    
        id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()
        return id_hash

    #Metodo que se encarga de verificar que la pagina a la que hacer soup esta disponible (grax Dan)
    def get_status_code(self, deeplink):        
        response_link = requests.get(deeplink)
        if response_link.status_code == 200:
            return response_link
        else:          
            count = 0
            print(f"\x1b[1;31;40m >>> Status Code: {str(response_link.status_code)} <<< \x1b[0m")
            while count != 10 and response_link.status_code != 200:    
                time.sleep(5)                                    
                response_link = requests.get(deeplink)
                count += 1
            return response_link

    #Metodo que se encarga de realizar el soup a la url indicada
    def soup(self, deeplink):
        self.get_status_code(deeplink) #Primero se intenta ingresar a la pagina
        response = self.currentSession.get(deeplink)
        content = response.text
        soup = BS(content, "lxml")
        return soup

    #Metodo que se encarga de reintentar las requests por errores de conexion (cortes de internet)
    def try_soup(self, deeplink):
        try:
            soup = self.soup(deeplink)
        except requests.exceptions.ConnectionError:
            print("\x1b[1;31;40m >>> Error de conexion, reintentando... <<< \x1b[0m")
            time.sleep(15)
            soup = self.soup(deeplink)
        return soup
    
    #Metodo que se encarga de obtener la duracion, si los segundos son mayores a 29 le agrega 1 minuto a la duracion
    def get_duration(self, content):
        if int(content.time.text.split(':')[2]) > 29:
            duration = int(content.time.text.split(':')[1]) + 1
        else:
            duration = int(content.time.text.split(':')[1])
        return duration

    #Metodo que se encarga de obtener la imagen, el try es debido a que a veces no tienen imagen
    def get_image(self, content):
        try:
            image = [content.img["src"]]
        except:
            image = None
        return image

    def get_season_episodes(self, season_list, parent_title, serie_deeplink, is_original):
        """
        Esto se encarga de crear correctamente una lista con los valores de cada season y la cantidad de epis de esa season
        Trae las seasons asi: ej ["1:10", "2:15"]; donde 1:10 hace referencia a que la season 1 tiene 10 epis
        No es muy practico pero funciona perfectamente. 
        """
        seasons_number = []
        seasons_payload = []
        seasons_list = []
        for season in season_list:
            if season not in seasons_number:
                seasons_number.append(season)
                season_episodes_list = str(season) + ":" + str(season_list.count(season))
                seasons_payload.append(season_episodes_list)
        for season in seasons_payload:
            season = self.payload_seasons(season, parent_title, serie_deeplink, is_original)
            seasons_list.append(season)
        return seasons_list

    #Metodo que se encarga de llenar el payload de cada season para las series
    def payload_seasons(self, season, parent_title, serie_deeplink, is_original):
        season_number = int(season.split(":")[0])
        season_episodes = int(season.split(":")[1])
        season_title = parent_title + ": Season " + str(season_number)
        season_id = self.id_hash(serie_deeplink, parent_title)
        return {
            "Id":           season_id,           #Importante
            "Synopsis":     None,     #Importante
            "Title":        season_title,        #Importante, E.J. The Wallking Dead: Season 1
            "Deeplink":     serie_deeplink,    #Importante
            "Number":       season_number,       #Importante
            "Year":         None,         #Importante
            "Image":        None, 
            "Directors":    None,   #Importante
            "Cast":         None,        #Importante
            "Episodes":     season_episodes,      #Importante
            "IsOriginal":   is_original    
        }

    #PAYLOADS
    def payload_movies(self, movie_id, movie_title, movie_duration, movie_deeplink, movie_description, movie_image, movie_dubbed, movies_category, is_original):
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
            "Subtitles":     None,
            "Dubbed":        movie_dubbed,      
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        movies_category,    #Important!      
            "Cast":          None,      
            "Directors":     None,    #Important!      
            "Availability":  None,     #Important!      
            "Download":      None,      
            "IsOriginal":    is_original,    #Important!      
            "IsAdult":       None,    #Important!   
            "IsBranded":     None,    #Important!   
            "Packages":      [{"Type": "free-vod"}],    #Obligatorio      
            "Country":       None,      
            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
            "CreatedAt":     self._created_at, #self._created_at #Obligatorio
        }
        #print(payload_movie)
        Datamanager._checkDBandAppend(self, payload_movie, self.list_db_movies_shows, self.payloads_movies)

    def payload_series(self, serie_title, serie_deeplink, serie_id, episodes_seasons, serie_description, serie_image, series_category, is_original):
        payload = {
            "PlatformCode":  self._platform_code, #Obligatorio      
            "Id":            serie_id,            #Obligatorio
            "Seasons":       episodes_seasons, #Unicamente para series
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
            "IsOriginal":    is_original,               #Important!      
            "IsAdult":       None,               #Important!   
            "IsBranded":     None,               #Important!   
            "Packages":      [{"Type": "free-vod"}],            #Obligatorio      
            "Country":       None,      
            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
            "CreatedAt":     self._created_at, #self._created_at   #Obligatorio
        }
        #print(payload)
        Datamanager._checkDBandAppend(self, payload, self.list_db_movies_shows, self.payloads_shows)

    def payload_episodes(self, episode_id, parent_id, parent_title, episode_number, season_number, episode_title, episode_duration, episode_deeplink, episode_description, episode_image, is_original):
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
            "IsOriginal":    is_original,    #Important!      
            "IsAdult":       None,    #Important!   
            "IsBranded":     None,    #Important!   
            "Packages":      [{"Type": "free-vod"}],    #Obligatorio      
            "Country":       None,      
            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
            "CreatedAt":     self._created_at, #self._created_at, #Obligatorio
        }
        #print(payload_epi)
        Datamanager._checkDBandAppend(self, payload_epi, self.list_db_episodes, self.payloads_episodes, isEpi=True)