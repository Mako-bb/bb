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
        
        #Llamamos a los metodos que crean las listas con TODAS los generos
        all_movies_genres = self.scraping_categories_movies(url, movies_categories)
        all_series_genres = self.scraping_categories_series(url, series_categories)

        #Llamamos a los metodos de movies, series, episodes y contenido extra
        self.scraping_category_movies(movies_categories, url, id_list, all_movies_genres)
        self.scraping_category_series(series_categories, url, id_list, all_series_genres)
        self.scraping_extra_content(url, id_list)

        self.currentSession.close()

        Upload(self._platform_code, self._created_at, testing=self.testing)

    """
    Metodo que se encarga de obtener TODAS los generos para una determinada movie
    Guarda en una lista todas las peliculas con el genero donde fueron encontradas
    Despues se busca con otro metodo todas las veces que el id de la peli este en la lista 
    y se queda con el genero que aparece
    """
    def scraping_categories_movies(self, url, movies_categories):
        print("\x1b[1;33;40m OBTENIENDO IDS DE LAS PELICULAS... \x1b[0m")
        movies_from_categories = []
        for category in movies_categories:
            movies_href = self.get_href(url, category)
            movies_category = category.text
            for movie_href in movies_href:
                movie_id = movie_href["href"].split("/")[-1]
                if movie_id != "#":
                    movies_from_categories.append(movies_category + ":" + movie_id)
        print("\x1b[1;32;40m TODOS LOS IDS DE LAS PELICULAS OBTENIDOS! \x1b[0m")
        return movies_from_categories
    
    #Tiene el mismo proposito que el metodo anterior
    def scraping_categories_series(self, url, series_categories):
        print("\x1b[1;33;40m OBTENIENDO IDS DE LAS SERIES... \x1b[0m")
        series_from_categories = []
        for category in series_categories:
            series_href = self.get_href(url, category)
            series_category = category.text
            for serie_href in series_href:
                serie_deeplink = url + serie_href["href"]
                serie_title = serie_href.img["title"]
                serie_id = self.id_hash(serie_deeplink, serie_title)
                series_from_categories.append(series_category + ":" + serie_id)
        print("\x1b[1;32;40m TODOS LOS IDS DE LAS SERIES OBTENIDOS! \x1b[0m")
        return series_from_categories
    
    #Metodo que se encarga de obtener las pelis de cada categoria
    def scraping_category_movies(self, movies_categories, url, id_list, all_movies_genres):
        #print(f"\x1b[1;32;40m SCRAPEANDO PELICULAS \x1b[0m")
        for category in movies_categories: 
            movies_href = self.get_href(url, category) #Encontramos todos los href de las movies dentro de la categoria
            movies_category = category.text
            print(f"\n\033[1;36m SCRAPEANDO CATEGORIA DE PELICULAS: {movies_category} \033[0m")
            for movie_href in movies_href: #Empezamos scrapeo de cada pelicula
                if movie_href["href"] != "#": #Hay dos tag <a> que su href es "#", esos no son pelicula
                    movie_deeplink = url + movie_href["href"] #Se crea el deeplink con la url de la pagina y el href de la pelicula
                    movie_id = movie_deeplink.split("/")[-1]
                    if movie_id not in id_list: #Verificamos que no haya sido visitada la movie con el id_list
                        genres_original_list = self.get_genres(all_movies_genres, movie_id)
                        movie_genres = genres_original_list[0]
                        is_original = genres_original_list[1]
                        self.scraping_movies(movie_deeplink, movie_genres, movie_id, id_list, is_original)
            Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)
    
    #Metodo que se encarga de obtener la info de cada pelicula 
    def scraping_movies(self, movie_deeplink, movie_genres, movie_id, id_list, is_original):
        #Con la variable movie_search podemos hacer que el soup traiga solo la peli que queremos con toda la info posible
        movie_search = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + movie_id
        movies = self.get_content(movie_search)
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
                self.payload_movies(movie_id, movie_title, movie_duration, movie_deeplink, movie_description, movie_image, movie_dubbed, movie_genres, is_original)
    
    def scraping_category_series(self, series_categories, url, id_list, all_series_genres):
        #print(f"\x1b[1;32;40m SCRAPEANDO SERIES \x1b[0m")
        for category in series_categories: #Empezamos scrapeo de categorias series
            series_href = self.get_href(url, category) #Encontramos todos los href de las series dentro de la categoria
            series_category = category.text
            print(f"\n\033[1;36m SCRAPEANDO CATEGORIA DE SERIES: {series_category} \033[0m")
            for serie in series_href: #Empezamos scrapeo de cada serie
                serie_deeplink = url + serie["href"] #Se crea el deeplink con la url de la pagina y el href de la serie
                serie_title = serie.img["title"]
                serie_id = self.id_hash(serie_deeplink, serie_title)
                if serie_id not in id_list: #Verificamos que no haya sido visitada la serie con el id_list
                    extra_content = False
                    genres_original_list = self.get_genres(all_series_genres, serie_id)
                    serie_genres = genres_original_list[0]
                    is_original = genres_original_list[1]
                    self.scraping_series(serie_genres, url, serie_deeplink, serie_title, serie_id, extra_content, id_list, is_original)

    #Metodo que obtiene la info de cada serie y luego de sus episodes 
    def scraping_series(self, serie_genres, url, serie_deeplink, serie_title, serie_id, extra_content, id_list, is_original):
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
            self.payload_series(serie_title, serie_deeplink, serie_id, episodes_seasons[1], serie_description, serie_image, serie_genres, is_original)
            Datamanager._insertIntoDB(self, self.payloads_shows, self.titanScraping)
            Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)
        else:
            print("\x1b[1;31;40m >>> Serie sin episodios, se saltea <<< \x1b[0m \n")
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

    #Metodo que se encarga de scrapear los episodes de cada serie
    def scraping_episodes(self, serie_episodes, url, id_list, serie_title, serie_id, extra_content, season_list, serie_deeplink, is_original):
        serie_episodes_count = 0 #Cuenta la cantidad de episodios de la serie
        for episodes in serie_episodes: 
            episode_id = episodes.split(" ")[0]
            episode_search = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + episode_id
            episode_content = self.get_content(episode_search)
            for episode in episode_content: #Deberia encontrar solo el epi que queremos, pero por las dudas se hace que recorra todo el contenido que encuentra
                if episode_id not in id_list: #Verificamos que no haya sido visitado el episode con el id_list
                    if episode_id == (episode.a["href"].split("/")[-1]):
                        episode_deeplink = url + episode.a["href"]
                        #Con la lista que hicimos anteriormente, nos quedamos el numero de temporada y de episode
                        season_number = int(episodes.split(" ")[1])
                        episode_number = int(episodes.split(" ")[2])
                        #Podemos pasarle el parent_title y parent_id directamente
                        parent_title = serie_title
                        parent_id = serie_id
                        #Esta variable (episode_title_raw) tiene el titulo entero del episodio (del tipo: parenttitle: s1 e1 - episodetitle)
                        episode_title_raw = episode.h2.text 
                        #Se llama al metodo que limpia el title del episodio
                        if extra_content == False:
                            episode_title = self.get_episode_title(episode_title_raw, season_number, episode_number, episodes, parent_title)
                        else:
                            episode_title = episode_title_raw
                        episode_image = self.get_image(episode)
                        episode_duration = self.get_duration(episode)
                        episode_description = episode.p.text.strip()
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
    Por este metodo se retrasa unos 10 minutos la ejecucion
    """
    def scraping_extra_content(self, url, id_list):
        print("\n\x1b[1;33;40m SCRAPEANDO CONTENIDO EXTRA \x1b[0m")
        extra_count = 0
        for page in range(999): #El valor del rango no es importante, siempre y cuando sea mayor a la cantidad de pages que tiene la pagina
            extra_deeplinks = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + f'&page={page}'
            soup = self.try_soup(extra_deeplinks)
            all_content = soup.find("div",{"class":"video-container"})
            try: 
                extra_content = all_content.find_all("article")
            except AttributeError:
                print("\x1b[1;31;40m >>> Attribute Error <<< \x1b[0m")
                time.sleep(5)
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
                                    #De esta manera scrapeamos la serie directamente y ya quedan todos los epis scrapeados de una vez
                                    serie_deeplink = url + aside.a["href"]
                                    serie_title = aside.img["title"]
                                    serie_id = self.id_hash(serie_deeplink, serie_title)
                                    if serie_id not in id_list: #Verificamos que no haya sido visitado la serie extra con el id_list
                                        serie_genres = None
                                        extra_content = True
                                        is_original = None
                                        self.scraping_series(serie_genres, url, serie_deeplink, serie_title, serie_id, extra_content, id_list, is_original) #Se encuentra la serie padre y se llama al metodo
                                        extra_count += 1
                                else:
                                    if ": " in extra_title: #Todos los episodios sueltos tienen ": ", por lo que no se scrapean debido a que seria mucho mas codigo por unos cuantos episodios
                                        break
                                    else:
                                        movie_deeplink = extra_deeplink
                                        movie_id = movie_deeplink.split("/")[-1]
                                        movie_genres = None
                                        is_original = None
                                        self.scraping_movies(movie_deeplink, movie_genres, movie_id, id_list, is_original) #Se scrapea la movie de la misma manera que las que estan en las categorias
                                        Datamanager._insertIntoDB(self, self.payloads_movies, self.titanScraping)
                                        extra_count += 1
                            else:
                                break
                break
            else:
                break
        print(f"\n\x1b[1;33;40m >>> Contenidos extra: {extra_count} <<< \x1b[0m")

    """
    A partir de aca empiezan los metodos para conseguir datos para el payload y otros para hacer manejo de errores al hacer soup
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

    #Metodo que se encarga de verificar que la pagina a la que hacer soup esta disponible (grax Dan), retrasa la ejecución bastante pero evita que se corte el codigo
    def get_status_code(self, deeplink):        
        response = self.currentSession.get(deeplink)
        if response.status_code == 200:
            return response
        else:          
            count = 0
            print(f"\x1b[1;31;40m >>> Status Code: {str(response.status_code)} <<< \x1b[0m")
            while count != 10 and response.status_code != 200:    
                time.sleep(5)                                    
                response = self.currentSession.get(deeplink)
                count += 1
            return response

    #Metodo que se encarga de realizar el soup a la url indicada
    def soup(self, deeplink):
        response = self.get_status_code(deeplink) #Se verifica el status code de la pagina
        content = response.text
        soup = BS(content, "lxml")
        return soup

    #Metodo que se encarga de reintentar las requests por errores de conexion (corte de vpn)
    def try_soup(self, deeplink):
        try:
            soup = self.soup(deeplink)
        except requests.exceptions.ConnectionError:
            print("\x1b[1;31;40m >>> Error de conexion, reintentando... <<< \x1b[0m")
            time.sleep(15)
            soup = self.soup(deeplink)
        return soup

    #Metodo que se encarga de obtener el contenido del deeplink cuando buscamos una pelicula o episodio, se hizo un try except porque rompia por un AttributeError
    def get_content(self, deeplink):
        soup = self.try_soup(deeplink)
        all_content = soup.find("div",{"class":"video-container"})
        try: 
            content = all_content.find_all("article")
        except AttributeError:
            print("\x1b[1;31;40m >>> Attribute Error <<< \x1b[0m")
            time.sleep(5)
            content = self.get_content(deeplink)
        return content

    #Metodo que se encarga de devolver una lista con todos los href de las peliculas o series
    def get_href(self, url, category):
        deeplink_category = url + category["href"]
        soup = self.try_soup(deeplink_category)
        contents = soup.find("div",{"id":"main"})
        href_list = contents.find_all("a")
        return href_list

    #Metodo que se encarga de generar una lista con todos los generos de una pelicula o serie     
    def get_genres(self, all_content_genres, movie_id):
        genres = []
        for content in all_content_genres:
            content_id_list = content.split(":")[-1]
            if content_id_list == movie_id:
                genre = content.split(":")[0]
                if genre not in genres:
                    genres.append(genre)
        is_original = self.is_original(genres)
        return genres, is_original

    #Metodo que se encarga de determinar en base a la categoria si un contenido es original
    def is_original(self, genres):
        if "Shout! Originals" in genres or "Shout! Studios" in genres:
            is_original = True
        else:
            is_original = False
        return is_original

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
    def payload_movies(self, movie_id, movie_title, movie_duration, movie_deeplink, movie_description, movie_image, movie_dubbed, movie_genres, is_original):
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
            "Genres":        movie_genres,    #Important!      
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

    def payload_series(self, serie_title, serie_deeplink, serie_id, episodes_seasons, serie_description, serie_image, serie_genres, is_original):
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
            "Genres":        serie_genres,    #Important!      
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