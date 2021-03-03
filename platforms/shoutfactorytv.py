# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from bs4                    import BeautifulSoup as BS
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class Shoutfactorytv():

    """  
    DATOS IMPORTANTES:
    ¿Necesita VPN? -> NO para scrapear, si para ver contenido
    ¿HTML, API, SELENIUM? -> BeautifulSoup
    Cantidad de contenidos (ultima revisión): Peliculas y series = 1227 / Episodios = 4609
    Tiempo de ejecucíon de Script = una banda / 3-4 horas
    """
    
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']
        self.currentSession = requests.session()
        self.payloads = []
        self.payloads_epi = []
        self.payloads_db = Datamanager._getListDB(self, self.titanScraping)
        self.payloads_epi_db = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        self.skippedTitles = 0
        self.skippedEpis = 0
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
                    
            self._scraping()
        
        if type == 'scraping':
            self._scraping()
        if type == 'testing':
            self._scraping(testing=True)

    def __query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}

        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at,
        }

        find_filter.update(extra_filter)

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )

        query = {item[field] for item in query}

        return query

    def _scraping(self, testing=False):

        main_url = "https://www.shoutfactorytv.com"
        main_request = self.currentSession.get(main_url)
        soup = BS(main_request.text, features="lxml")

        category_container = soup.find_all("div",{"class":"drop-holder"})
        category_movies = category_container[0].find_all("a")
        category_series = category_container[1].find_all("a")

        for category in category_movies:
            request_category = self.currentSession.get(main_url+category["href"])
            soup_category = BS(request_category.text, features="lxml")
            
            #la pagina tiene un ul para cada hilera de peliculas, las traemos como lista
            movie_row = soup_category.find_all("ul",class_="thumbnails add film")
            #por cada ul tenemos que traer los li que vienen dentro
            for ul in movie_row:
                movie_list = ul.find_all("li")
                #y cada li vendria siendo una peli y de ahi sacamos la data.
                for movie in movie_list:
                    movie_title = movie.div.img["title"]
                    movie_deeplink = main_url+movie.div.a["href"]
                    movie_id = hashlib.md5(movie_deeplink.encode('utf-8')).hexdigest()
                    movie_image = [movie.div.img["src"]]

                    #aqui buscamos el nombre de la peli en la pagina ya que es la unicaforma de extraer la duracion
                    url_search_movie = "https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q="+movie_title
                    request_url_movie = self.currentSession.get(url_search_movie)
                    soup_search = BS(request_url_movie.text, features="lxml")
                    
                    #traemos una lista para todos los resultados de la busqueda
                    items = soup_search.find_all("article",class_="post")
                    #y si el nombre del resultado en la busqueda coincide con 
                    #el titulo de la pelicula que sacamos en la categoria sacamos los datos
                    for item in items:
                        if item.find("h2").text == movie_title:
                            movie_synopsis = item.find("p").text.strip()
                            movie_duration = int(item.find("time").text.split(": ")[-1].split(":")[0])
                    package = [{"Type": "free-vod"}]

                    #Limpieza titulo, hubo que hacerla aquí para no romper el codigo
                    if "[Dubbed]" in movie_title:
                        movie_title = movie_title.split("[Dubbed]")[0].strip()
                    
                    if "[Subtitled]" in movie_title:
                        movie_title = movie_title.split("[Subtitled]")[0].strip()
                    
                    if "[Audio Commentary]" in movie_title:
                        movie_title = movie_title.split("[Audio Commentary]")[0].strip()
                    
                    if "[VHS Vault]" in movie_title:
                        movie_title = movie_title.split("[VHS Vault]")[0].strip()

                    #asignamos si las peliculas son originales o no
                    if (category.text == "Shout! Originals") or  (category.text == "Shout! Studios"):
                        original_movie = True
                    else:
                        original_movie = False

                    movie_genre = category.text
                    
                    #limpiamos el genero
                    if movie_genre == "TokuSHOUTsu":
                        movie_genre = "tokusatsu"

                    payload = {
                        "PlatformCode":  self._platform_code, #Obligatorio      
                        "Id":            movie_id, #Obligatorio
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
                        "Synopsis":      movie_synopsis,      
                        "Image":         movie_image,      
                        "Rating":        None,     #Important!      
                        "Provider":      None,      
                        "Genres":        [movie_genre],    #Important!      
                        "Cast":          None,      
                        "Directors":     None,    #Important!      
                        "Availability":  None,     #Important!      
                        "Download":      None,      
                        "IsOriginal":    original_movie,    #Important!      
                        "IsAdult":       None,    #Important!   
                        "IsBranded":     None,    #Important!   
                        "Packages":      package,    #Obligatorio      
                        "Country":       None,      
                        "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                        "CreatedAt":     self._created_at, #Obligatorio
                    }
                    Datamanager._checkDBandAppend(self, payload, self.payloads_db, self.payloads)

        for category in category_series:
            request_category = self.currentSession.get(main_url+category["href"])
            soup_category = BS(request_category.text, features="lxml")

            #la pagina tiene un ul para cada hilera de serie, las traemos como lista
            serie_row = soup_category.find_all("ul",class_="thumbnails add")
            #por cada ul tenemos que traer los li que vienen dentro
            for ul in serie_row:
                serie_list = ul.find_all("li")
                #y cada li vendria siendo una serie y de ahi sacamos la data.
                for serie in serie_list:
                    serie_title = serie.div.img["title"]
                    serie_deeplink = main_url+serie.div.a["href"]
                    serie_id = hashlib.md5(serie_deeplink.encode('utf-8')).hexdigest()
                    serie_image = [serie.div.img["src"]]
                    
                    serie_genre = category.text

                    if serie_genre == "TokuSHOUTsu":
                        serie_genre = "tokusatsu"

                    #asignamos si las peliculas son originales o no
                    if serie_genre == "Shout! Originals":
                        original_serie = True
                    else:
                        original_serie = False

                    #hacemos request a la serie para sacar synopsis, temporadas y episodios
                    request_serie = self.currentSession.get(serie_deeplink)
                    soup_serie = BS(request_serie.text, features="lxml")
                    if soup_serie.find("div",{"id":"info-slide"}):
                        serie_synopsis = soup_serie.find("div",{"id":"info-slide"}).p.text.strip()
                    else:
                        serie_synopsis = None
                    #Sacamos los datos de las temporadas
                    container_seasons = soup_serie.find("ul",class_="tabset series")
                    #este es un contador para sumar el tab de temporada y pasar al siguiente
                    tab_season_counter = 0
                    for season in container_seasons.find_all("li"):
                        season_title = serie_title+": "+season.a.text
                        season_id = hashlib.md5(season_title.encode('utf-8')).hexdigest()

                        #if al que entra si las temporadas de la serie estan separas en distintos tab, Ej: "Season 1","Season 1"
                        if len(season_title.split(" ")[-1]) < 3:
                            season_list = []
                            season_num = int(season_title.split(" ")[-1])
                            tab_season = soup_serie.find("div",{"id":"tab"+str(tab_season_counter)})
                            episode_quantity = len(tab_season.find_all("div",class_="caption"))

                            season_list.append({
                            "Id": season_id,                   #Importante
                            "Synopsis": None,                  #Importante   
                            "Title": season_title,             #Importante, E.J. The Wallking Dead: Season 1 
                            "Deeplink":  None,                 #Importante
                            "Number": season_num,              #Importante
                            "Year": None,                      #Importante
                            "Image": None, 
                            "Directors": None,                 #Importante
                            "Cast": None,                      #Importante
                            "Episodes": episode_quantity,      #Importante 
                            "IsOriginal": None    
                            })
                        
                        
                        #if al que entra si las temporadas de la serie estan juntos en un mismo tab, Ej: "Season 1-3","Season 10-11",
                        else:
                            season_list = None
                        tab_season_counter = tab_season_counter+1
                        
                    
                    payload = {
                        "PlatformCode":  self._platform_code, #Obligatorio      
                        "Id":            serie_id,            #Obligatorio
                        "Seasons":       season_list,
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
                        "Synopsis":      serie_synopsis,      
                        "Image":         serie_image,      
                        "Rating":        None,               #Important!      
                        "Provider":      None,      
                        "Genres":        [serie_genre],    #Important!      
                        "Cast":          None,      
                        "Directors":     None,               #Important!      
                        "Availability":  None,               #Important!      
                        "Download":      None,      
                        "IsOriginal":    original_serie,               #Important!      
                        "IsAdult":       None,               #Important!   
                        "IsBranded":     None,               #Important!   
                        "Packages":      package,            #Obligatorio      
                        "Country":       None,      
                        "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                        "CreatedAt":     self._created_at,   #Obligatorio
                    }
                    Datamanager._checkDBandAppend(self, payload, self.payloads_db, self.payloads)
                    
                    #la pagina tiene un ul para cada hilera de episodios, las traemos como lista
                    episode_row = soup_serie.find_all("ul",class_="thumbnails add series series")
                    #por cada ul tenemos que traer los li que vienen dentro, que son los episodios
                    for ul in episode_row:
                        episode_list = ul.find_all("li")
                        #y cada li vendria siendo una serie y de ahi sacamos la data.
                        for episode in episode_list:
                            episode_title = episode.span.text
                            sea_epi_container = episode.find_all("span")[1].text.strip()
                            sea_epi_split = sea_epi_container.split(",")
                            season_episode = int(sea_epi_split[0].split(" ")[-1])
                            num_episode = int(sea_epi_split[-1].split(" ")[-1])
                            episode_image = [episode.img["src"]]
                            episode_deeplink = main_url+episode.a["href"]
                            episode_id = hashlib.md5(episode_deeplink.encode('utf-8')).hexdigest()
                            url_search_episode = "https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q="+episode_title
                            request_url_episode = requests.get(url_search_episode)
                            #hay que hacer un request para sacar la synopsis y la duracion
                            soup_search_episode = BS(request_url_episode.text, features="lxml")
                            items = soup_search_episode.find_all("article",class_="post")
                            for item in items:
                                if item.find("h2").text == episode_title:
                                    synopsis_episode = item.find("p").text.strip()
                                    duration_episode = int(item.find("time").text.split(": ")[-1].split(":")[0])

                            #limpieza de titulo, hubo que hacerla aqui para no romper el codigo
                            #primero limpiamos por " - " ya que es un factor comun
                            if " - " in episode_title:
                                episode_title = episode_title.split(" - ")[-1]

                            #limpiamos titulo power ranger, Ej: Everyone Appears!! (aka Everyone's Here!!)
                            if " (aka" in episode_title:
                                episode_title = episode_title.split(" (aka")[-1].replace(")","").strip()
                            
                            #limpiamos titulos de capitulos que contengan el titulo de la serie en el
                            if serie_title+": " in episode_title:
                                episode_title = episode_title.split(serie_title+": ")[-1]

                            #fix de un capitulo repetido mal agregado en la pagina
                            if (episode_id == "8dd44940b8144776b119c446d7512a27") and (serie_title == "Father Knows Best"):
                                continue

                            payload_epi = {
                                "PlatformCode":  self._platform_code, #Obligatorio      
                                "Id":            episode_id, #Obligatorio
                                "ParentId":      serie_id, #Obligatorio #Unicamente en Episodios
                                "ParentTitle":   serie_title, #Unicamente en Episodios 
                                "Episode":       num_episode, #Obligatorio #Unicamente en Episodios  
                                "Season":        season_episode, #Obligatorio #Unicamente en Episodios
                                "Title":         episode_title, #Obligatorio      
                                "CleanTitle":    _replace(episode_title), #Obligatorio      
                                "OriginalTitle": None,                          
                                "Type":          "episode",     #Obligatorio      
                                "Year":          None,     #Important!     
                                "Duration":      duration_episode,      
                                "ExternalIds":   None,      
                                "Deeplinks": {          
                                    "Web":       episode_deeplink,       #Obligatorio          
                                    "Android":   None,          
                                    "iOS":       None,      
                                },      
                                "Synopsis":      synopsis_episode,      
                                "Image":         episode_image,      
                                "Rating":        None,     #Important!      
                                "Provider":      None,      
                                "Genres":        [serie_genre],    #Important!      
                                "Cast":          None,      
                                "Directors":     None,    #Important!      
                                "Availability":  None,     #Important!      
                                "Download":      None,      
                                "IsOriginal":    None,    #Important!      
                                "IsAdult":       None,    #Important!   
                                "IsBranded":     None,    #Important!   
                                "Packages":      package,    #Obligatorio      
                                "Country":       None,      
                                "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                                "CreatedAt":     self._created_at, #Obligatorio
                            }
                            Datamanager._checkDBandAppend(self, payload_epi, self.payloads_epi_db, self.payloads_epi, isEpi=True)

        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)
        Datamanager._insertIntoDB(self, self.payloads_epi, self.titanScrapingEpisodios)

        Upload(self._platform_code, self._created_at,testing=testing)
