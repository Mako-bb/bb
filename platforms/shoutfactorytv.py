#IMPORTANTE: LA EJECUCION TARDA UN POCO MAS DE 5 HORAS DEBIDO A LOS EPIS
#FALTA HACER UNOS CAMBIOS Y OPTIMIZAR EL CODIGO, PERO LA "BASE" ES ESTA
#TITANSCRAPING: APROX 1518
#TITANSCRAPINGEPISODE: APROX 5290
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

class Shoutfactorytv():
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
        movies_count = 0
        series_count = 0
        episodes_count = 0
        movies_deeplinks = []
        series_deeplinks = []
        url = 'https://www.shoutfactorytv.com' # URL principal de la pagina
        response = requests.get(url)
        content = response.text
        soup = BS(content, 'lxml') # Hacemos soup a la URL principal
        #print(soup.prettify())
        categories = soup.find_all("div",{"class":"divRow"})
        movies_categories = categories[0].find_all("a") # Encontrar todos los links de las categorias movies
        series_categories = categories[1].find_all("a") # Encontrar todos los links de las categorias series
        """"""
        print("SCRAPEANDO MOVIES")
        for category in movies_categories: # Empieza scrapeo de cada categoria de movies
            deeplink_category = url + category["href"] # Concatenamos la url original con el link especifico de cada categoria
            print(f'SCRAPEANDO CATEGORIA MOVIES: {deeplink_category}')
            response = requests.get(deeplink_category)
            content = response.text
            soup = BS(content, 'lxml') # Hacemos soup a cada category
            movies = soup.find("div",{"id":"main"})
            movies_href = movies.find_all("a") # Encontramos todos los links de las movies
            for movie in movies_href: # Empezamos a scrapear cada movie
                if movie["href"] != "#": # Hay dos tag <a> que su href es "#", esos no son movies
                    deeplink_movie = url + movie["href"]
                    #print(deeplink_movie)
                    if deeplink_movie not in movies_deeplinks:
                        try:
                            movie_image = movie.img["src"]
                        except:
                            movie_image = None
                        movie_id = deeplink_movie.split("/")[4]
                        #print(movie_id)
                        response = requests.get(deeplink_movie)
                        content = response.text
                        soup = BS(content, 'lxml') # Hacemos soup a cada movie
                        try:
                            movie_title = movie.img["title"]
                        except:
                            movie_title = soup.find("span").text
                        #print(movie_title)
                        movie_description = soup.find("p").text
                        #print(movie_description)
                        movies_count = movies_count + 1
                        movies_deeplinks.append(deeplink_movie)

                        payload_movie = {
                            "PlatformCode":  self._platform_code, #Obligatorio      
                            "Id":            movie_id, #Obligatorio
                            "Title":         movie_title, #Obligatorio      
                            "CleanTitle":    _replace(movie_title), #Obligatorio      
                            "OriginalTitle": None,                          
                            "Type":          "movie",     #Obligatorio      
                            "Year":          None,     #Important!     
                            "Duration":      None,      
                            "ExternalIds":   None,      
                            "Deeplinks": {          
                                "Web":       deeplink_movie,       #Obligatorio          
                                "Android":   None,          
                                "iOS":       None,      
                            },      
                            "Synopsis":      movie_description,      
                            "Image":         movie_image,      
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
                            "Packages":      [{"Type":"subscription-vod"}],    #Obligatorio      
                            "Country":       None,      
                            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                            "CreatedAt":     None, # self._created_at #Obligatorio
                        }
                        #print(payload_movie)
        
        print("SCRAPEANDO SERIES")
        for category in series_categories: # Empieza scrapeo de cada categoria de series
            deeplink_category = url + category["href"]
            print(f"SCRAPEANDO CATEGORIA SERIES: {deeplink_category}")
            response = requests.get(deeplink_category)
            content = response.text
            soup = BS(content, 'lxml')
            series = soup.find("div",{"id":"tab1"})
            series_href = series.find_all("a")
            for serie in series_href: # Empezamos scrapeo de cada serie
                deeplink_serie = url + serie["href"]
                print(f"SCRAPEANDO EPISODIOS DE: {deeplink_serie}")
                if deeplink_serie not in series_deeplinks:
                    try:
                        serie_image = serie.img["src"]
                    except:
                        serie_image = None
                    #serie_id = deeplink_serie.split("/")[4]
                    #print(serie_id)
                    serie_title = serie.img["title"]
                    response = requests.get(deeplink_serie)
                    content = response.text
                    soup = BS(content, 'lxml')
                    #print(serie_title)
                    #serie_description = soup.find("p").text
                    #print(serie_description)
                    #SERIE IMAGE
                    serie_seasons = soup.find("li",{"class":"last"}).text.split(" ")[1].strip()
                    print(f'TEMPORADAS: {serie_seasons}')
                    series_count = series_count + 1
                    series_deeplinks.append(deeplink_serie)
                
                    payload = {
                            "PlatformCode":  self._platform_code, #Obligatorio      
                            "Id":            None,            #Obligatorio
                            "Seasons":       None, # serie_seasons #DEJAR EN NONE, se va a hacer al final cuando samuel diga
                            "Title":         serie_title,         #Obligatorio      
                            "CleanTitle":    _replace(serie_title), #Obligatorio      
                            "OriginalTitle": None,                          
                            "Type":          "serie",            #Obligatorio      
                            "Year":          None,               #Important!     
                            "Duration":      None,      
                            "ExternalIds":   None,      
                            "Deeplinks": {          
                                "Web":       deeplink_serie,     #Obligatorio          
                                "Android":   None,          
                                "iOS":       None,      
                            },      
                            "Synopsis":      None,      
                            "Image":         serie_image,      
                            "Rating":        None,               #Important!      
                            "Provider":      None,      
                            "Genres":        None,    #Important!      
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
                            "CreatedAt":     None, #self._created_at   #Obligatorio
                        }
                    #print(payload)

                    episodes = soup.find_all("div",{"class":"holder"})
                    episodes_href = episodes[1].find_all("a")
                    for episode in episodes_href:
                        deeplink_episode = url + episode["href"]
                        try:
                            episode_image = episode.img["src"]
                        except:
                            episode_image = None
                        episode_id = deeplink_episode.split("/")[5]
                        #print(episode_id)
                        # episode_title es complicado porque aunque tiene un formato "estandar", tambien viene de 1000 maneras distintas
                        #try:
                            #episode_title = episode.find("span").text.split('-')[1].strip() # No todos lo titulos tienen -
                            #print(episode_title)
                        #except: 
                            #try:
                                #episode_title = episode.find("span").text.split(':')[2].strip()
                            #except:
                                #episode_title = episode.find("span").text
                            #print(episode_title)
                        parent_title = serie_title #episode.find("span").text.split(':')[0] 
                        #print(parent_title)
                        season_episode_number = episode.find_all("span")
                        #print(season_episode_number)
                        season_number = season_episode_number[1].text.split(',')[0].split(':')[1].strip()
                        #print(season_number)
                        episode_number = season_episode_number[1].text.split(',')[1].split(':')[1].strip()
                        #print(episode_number)
                        print(f'TEMPORADA: {season_number}, EPISODIO: {episode_number}')
                        response = requests.get(deeplink_episode)
                        content = response.text
                        soup = BS(content, 'lxml')
                        episode_description = soup.find("p").text
                        episodes_count = episodes_count + 1

                        payload_epi = {
                                "PlatformCode":  self._platform_code, #Obligatorio      
                                "Id":            episode_id, #Obligatorio
                                "ParentId":      None, #Obligatorio #Unicamente en Episodios
                                "ParentTitle":   parent_title, #Unicamente en Episodios 
                                "Episode":       episode_number, #Obligatorio #Unicamente en Episodios  
                                "Season":        season_number, #Obligatorio #Unicamente en Episodios
                                "Title":         None, #Obligatorio           
                                "OriginalTitle": None,                                
                                "Year":          None,     #Important!     
                                "Duration":      None,      
                                "ExternalIds":   None,      
                                "Deeplinks": {          
                                    "Web":       deeplink_episode,       #Obligatorio          
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
                                "CreatedAt":     None, #self._created_at, #Obligatorio
                            }
                        #print(payload_epi)
        
        print("TOAS LAS MOVIES, SERIES Y EPISODES SCRAPEADOS")
        print(f'TOTAL DE MOVIES SCRAPEADAS: {movies_count}')
        print(f'TOTAL DE SERIES SCRAPEADAS: {series_count}')
        print(f'TOTAL DE EPISODES SCRAPEADOS: {episodes_count}')

        # Se podria sacar el categories de cada serie y pelicula ya que aparecen en la pagina

        # DATOS SCRAPEADOS:
        # MOVIES: titulo, deeplink, image, descripcion
        # SERIES: title, deeplinlk, image, temporadas
        # EPISODES: titulo?, deeplink, image, parent_title, temporada, episodio, descripcion 