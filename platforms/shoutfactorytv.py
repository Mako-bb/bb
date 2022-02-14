# -*- coding: utf-8 -*-
from pydoc import synopsis
import time
from tkinter import W
from webbrowser import get
from pymongo import response
import requests
import hashlib   
import pymongo 
import re
from requests.api import request
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from bs4                    import BeautifulSoup as BS
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class Shoutfactorytv():
    """
    Shoutfactorytv es una ott de Estados Unidos.

    DATOS IMPORTANTES:
    - Versión Final: Si.
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: NO.
    - ¿Usa BS4?: Sí.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? 04/02/2022 -> 50 minutos
    - ¿Cuanto contenidos trajo la ultima vez? TS:163 TSE: 960 07/10/21

    OTROS COMENTARIOS:
        Contenia series sin episodios, se modificó el script para excluirlas.
        #########################################################################
                                        | |           (_)          
            ___ ___  _ __ ___   ___ _ __ | |_ __ _ _ __ _  ___  ___ 
            / __/ _ \| '_ ` _ \ / _ \ '_ \| __/ _` | '__| |/ _ \/ __|
            | (_| (_) | | | | | |  __/ | | | || (_| | |  | | (_) \__ \
            \___\___/|_| |_| |_|\___|_| |_|\__\__,_|_|  |_|\___/|___/

        Si se corta es por conexión
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        self._created_at = time.strftime("%Y-%m-%d")

        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.parent_ids = {}

        self.payloads = []
        self.payloads_episodes = []
        self.payloads_db = Datamanager._getListDB(self, self.titanScraping)
        self.payloads_episodes_db = Datamanager._getListDB(self, self.titanScrapingEpisodios)

        self.skippedEpis = 0
        self.skippedTitles = 0
        self._url = self._config['url']

        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}
                    
        self._payload_template = { 
            "PlatformCode":     None, #Obligatorio   
            "Id":               None, #Obligatorio
            "Seasons":          None,
            "Title":            None, #Obligatorio      
            "CleanTitle":       None, #Obligatorio      
            "OriginalTitle":    None,          
            "Type":             None,     #Obligatorio  #movie o serie     
            "Year":             None,     #Important!  1870 a año actual  
            "Duration":         None,     #en minutos   
            "ExternalIds":      "list", #*      
            "Deeplinks": {
                "Web":          "str",       #Obligatorio          
                "Android":      "str",          
                "iOS":          "str",      
            },
            "Synopsis":         "str",      
            "Image":            "image",    
            "Rating":           "str",     #Important!      
            "Provider":         "list",      
            "Genres":           None,    #Important!      
            "Cast":             "list",    #Important!  
            "Directors":        "list",    #Important!      
            "Availability":     "str",     #Important!      
            "Download":         "bool",      
            "IsOriginal":       "bool",    #Important!  
            "IsAdult":          "bool",    #Important!   
            "IsBranded":        "bool",    #Important!   (ver link explicativo)
            "Packages": [
                    {
                        "asd":"asd"
                    }
            ],    #Obligatorio      
            "Country":          "list",
            "Timestamp":        "str", #Obligatorio
            "CreatedAt":        self._created_at #Obligatorio
        }

        self._payload_episode_template = {      
            "PlatformCode":  "str", #Obligatorio      
            "Id":            "str", #Obligatorio
            "ParentId":      "str", #Obligatorio #Unicamente en Episodios
            "ParentTitle":   "str", #Unicamente en Episodios 
            "Episode":       int(),#Obligatorio #Unicamente en Episodios  
            "Season":        int(), #Obligatorio #Unicamente en Episodios
            "Title":         "str", #Obligatorio      
            "OriginalTitle": "str",                          
            "Year":          "int",     #Important!     
            "Duration":      "int",      
            "ExternalIds":   [], 
            "Deeplinks": {          
                "Web":       "str",       #Obligatorio          
                "Android":   "str",          
                "iOS":       "str",      
            },      
            "Synopsis":      "str",      
            "Image":         list(),     
            "Subtitles": "list",
            "Rating":        "str",     #Important!      
            "Provider":      None,      
            "Genres":        None,    #Important!      
            "Cast":          None,    #Important!        
            "Directors":     None,    #Important!      
            "Availability":  "str",     #Important!      
            "Download":      False,      
            "IsOriginal":    True,    #Important!      
            "IsAdult":       None,    #Important!   
            "IsBranded":     "bool",    #Important!   (ver link explicativo)
            "Packages":      "list",    #Obligatorio      
            "Country":       [],      
            "Timestamp":     "str", #Obligatorio      
            "CreatedAt":     "str", #Obligatorio 
        }

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
        ## ACA COMIENZA EL SCRAPREO
        content = requests.get(self._url)
        self.get_payload(type='movie', page=content)
        self.get_payload(type='serie', page=content)        # Tambien
        Upload(self._platform_code, self._created_at, testing=self.testing)

    def get_category_link(self, content):
        return self._url + content['href']

    def get_title(self, content):
        return content['title']
    
    def get_image(self, content):
        return content['src']

    def get_deeplink(self, content):
        return self._url + content.find('a')['href']

    def get_id(self, title, deeplink):
        return hashlib.md5((title + deeplink).encode('utf-8')).hexdigest()
    
    def get_soup_from_category(self, category):
        category_page = requests.get(category)
        soup = BS(category_page.content, 'html.parser')
        return soup.find_all('div', class_='img-holder')

    def get_synopsis(self, deeplink):
        page_content = requests.get(deeplink)
        soup = BS(page_content.content, 'html.parser')
        try:
            return soup.find('p').getText()
        except:
            return None

    def get_content_soup(self, page):
        soup = BS(page.content, 'html.parser')
        return soup.find_all('div', class_='divRow') 

    def get_number_of_episodes_from_season(self, deeplink, season):
        page = requests.get(deeplink)
        soup = BS(page.content, "html.parser")
        temp  = soup.find_all("div", class_="caption")
        for episode in temp:
            episode_season = episode.find('span')[1].text
            print(episode_season)

        return 0

    def get_number_of_seasons(self, deeplink):
        page = requests.get(deeplink)
        soup = BS(page.content, 'html.parser')
        temp = soup.find_all("ul", class_="tabset series")
        number_of_season = temp[0].find_all('a')

        return len(number_of_season)

    def get_episode_info(self, deeplink):
        #seasons = self.get_number_of_seasons(deeplink)
        page = requests.get(deeplink)
        soup = BS(page.content, "html.parser")
        temp  = soup.find_all("div", class_="caption")

        info = list()

        for episode in temp:
            episode_info = episode.find_all('span')[1].text
            info.append(episode_info.split())
        
        return info

    def test_function(self, deeplink):
        page = requests.get(deeplink)
        self.get_number_of_seasons(page)

    def get_season_info(self, id, title, image, deeplink):
        seasons = self.get_number_of_seasons(deeplink)

        season_list = []
        for season_number in seasons:
            season_info = { 
                "Id":                       id, #"str",           #Importante
                "Synopsis":                 None,
                "Title":                    title + ": Season " + str(season_number),          #title #Importante, E.J. The Wallking Dead: Season 1
                "Deeplink":                 deeplink, #"str",    #Importante
                "Number":                   season_number, 
                "Year": "int",              #Importante
                "Image":                    image, 
                "Directors":                [],
                "Cast":                     [],          #Importante
                "Episodes":                 self.get_number_of_episodes_from_season(deeplink, season_number),
                "IsOriginal":               True
            }

        season_list.append(season_info)
        print(season_list)

        return season_list

    def get_duration_for_movie(self, deeplink, title):
        movie_id = deeplink.split("/")[-1]

        search_info = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + movie_id 
        page = requests.get(search_info)
        content = page.text

        soup = BS(content, 'lxml')
        search_movie_soup = soup.find("div",{"class":"video-container"})
        if search_movie_soup == None:
            return 0
        movies = search_movie_soup.find_all("article")
        for movie in movies: 
            if movie_id == (movie.a["href"].split("/")[-1]):
                if int(movie.time.text.split(':')[2]) > 29:
                    return int(movie.time.text.split(':')[1]) + 1
                else:
                    return int(movie.time.text.split(':')[1])

    def get_duration_and_synopsis_for_episode(self, href):
        search_info = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q=' + href 
        page = requests.get(search_info)
        content = page.text
        soup = BS(content, 'lxml')
        search_episode_soup = soup.find('div', class_='video-container')
        temp = search_episode_soup.find('time')

        try:
            if int(temp.text.split(':')[2]) > 29:
                duration = int(temp.text.split(':')[1]) + 1
            else: 
                duration = int(temp.text.split(':')[1])
        except:
            duration = None
        
        synopsis = search_episode_soup.find('p').text.lstrip()
        return duration, synopsis


    def get_payload(self, type, page):
        if page.status_code == 200:
            categories = self.get_content_soup(page)
        else:
            print("Error volver a correr el script")
            return

        if type == 'movie':
            temp = categories[0]
        elif type == 'serie':
            temp = categories[1]
            
        content_category = temp.find_all('a') 
        content_dicc = {}

        for content in content_category:
            category_link = self.get_category_link(content)                           # obtenemos el enlace de cada categoria para empezar a scrapear peliculas en ese orden
            temp = self.get_soup_from_category(category_link) 

            for elem in temp:
                content_info = elem.find('img')
                
                if content_info == None:
                    break

                title = self.get_title(content_info)                                    # título de la película
                deeplink = self.get_deeplink(elem)                                      # deeplink a la película
                duration = self.get_duration_for_movie(deeplink, title)                 # obtenemos la duración de la pelicula en base al deeplink y al título 
                image = self.get_image(content_info)                                    # imagen promocional
                id = self.get_id(title, deeplink)                                       # id generado con hashlib con md5, estoy seguro que con el título 
                                                                                        # y el deeplink no va a generar uno repetido
                if id not in content_dicc:
                    content_dicc[id] = True

                    if type == 'movie':
                        synopsis = self.get_synopsis(deeplink)                                  # la siguiente request es para conseguir                                   
                        payload_movies = self._payload_template.copy()
                        genre = str(content.text)
    
                        payload_movies["PlatformCode"] =                self._platform_code
                        payload_movies["Id"] =                          id
                        del payload_movies["Seasons"]                    # Eliminamos seasons de movies
                        payload_movies["Title"] =                       title
                        payload_movies["CleanTitle"] =                  _replace(title)
                        payload_movies["Type"] =                        "movie"
                        payload_movies["Duration"] =                    duration
                        payload_movies["Deeplinks"]["Web"] =            deeplink
                        payload_movies["Synopsis"] =                    synopsis
                        payload_movies["Image"] =                       image
                        payload_movies["Genre"] =                       genre
                        payload_movies["Timestamp"] =                   datetime.now().isoformat()
                        payload_movies["CreatedAt"] =                   self._created_at
                        payload_movies["Packages"] =                    [{"Type":"free-vod"}]
                        Datamanager._checkDBandAppend(self, payload_movies, self.payloads_db, self.payloads)
                    elif type == 'serie':
                        payload_serie = self._payload_template.copy()
                        self.parent_ids[title] = id
                        payload_serie["PlatformCode"] =                 self._platform_code
                        payload_serie["Id"] =                           id                    
                        payload_serie["Seasons"] =                      self.get_number_of_seasons(deeplink)
                        payload_serie["Title"] =                        title
                        payload_serie["CleanTitle"] =                   _replace(title)
                        payload_serie["Type"] =                         "serie"             
                        payload_serie["Deeplinks"]["Web"] =             deeplink
                        payload_serie["Image"] =                        image
                        payload_serie["Genre"] =                        content.text
                        payload_serie["Timestamp"] =                    datetime.now().isoformat()
                        payload_serie["CreatedAt"] =                    self._created_at
                        payload_serie["Packages"] =                     [{"Type":"free-vod"}]                          

                        Datamanager._checkDBandAppend(self, payload_serie, self.payloads_db, self.payloads)
                        self.get_payload_episodes(title, id, content, deeplink)
        Datamanager._insertIntoDB(self, self.payloads_db, self.titanScraping)

    def get_payload_episodes(self, parentTitle, parentId, genre, deeplink):
        serie = requests.get(deeplink)
        soup = BS(serie.content, 'lxml')

        deeplinks = soup.find_all('div', class_='holder')
        deeplinks = deeplinks[1].find_all('a')
        titles = soup.find_all('span', class_="title")
        serie_info = self.get_episode_info(deeplink)

        for i in range(0, len(titles)):
            href = deeplinks[i]['href']
            duration, synopsis = self.get_duration_and_synopsis_for_episode(href)
            payload_episode = self._payload_episode_template.copy()
            payload_episode["PlatformCode"]     = self._platform_code
            payload_episode["Id"]               = self.get_id(titles[i].text, deeplink)
            payload_episode["ParentId"]         = parentId
            payload_episode["ParentTitle"]      = parentTitle
            payload_episode["Episode"]          = int(serie_info[i][3])
            payload_episode["Season"]           = int(re.sub(r"[^0-9]", '', serie_info[i][1]))
            payload_episode["Title"]            = str(titles[i].text)
            payload_episode["OriginalTitle"]    = str(titles[i].text)
            payload_episode["Year"]             = int(1999)
            payload_episode["Duration"]         = duration
            payload_episode["Deeplinks"]["Web"] = "https://www.shoutfactorytv.com" + href 
            payload_episode["Synopsis"]         = synopsis
            payload_episode["Genre"]            = genre.text
            payload_episode['Packages']         = [{"Type":"free-vod"}]
            payload_episode['Country']          = ["US"]
            payload_episode["Timestamp"]        = datetime.now().isoformat()
            payload_episode["CreatedAt"]        = self._created_at
            Datamanager._checkDBandAppend(self, payload_episode, self.payloads_episodes_db, self.payloads_episodes, isEpi = True)
        Datamanager._insertIntoDB(self, self.payloads_episodes_db, self.titanScrapingEpisodios)
