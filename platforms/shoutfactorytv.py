from gettext import find
from os import link
from socket import TIPC_LOW_IMPORTANCE
from turtle import title
from urllib import response
from wsgiref.simple_server import demo_app
from bs4 import BeautifulSoup
from httpx import get
import requests 
import time
import requests
import pymongo
import re
from handle.replace import _replace
from common import config 
from datetime import datetime
from handle.mongo import mongo
from handle.datamanager import Datamanager
from updates import deeplinks
from updates.upload import Upload
import hashlib


class Shoutfactorytv():

    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.skippedEpis = 0
        self.skippedTitles = 0
        self.list_db_series_movies = Datamanager._getListDB(self, self.titanScraping)
        self.list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        self.payloads = []  #Se agrega los payloads de series y movies
        self.payloads_episodes = []
        #Url para encontrar la información de los contenidos por separado
        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",}
        self.testing = True
        self._scraping()
    
    def _scraping(self, testing=False):
        self.url = "https://www.shoutfactorytv.com"
        response = requests.get(self.url) #Enviamos una solicitud a la pag
        content = response.text #Lo transforma en texto 
        soup = BeautifulSoup(content, 'lxml') #Transformar un doc. HTML o XML en un árbol complejo de objetos Python
        section = soup.find_all("div", {"class", "drop-holder"}) #Por categorias
        #movies_categories = section[0]
        #series_categories = section[1]

        #self.get_movies(movies_categories)
        #self.get_series(series_categories)

        self.get_content_episodes()

    # Scripts para traer todas las peliculas
    '''def get_movies(self, movies_categories): #Contiene el tag con la info de las categorias
        categ = movies_categories.find_all("a") #Le pido que encuentre el tag 'a' 
        for item in categ: #Itero cada tag 'a' de cada pelicula
            url_categ = self.url + item['href'] #Hago que en cada item encuentre el tag 'href'
            print(url_categ)
            content = requests.get(url_categ) #Le hago una solicitud a la pag.
            self.get_content_movies(content)
            print('---------------------URL MOVIES--------------------')'''

    #Traemos la informacion de las series
    def get_series(self, series_categories):
        categ_serie = series_categories.find_all("a")
        for i in categ_serie:
            url_categ_serie = self.url + i['href'] #Links de las categorias
            print(url_categ_serie)
            contenido = requests.get(url_categ_serie)       
            self.get_content_series(contenido)
            #self.get_content_episodes(contenido)
            print('------------URL SERIES-----------')
    
    #Esta funcion busca la clase img_holder en donde se encuentra el title, deeplink y src.
    def get_data(self, content):
        content_movies = BeautifulSoup(content.text, 'lxml')
        img_holder_data = content_movies.find_all('div', attrs={'class': 'img-holder'})
        return img_holder_data
    
    def get_content_movies(self, content):
        content_movies = self.get_data(content)
        list_id_movies = []
        for movie in content_movies:
            deeplink = self.get_deeplink(movie)
            title = self.get_title_and_imagen(deeplink)['title']
            src = self.get_title_and_imagen(deeplink)['imagen'] 
            duration = self.get_duration_and_synopsis(deeplink)['duration']
            synopsis = self.get_duration_and_synopsis(deeplink)['synopsis']

            id = self.generate_id(title, deeplink)

            if id not in list_id_movies:
                list_id_movies.append(id)
                self.get_payload_movies(title, id, deeplink, src, synopsis, duration)

    def get_title_and_imagen(self, deeplink):
        url_contenido = 'https://www.shoutfactorytv.com/videos?utf8=✓&commit=submit&q=1&q={deeplink}'.format(deeplink = deeplink.split("/")[-1])   
        data = requests.get(url_contenido)
        soup = BeautifulSoup(data.text, 'lxml')
        img_holder = soup.find_all('div', {'class', 'img-holder'})
        for movie in img_holder:
            title = self.get_title(movie)
            imagen = self.get_src(movie)
            return {"title": title, "imagen": imagen}
            
    def get_content_series(self, content):
        content_series = self.get_data(content)
        list_id_series = []
        self.list_series = []
        for serie in content_series:
            title = self.get_title(serie)
            deeplink = self.get_deeplink(serie)
            src = self.get_src(serie)
            synopsis = self.get_synopsis_serie(serie)
            id = self.generate_id(title, deeplink)

            episodios = self.get_content_episodes(deeplink)


            self.list_series.append({'title': self.get_title(serie),'id': self.generate_id(deeplink, serie)})
            print(self.list_series)

            if id not in list_id_series:
                list_id_series.append(id)
                self.get_payload_series(title, id, deeplink, src, synopsis) 

    def get_content_episodes(self, deeplink):
        url_content = deeplink
        data = requests.get(url_content)
        soup = BeautifulSoup(data.text, 'lxml')
        link_episodes = soup.find_all('div', {'class', 'img-holder'})
        for items in link_episodes:
            title = self.get_title(items)
            imagen = self.get_src(items)
            deeplink = self.get_deeplink_episodes(deeplink)
            number_season = self.get_season_and_number(deeplink)['season']
            number_episode = self.get_season_and_number(deeplink)['episode']
            duration = self.get_duration_and_synopsis(deeplink)['duration']
            synopsis = self.get_duration_and_synopsis(deeplink)['synopsis']
            parentId = self.get_parent_id(title) 
            parentTitle = self.get_parent_title(title) #o serie 

            print(title)
            print(imagen)
            print(deeplink)
            print(number_season)
            print(number_episode)
            print(duration)
            print(synopsis)
            print(parentId)
            print(parentTitle)

    #Se busca el title de cada movie y serie
    def get_title(self, data):
        if data.img != None:
            title = data.img.get('title')
        else:
            title = data.a.get('href').split('/')[1].replace('-', ' ').capitalize
        return title

    #Se busca el link de cada movie y serie
    def get_deeplink(self, data):
        if data.a != None:
            deeplink = data.a
            deeplink = self.url + deeplink['href']
        else: 
            deeplink = None
        return deeplink

    #Se busca la imagen de cada movie y serie
    def get_src(self, data):
        if data.img != None:
            src = data.img.get('src')
        else:
            src = None
        return src  
    
    def generate_id(self, title, deeplink):
        id_hash = str(title) + str(deeplink)    
        id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()
        return id_hash

    #Se busca synopsis de series
    def get_synopsis_serie(self, content):
        series = self.get_data(content)
        for serie in series:
            deeplink = self.get_deeplink(serie)
            response = requests.get(deeplink)
            soup = BeautifulSoup(response.text, 'html.parser')
            s2 = soup.find_all('div', attrs={'id': 'info-slide-content'})
            for data in s2:
                synopsis = data.p.text
                return synopsis

    #Se encuenta la duracion y la sinopsis de movies
    def get_duration_and_synopsis(self, deeplink):
        url_content = 'https://www.shoutfactorytv.com/videos?utf8=✓&commit=submit&q=1&q={deeplink}'.format(deeplink = deeplink.split("/")[-1])   
        data = requests.get(url_content)
        soup = BeautifulSoup(data.text, 'lxml')
        article = soup.find_all('article', {'class': 'post'})
        for item in article:
            duration = int(item.time.text.split(" ")[1].split(":")[0])
            synopsis = item.find('p').text.strip()
            return {"duration": duration, "synopsis": synopsis}

    def get_deeplink_episodes(self, deeplink):
        url_contenido = deeplink
        data_url = requests.get(url_contenido)
        soup = BeautifulSoup(data_url.text, 'lxml')
        link_episodes = soup.find_all('ul', {'class', 'thumbnails'})
        for items in link_episodes:
            deep_link = items.find_all('li')
            for i in deep_link:
                deeplink = i.a
                deeplink = self.url + deeplink['href']

    def get_season_and_number(self, deeplink):
        url_content = deeplink
        data = requests.get(url_content)
        soup = BeautifulSoup(data.text, 'lxml')
        link_episodes = soup.find_all('div', {'class', 'caption'})
        for item in link_episodes:
            span_content = item.find_all('span')[1]
            for i in span_content:
                season =  int(i.split(",")[0].split(":")[1].strip())
                episode = int(i.split(",")[1].split(":")[1].strip())
                return {"season": season, "episode": episode} 

    def get_parent_id(self, title):
        for i in self.list_series:
            if title == i['title']:
                parentId = i['id']
                return parentId 
            
    def get_parent_title(self, title):
        title = self.get_title(title)
        return title 

    def get_payload_movies(self, title, id, deeplink, src, synopsis, duration):
        payload_movies= {
                        "PlatformCode":  self._platform_code, #Obligatorio      
                        "Id":            id, #Obligatorio
                        "Title":         title, #Obligatorio      
                        "CleanTitle":    _replace(title), #Obligatorio      
                        "OriginalTitle": None,                          
                        "Type":          "movie",     #Obligatorio      
                        "Year":          None,     #Important!     
                        "Duration":      duration,      
                        "ExternalIds":   None,      
                        "Deeplinks": {          
                            "Web":       deeplink,       #Obligatorio          
                            "Android":   None,          
                            "iOS":       None,      
                        },      
                        "Synopsis":      synopsis,      
                        "Image":         src,      
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
                        "CreatedAt":     self._created_at #Obligatorio
        }
        Datamanager._checkDBandAppend(self, payload_movies, self.list_db_series_movies, self.payloads)
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)
    
    def get_payload_series(self, title, id, deeplink, src, synopsis):
        payload_series = {
                        "PlatformCode":  self._platform_code, #Obligatorio      
                        "Id":            id,            #Obligatorio
                        "Seasons":       None, #DEJAR EN NONE, se va a hacer al final cuando samuel diga
                        "Title":         title,         #Obligatorio      
                        "CleanTitle":    _replace(title), #Obligatorio      
                        "OriginalTitle": None,                          
                        "Type":          "serie",            #Obligatorio      
                        "Year":          None,               #Important!     
                        "Duration":      None,      
                        "ExternalIds":   None,      
                        "Deeplinks": {          
                            "Web":       deeplink,     #Obligatorio          
                            "Android":   None,          
                            "iOS":       None,      
                        },      
                        "Synopsis":      synopsis,      
                        "Image":         src,      
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
                        "CreatedAt":     self._created_at   #Obligatorio
        }
        Datamanager._checkDBandAppend(self, payload_series, self.list_db_series_movies, self.payloads)
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)

    def get_payload_episodes(self, payload_episodes):
        payload_episodes = {
                                "PlatformCode":  self._platform_code, #Obligatorio      
                                "Id":            None, #Obligatorio
                                "ParentId":      None, #Obligatorio #Unicamente en Episodios
                                "ParentTitle":   None, #Unicamente en Episodios 
                                "Episode":       None, #Obligatorio #Unicamente en Episodios  
                                "Season":        None, #Obligatorio #Unicamente en Episodios
                                "Title":         None, #Obligatorio           
                                "OriginalTitle": None,                                
                                "Year":          None,     #Important!     
                                "Duration":      None,      
                                "ExternalIds":   None,      
                                "Deeplinks": {          
                                    "Web":       None,       #Obligatorio          
                                    "Android":   None,          
                                    "iOS":       None,      
                                },      
                                "Synopsis":      None,      
                                "Image":         None,      
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
                                "CreatedAt":     self._created_at, #Obligatorio
        }
        Datamanager._checkDBandAppend(self, payload_episodes, self.payloads_db, self.payloads, isEpi=True)
        Datamanager._insertIntoDB(self, self.payloads_episodes, self.titanScrapingEpisodios)
        
