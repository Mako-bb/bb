from gettext import find
from os import link
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
   
        #Url para encontrar la información de los contenidos por separado
        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",}
        self.testing = True
        self._scraping()
    
    def _scraping(self, testing=False):
        payloads = []
        payloads_series = []
        list_db_series_movies = Datamanager._getListDB(self, self.titanScraping)
        list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)

        #self.get_payload_movies(movie_data)
        #self.get_payload_series(payloads_series)
        #self.get_payload_episodes(episode_data)

        self.url = "https://www.shoutfactorytv.com"
        response = requests.get(self.url) #Enviamos una solicitud a la pag
        content = response.text #Lo transforma en texto 
        soup = BeautifulSoup(content, 'lxml') #Transformar un doc. HTML o XML en un árbol complejo de objetos Python
        section = soup.find_all("div", {"class", "drop-holder"}) #Por categorias
        movies_categories = section[0]
        #series_categories = section[1]

        self.get_movies(movies_categories)
        #self.get_series(series_categories)

    # Scripts para traer todas las peliculas
    def get_movies(self, movies_categories): #Contiene el tag con la info de las categorias
        categ = movies_categories.find_all("a") #Le pido que encuentre el tag 'a' 
        for item in categ: #Itero cada tag 'a' de cada pelicula
            url_categ = self.url + item['href'] #Hago que en cada item encuentre el tag 'href'
            print(url_categ)
            content = requests.get(url_categ) #Le hago una solicitud a la pag.
            self.get_content_movies(content)
            
            print('---------------------URL MOVIES--------------------')

    #Traemos la informacion de las series
    '''def get_series(self, series_categories):
        categ_serie = series_categories.find_all("a")
        for i in categ_serie:
            url_categ_serie = self.url + i['href'] #Links de las categorias
            print(url_categ_serie)
            contenido = requests.get(url_categ_serie)   
            print(contenido)      
            self.get_content_series(contenido)

            print('------------URL SERIES-----------')'''
    
    #Esta funcion busca la clase img_holder en donde se encuentra el title, deeplink y src.
    def get_data(self, content):
        content_movies = BeautifulSoup(content.text, 'lxml')
        img_holder_data = content_movies.find_all('div', attrs={'class': 'img-holder'})
        return img_holder_data
    
    def get_content_movies(self, content):
        content_movies = self.get_data(content)
        list_deeplink = []
        for movie in content_movies:
            title = self.get_title(movie)
            deeplink = self.get_deeplink(movie)
            src = self.get_src(movie) 
            duration = self.get_duration_movies(movie)
            synopsis = self.get_synopsis_movies(movie)

            print(title)
            print(deeplink)
            print(src)
            print(duration)
            print(synopsis)

            id_hash = str(title) + str(deeplink)    
            id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()
            print(id_hash)

            #list_deeplink.append(deeplink)
            #print(list_deeplink)
            print('-------------------------') 
        #list_deeplink = self.validateList(list_deeplink) 

    '''def validateList(self, list_deeplink):
        nueva=[]
        for elemento in list_deeplink:
            if not elemento in nueva:
                nueva.append(elemento)
        return nueva    '''


    def get_content_series(self, content):
        content_series = self.get_data(content)
        for serie in content_series:
            title = self.get_title(serie)
            deeplink = self.get_deeplink(serie)
            src = self.get_src(serie)
            synopsis = self.get_synopsis_serie(serie)

            print(title)
            print(deeplink)
            print(src)
            print(synopsis)

            id_hash = str(title) + str(deeplink)    
            id_hash = hashlib.md5(id_hash.encode('utf-8')).hexdigest()
            print(id_hash)
            print('-------------------------')  

    def get_content_episodes(self, content):
        pass

    #Se busca el title de cada movie y serie
    def get_title(self, data):
            if data.img != None:
                title = data.img.get('title')
            else: 
                title = data.a.get('href').split('/')[1].replace('-', ' ').capitalize
            return title

    #Se busca el link de cada movie y serie
    def get_deeplink(self, data):
        deeplink = data.a
        deeplink = self.url + deeplink['href']
        return deeplink

    #Se busca la imagen de cada movie y serie
    def get_src(self, data):
        if data.img != None:
            src = data.img.get('src')
        else:
            src = None
        return src  
    
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
                print(synopsis)

    #Contiene el tag article en donde se encuenta la duracion y la sinopsis de movies
    def get_article_duration_and_synopsis(self, content):
        url_content =  'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q={title}'.format(title = self.get_title(content))
        data = requests.get(url_content)
        soup = BeautifulSoup(data.text, 'lxml')
        article = soup.find_all('article', {'class': 'post'})
        return article

    #Contiene la funcion de article y busca la synopsis a partir de eso
    def get_synopsis_movies(self, content):
        article_synopsis = self.get_article_duration_and_synopsis(content)
        for item in article_synopsis:
            link_synopsis = item.a
            link = self.url + link_synopsis['href'] 
            getDeeplink = self.get_deeplink(content)
            if link == getDeeplink:
                synopsis = (item.p).text
                return synopsis

    #Contiene la funcion de article y busca la duration a partir de eso
    def get_duration_movies(self, content):
        article = self.get_article_duration_and_synopsis(content)
        for item in article:
            link = item.a
            link_movies = self.url + link['href'] 
            getDeeplink = self.get_deeplink(content)
            if link_movies == getDeeplink:
                duration = (item.find('time', {'class', 'duration'}).text.split(" ")[1].split(":")[0])
                return duration 

    def get_payload_movies(self, movie):
        payload_movies= {
                        "PlatformCode":  self._platform_code, #Obligatorio      
                        "Id":            None, #Obligatorio
                        "Title":         self.get_title(movie), #Obligatorio      
                        "CleanTitle":    _replace(None), #Obligatorio      
                        "OriginalTitle": None,                          
                        "Type":          "movie",     #Obligatorio      
                        "Year":          None,     #Important!     
                        "Duration":      self.get_duration_movies(movie),      
                        "ExternalIds":   None,      
                        "Deeplinks": {          
                            "Web":       self.get_deeplink(movie),       #Obligatorio          
                            "Android":   None,          
                            "iOS":       None,      
                        },      
                        "Synopsis":      self.get_synopsis_movies(movie),      
                        "Image":         self.get_src(movie),      
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

    def get_payload_series(self):
        payload_series = {
                        "PlatformCode":  self._platform_code, #Obligatorio      
                        "Id":            None,            #Obligatorio
                        "Seasons":       None, #DEJAR EN NONE, se va a hacer al final cuando samuel diga
                        "Title":         self.get_title,         #Obligatorio      
                        "CleanTitle":    _replace(None), #Obligatorio      
                        "OriginalTitle": None,                          
                        "Type":          "serie",            #Obligatorio      
                        "Year":          None,               #Important!     
                        "Duration":      None,      
                        "ExternalIds":   None,      
                        "Deeplinks": {          
                            "Web":       self.get_deeplink,     #Obligatorio          
                            "Android":   None,          
                            "iOS":       None,      
                        },      
                        "Synopsis":      self.get_synopsis_series,      
                        "Image":         self.get_src,      
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