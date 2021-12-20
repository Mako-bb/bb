from bs4 import BeautifulSoup
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
from updates.upload import Upload


class Shoutfactorytv():

    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.skippedEpis = 0
        self.skippedTitles = 0

        #### URL ####
        #self._url = self._config['url'] 
        #self._url_movies = self._config['url_movies']
        #self._url_shows = self._config['url_shows']

        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}

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

    # Scripts para traer todas las peliculas
    def get_movies(self):
        url_movies = 'https://www.shoutfactorytv.com/film' #Contiene el link de movies
        response = requests.get(url_movies) #Enviamos una solicitud a la pag
        content = response.text #Lo transforma en texto 
        soup = BeautifulSoup(content, 'lxml')
        #content = soup.find_all("div", {"class", "movies-list"}) #Por categorias
        
        data = soup.find("div", {"class", "tab-content add film"}) #All movies
        title = data.find_all("img", {"title"}).get_text() #All movies


    def get_payload_movies(self, payload_movies):
        payload_movies= {
                        "PlatformCode":  self._platform_code, #Obligatorio      
                        "Id":            None, #Obligatorio
                        "Title":         None, #Obligatorio      
                        "CleanTitle":    _replace(None), #Obligatorio      
                        "OriginalTitle": None,                          
                        "Type":          "movie",     #Obligatorio      
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
                        "Packages":      [{"Type":"subscription-vod"}],    #Obligatorio      
                        "Country":       None,      
                        "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                        "CreatedAt":     self._created_at #Obligatorio
    }

    def get_payload_series(self, payload_series):
        payload_series = {
                        "PlatformCode":  self._platform_code, #Obligatorio      
                        "Id":            None,            #Obligatorio
                        "Seasons":       None, #DEJAR EN NONE, se va a hacer al final cuando samuel diga
                        "Title":         None,         #Obligatorio      
                        "CleanTitle":    _replace(None), #Obligatorio      
                        "OriginalTitle": None,                          
                        "Type":          "serie",            #Obligatorio      
                        "Year":          None,               #Important!     
                        "Duration":      None,      
                        "ExternalIds":   None,      
                        "Deeplinks": {          
                            "Web":       None,     #Obligatorio          
                            "Android":   None,          
                            "iOS":       None,      
                        },      
                        "Synopsis":      None,      
                        "Image":         None,      
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