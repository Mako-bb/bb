############## IMPORTS ##############
import time
import requests
#import pymongo
import re
#import json
from handle.replace import _replace
from common import config
from datetime import datetime
from handle.mongo import mongo
from handle.datamanager import Datamanager
from updates.upload import Upload
from bs4 import BeautifulSoup as BS


class ShoutfactorytvPau():
    """   ShoutFactoryTv  
        Para ejecutar la plataforma : python main.py ShoutfactorytvPau --c US --o testing
        Usa: BS4

Upload(self._platform_code, self._created_at, testing = self.test)

self.test = True if operation == "testing" else False

 ("div", class_ = 'swiper-wrapper')


    """


############## INNIT ##############
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
            ################# URLS  #################
            self._movies_url = self._config['movie_url']
            self._serie_url = self._config['serie_url']
            self.testing = False
            self.sesion = requests.session()
            self.headers = {"Accept": "application/json",
                            "Content-Type": "application/json; charset=utf-8"}
            
            if type == 'return':
                """
                Retorna a la Ultima Fecha
                """
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
        # Definimos los links de las páginas y con requests y bs4 traemos la data
        print("Haciendo la request")
        movie_data = requests.get(self._movies_url)
        serie_data = requests.get(self._serie_url)

        print("Obteniendo películas")
        self.get_payload_movies(movie_data)
        print("Obteniendo series")
        self.get_payload_serie(serie_data)
        self.get_payload_epis()

    # Agregar contador
    # Agregar timer
    # Agregar def/find_all de obtención de categorías, puede ser el mismo para ambos scraps
    # Agregar print de obtención de categorías


############## PAYLOAD MOVIES ##############
    def get_payload_movies(self, movie_data):
        contador = 0
        soup = BS(movie_data.content, 'html.parser')
        categorias = soup.find("div")
        print(categorias)
        raise KeyError
        payload_movie = {
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

    ############## PAYLOAD SERIES ##############
    def get_payload_serie(self):
        payload_serie = {
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


    ############## PAYLOAD EPISODES ##############
    def get_payload_epis(self):
        payload_epi = {
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
    