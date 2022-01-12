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
            self._url = self._config['url']
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
        # Definimos los links de las páginas y con requests y bs4; traemos la data
        print("Haciendo la request")
        _data = requests.get(self._url)
        self.get_categories(_data)

        print("Obteniendo películas")
        self.get_payload_movies(_data)
        print("Obteniendo series")
        self.get_payload_serie(_data)
        self.get_payload_epis()

    # Agregar contador
    # Agregar timer
    # Agregar def/find_all de obtención de categorías, puede ser el mismo para ambos scraps
    # Agregar print de obtención de categorías


############## PAYLOAD MOVIES ##############
    def get_payload_movies(self, rec):
        contador = 0
        payloads = []
        #list_db = Datamanager._getListDB(self, self.titanScraping)
        for item in data:
            contador = contador + 1
            self.get_title(item)
            self.get_deeplink(item)
            print(contador)
        """payload_movie = {
            "PlatformCode":  self._platform_code, #Obligatorio      
            "Id":            'None', #Obligatorio
            "Title":         self.get_title(item), #Obligatorio      
            "CleanTitle":    self.get_title(item), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          "movie",     #Obligatorio      
            "Year":          None,     #Important!     
            "Duration":      None,      
            "ExternalIds":   None,      
            "Deeplinks": {          
                "Web":       self.get_deeplink(item),       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },      
            "Synopsis":      self.get_syn(item),      
            "Image":         self.get_image(item),      
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
        payloads.append(payload_movie)
    print(contador, payloads)"""
        
        '''Datamanager._checkDBandAppend(self, payload_movie, list_db, payloads)
        Datamanager._insertIntoDB(self, payloads, self.titanScraping)'''

    ############## PAYLOAD SERIES ##############
    def get_payload_serie(self):
        payloads_series = []
        list_db_series = Datamanager._getListDB(self, self.titanScrapingEpisodios)
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

        Datamanager._checkDBandAppend(self, payload_serie, list_db_series, payloads_series)
        self.copiapayloads = [{"Id":pay["Id"], "CleanTitle":pay["CleanTitle"].lower().strip()} for pay in payloads_series]
        Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)


    ############## PAYLOAD EPISODES ##############
    def get_payload_epis(self):
        payloads_episodes = []
        list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)
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
        Datamanager._checkDBandAppend(self, payload_epi, list_db_episodes, payloads_episodes)
        Datamanager._insertIntoDB(self, payloads_episodes, self.titanScraping)
    

    def get_categories(self, data):
        soup = BS(data.content, 'html.parser')
        container = soup.find('div', attrs={'class': 'nav-holder'}).find("div",{"class":"dropdown"}).find_all('li')
        print('************************ Movies ************************')
        movies_cont = list(container[1:34])
        print("Categorías movies: " + str(len(movies_cont)))
        print('************************ Series ************************')
        series_cont = list(container[36:58])
        print("Categorías series: " + str(len(series_cont)))
        
        print(' URL CATEGORÍAS MOVIES')
        for item in movies_cont:
            self.url_movies = self._url +  item.a.get('href')
            print(self.url_movies)

            for url in self.url_movies:
                rec = requests.get(url)
            return rec


        print(' URL CATEGORÍAS SERIES')
        for item in series_cont:
            self.url_series = self._url +  item.a.get('href')
            print(self.url_series)
            
            for url in self.url_series:
                req = requests.get(url)
            return req

    
    def get_title(self, item):
        '''<a href="/the-autobiography-of-miss-jane-pittman/61523b6119228800013878c6">
        <img alt="The Autobiography Of Miss Jane Pittman" height="318" src="https://gvupload.zype.com/53c0457a69702d4d66040000/video_image/6173001ebb45210001a94008/1634926622/original.png?1634926622" 
        title="The Autobiography Of Miss Jane Pittman" width="220"/>
        </a>'''
        title = item.img.get('title')
        print(title)

    def get_deeplink(self, item):
        self.deeplink = self._movies_url + item.div.li.a.get('href')
        print(self.deeplink)


    def get_syn(self, item):
        r = requests.get(self.deeplink)
        s2 = BS(r.content, 'html.parser')
        syn = s2.find('div', {"id": "wrapper"}).find('div', {"id": "main"}).find('div', {'class': 'holder'}).find('div', {'class': 'text-holder'}).find('p')
        print(syn)

    
    def get_image(self, item):
        img = item.img.get('src')
        print(img)







