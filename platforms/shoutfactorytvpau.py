############## IMPORTS ##############
import time
from urllib import response
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
            self.title_validator = []
            self.contador = 0
            self.repe = 0
            
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
        print("*********** Obteniendo categorías ***********")
        _data = requests.get(self._url)
        self.get_categories(_data)
        self.movies_list = []
        print('*********** Haciendo las request *************')
        for url in self.url_movies:
            req_movies = requests.get(url, 'lxml')
            soup_category = BS(req_movies.text, 'lxml')
            soup_movies = soup_category.find_all('div', {'class':'img-holder'})
            self.movies_list.append(soup_movies)
        
        print("Obteniendo series")
        self.get_payload_serie()

        print("Obteniendo peliculas")
        self.get_payload_movies()

        print("Obteniendo series")
        self.get_payload_serie()        

        self.get_payload_epis()

    # Agregar timer
    # Chequear validador de repetidos
    # Obtener la duración
    # Intentar la obtención de la duración
    # titanScraping: total 1530
    # titanScrapingEpisodes: total 5375
    # Movies: 1422 Series: 124


############## PAYLOAD MOVIES ##############
    def get_payload_movies(self):
        self.url_validator = []
        payloads = []
        #list_db = Datamanager._getListDB(self, self.titanScraping)
        for items in self.movies_list:
            print('***** ITEMS *****') 
            print(items)
            for item in items:
                self.get_deeplink(item)
                self.validar_repetidos()
        for movie in self.url_validator:
            r = requests.get(movie, 'html.parser')
            self.s2 = BS(r.text, 'html.parser')
            print('    LOADING.....................')
            time.sleep(1)
            self.get_title(movie)
            print('***** ITEM *****')
            print(movie)
            payload_movie = {
                "PlatformCode":  self._platform_code, #Obligatorio      
                "Id":            'None', #Obligatorio
                "Title":         self.get_title(), #Obligatorio      
                "CleanTitle":    self.get_title(), #Obligatorio      
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
                "Synopsis":      self.get_syn(),      
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
        print(self.url_validator)
        
        '''Datamanager._checkDBandAppend(self, payload_movie, list_db, payloads)
        Datamanager._insertIntoDB(self, payloads, self.titanScraping)'''

    ############## PAYLOAD SERIES ##############
    def get_payload_serie(self):
        self.url_validator = []
        payloads_series = []
        #list_db_series = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        for items in self.url_series:
            print('***** ITEMS *****')  #Muestra los urls de las categorías de las series
            print(items)
            r = requests.get(items, 'html.parser')
            self.s1 = BS(r.text, 'html.parser')
            time.sleep(1)
            print('    LOADING.....................')
            series = self.s1.find('div', {"id": "main"}).find('div', {'class': 'holder'}).find_all('div', {'class': 'img-holder'})
            print(series)
            for item in series:
                print('***** ITEM *****')
                self.get_deeplink(item)
                self.validar_repetidos()
        self.validar_titulos()
        for serie in self.url_validator:
            r = requests.get(serie, 'html.parser')
            self.s2 = BS(r.text, 'html.parser')
            print('    LOADING.....................')
            time.sleep(1)
            self.get_title()
            """payload_serie = {
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
            }"""
        raise KeyError
        """Datamanager._checkDBandAppend(self, payload_serie, list_db_series, payloads_series)
        self.copiapayloads = [{"Id":pay["Id"], "CleanTitle":pay["CleanTitle"].lower().strip()} for pay in payloads_series]
        Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)"""


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
        movies_cont = list(container[1:35])
        print("Categorías movies: " + str(len(movies_cont)))
        print('************************ Series ************************')
        series_cont = list(container[36:58])
        print("Categorías series: " + str(len(series_cont)))
        print('')
        print(' URL CATEGORÍAS MOVIES')
        self.url_movies = []
        for item in movies_cont:
            url_movies = self._url +  item.a.get('href')
            self.url_movies.append(url_movies)
        print(self.url_movies)

        print(' URL CATEGORÍAS SERIES')
        self.url_series = []
        for item in series_cont:
            url_series = self._url +  item.a.get('href')
            self.url_series.append(url_series)
        print(self.url_series)

    
    def validar_repetidos(self):
        if self.deeplink not in self.url_validator:
            self.url_validator.append(self.deeplink)
        else:
            print('---------- Contenido repetido ----------')

    
    def validar_titulos(self):
        for content in self.url_validator:
            r = requests.get(content, 'html.parser')
            self.s2 = BS(r.text, 'html.parser')
            print('    LOADING.....................')
            time.sleep(1)
            self.get_title()
            if self.title not in self.title_validator:
                self.title_validator.append(self.title)
                print(len(self.title_validator))
                self.contador += 1
            else:
                self.url_validator.remove(self.deeplink) # NOPE, BUSCÁ OTRA FORMA
            print(self.url_validator)
        raise KeyError

    def get_title(self):
        try:
            self.title = self.s2.find('div', {"id": "main"}).find('div', {'class': 'holder'}).find('h2').find('span').get_text()
        except:
            pass
        try:
            self.title = self.s2.find('div', {"id": "main"}).find('h1').get_text().replace('                                                ','')
        except:
            self.title = self.deeplink.replace('https://www.shoutfactorytv.com/series/', '').replace('-', ' ').replace('series', '').title()
        self.title_validator.append(list(dict.fromkeys(self.title)))
        print(self.title)
        return self.title

    def get_deeplink(self, item):
        self.deeplink = self._url + item.a.get('href')
        print(self.deeplink)
        return self.deeplink


    def get_syn(self):
        syn = self.s2.find('div', {"id": "main"}).find('div', {'class': 'holder'}).find('p').get_text()
        return syn

    
    def get_image(self, item):
        try:
            img = [item.img.get('src')]
        except:
            img = None
        print(img)
        return img


    def get_duration(self):
        pass       


    """ def series_title(self):
        # Usar el try except; poner los replace todos juntos; reemplazar el series del titulo
        try:
            self.stitle = self.s2.find('div', {"id": "main"}).find('h1').get_text().replace('                                                ','')
        except:
            self.stitle = self.deeplink.replace('https://www.shoutfactorytv.com/series/', '').replace('-', ' ').replace('series', '').title()
        self.title_validator.append(self.stitle)
        print(self.stitle)
        return self.stitle  """


