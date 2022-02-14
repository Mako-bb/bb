############## IMPORTS ##############
from nturl2path import url2pathname
from operator import index
import time
import requests
import hashlib
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
            self.movies_urls = []
            self.epis_urls = []
            self.contador = 0
            
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

        print("Obteniendo peliculas")
        self.get_payload_movies()
        
        print("Obteniendo series")
        self.get_payload_serie()       

    # Agregar timer
    # Chequear payloads repetidos
    # titanScraping: total 1530 (Marian)
    # titanScrapingEpisodes: total 5375 (Marian)
    # Movies: 1440 Series: 124 (Yop)


############## PAYLOAD MOVIES ##############
    def get_payload_movies(self):
        self.url_validator = []
        payloads = []
        #list_db = Datamanager._getListDB(self, self.titanScraping)
        for items in self.movies_list:
            for item in items:
                self.get_deeplink(item)
                self.validar_repetidos()

        contador = 0
        for url in self.url_validator:
            self.movies_urls.append(url)
            print('----------------------------------')
            print(str(contador) + ' / ' + str(len(self.url_validator)))
            r = requests.get(url, 'html.parser')
            self.s2 = BS(r.text, 'html.parser')
            print('    LOADING.....................')
            time.sleep(1)
            payload_movie = {
                "PlatformCode":  self._platform_code, #Obligatorio      
                "Title":         self.get_title(url), #Obligatorio      
                "CleanTitle":    self.get_title(url), #Obligatorio 
                "Id":            self.get_id(), #Obligatorio     
                "OriginalTitle": None,                          
                "Type":          "movie",     #Obligatorio      
                "Year":          None,     #Important!     
                "Duration":      self.get_duration(url),      
                "ExternalIds":   None,      
                "Deeplinks": {          
                    "Web":       str(url),     #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },      
                "Synopsis":      self.get_syn(),      
                "Image":         self.get_image(),      
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
                "Packages":      [{"Type":"free-vod"}],    #Obligatorio      
                "Country":       None,      
                "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                "CreatedAt":     self._created_at #Obligatorio
                }
            payloads.append(payload_movie)
            contador += 1
                        
        '''Datamanager._checkDBandAppend(self, payload_movie, list_db, payloads)
        Datamanager._insertIntoDB(self, payloads, self.titanScraping)'''

    ############## PAYLOAD SERIES ##############
    def get_payload_serie(self):
        self.url_validator = []
        payloads_series = []
        #list_db_series = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        for items in self.url_series:
            r = requests.get(items, 'html.parser')
            self.s1 = BS(r.text, 'html.parser')
            time.sleep(1)
            print('    LOADING.....................')
            series = self.s1.find('div', {"id": "main"}).find('div', {'class': 'holder'}).find_all('div', {'class': 'img-holder'})
            for item in series:
                print('----------------------------------')
                self.get_deeplink(item)
                self.validar_repetidos()

        contador = 0
        for url in self.url_validator:

            print('----------------------------------')
            print(str(contador) + ' / ' + str(len(self.url_validator)))
            print(url)
            r = requests.get(url, 'html.parser')
            self.s3 = BS(r.text, 'html.parser')
            print('    LOADING.....................')
            time.sleep(1)
            payload_serie = {
                "PlatformCode":     self._platform_code, #Obligatorio   
                "Seasons":          [ #Unicamente para series
                    {
                    "Id":           None,           #Importante
                    "Synopsis":     None,     #Importante
                    "Title":        None,        #Importante, E.J. The Wallking Dead: Season 1
                    "Deeplink":     None,    #Importante
                    "Number":       None,       #Importante
                    "Year":         None,         #Importante
                    "Image":        None, 
                    "Directors":    None,   #Importante
                    "Cast":         None,        #Importante
                    "Episodes":     None,      #Importante
                    "IsOriginal":   None    #packages
                    },
                ],
                "Crew":          [ #Importante
                    {
                        "Role": 'str', 
                        "Name": 'str'
                    },
                ],
                "Title":         self.get_title(url), #Obligatorio      
                "CleanTitle":    self.get_title(url), #Obligatorio 
                "Id":            self.get_id(), #Obligatorio     
                "OriginalTitle": None,                          
                "Type":          None,     #Obligatorio  #movie o serie     
                "Year":          None,     #Important!  1870 a año actual   
                "Duration":      None,     #en minutos   
                "ExternalIds":   None,       
                "Deeplinks": {
                    "Web":       str(url), #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },
                "Synopsis":      self.get_syn(),      
                "Image":         self.get_image(),      
                "Subtitles":     None,
                "Dubbed":        None,
                "Rating":        None,     #Important!      
                "Provider":      None,      
                "Genres":        None,    #Important!      
                "Cast":          None,    #Important!        
                "Directors":     None,    #Important!      
                "Availability":  None,     #Important!      
                "Download":      None,      
                "IsOriginal":    None,    #Important!        
                "IsAdult":       None,    #Important!   
                "IsBranded":     None,    #Important!   (ver link explicativo)
                "Packages":      [{"Type":"free-vod"}],    #Obligatorio      
                "Country":       None,
                "Timestamp":     datetime.now().isoformat(), #Obligatorio
                "CreatedAt":     self._created_at, #Obligatorio
                }
            payloads_series.append(payload_serie)
            contador += 1
            print("---------- Obteniendo episodios ----------")
            self.get_payload_epis()
            print('----------------------------------')
        """Datamanager._checkDBandAppend(self, payload_serie, list_db_series, payloads_series)
        self.copiapayloads = [{"Id":pay["Id"], "CleanTitle":pay["CleanTitle"].lower().strip()} for pay in payloads_series]
        Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)"""


    ############## PAYLOAD EPISODES ##############
    def get_payload_epis(self):
        print(len(self.url_validator))
        self.get_epis()
        payloads_episodes = []
        """ list_db_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios) """
        for item in self.epis_list:
            self.get_deeplink(item)
            self.validar_epis()

            """ r = requests.get(self.deeplink, 'html.parser')
            self.s3 = BS(r.text, 'html.parser')
            self.get_title(item)
            self.get_id()
            self.get_syn()
            self.get_duration()
            self.get_image() """
            

            """ payload_epi = {
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
                "Packages":      [{"Type":"free-vod"}],    #Obligatorio      
                "Country":       None,      
                "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                "CreatedAt":     self._created_at, #Obligatorio
            } """
        """ Datamanager._checkDBandAppend(self, payload_epi, list_db_episodes, payloads_episodes)
        Datamanager._insertIntoDB(self, payloads_episodes, self.titanScraping) """
    

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
            url = self.deeplink.split('/')
            if url[-1] not in self.url_validator:
                print('---------- Agregando contenido ----------')
                self.url_validator.append(self.deeplink)
                print(len(self.url_validator))
                print('----------------------------------')
            else:
                print('---------- Contenido repetido ----------')
                print('----------------------------------')
        else:
            print('---------- Contenido repetido ----------')
            print('----------------------------------')


    def get_title(self, url):
        try:
            self.title = self.s2.find('div', {"id": "main"}).find('div', {'class': 'holder'}).find('h2').find('span').get_text().strip()
        except:
            try:
                self.title = self.s3.find('div', {"id": "main"}).find('h1').get_text().strip()
            except:
                self.title = url.replace('https://www.shoutfactorytv.com/series/', '').replace('-', ' ').replace('series', '').title()
        print(self.title)
        return self.title


    def get_deeplink(self, item):
        self.deeplink = self._url + item.a.get('href')
        print(self.deeplink)
        return self.deeplink


    def get_syn(self):
        try:
            syn = self.s2.find('div', {"id": "main"}).find('div', {'class': 'holder'}).find('p').get_text().strip()
        except:
            try:
                syn = self.s3.find('div', {"id": "main"}).find('div', {'class': 'visual add'}).find('div', {'id' : 'info-slide'}).find('p').get_text().strip()
            except:
                syn = None
        print(syn)
        return syn

    
    def get_image(self):
        # Buscar la imagen de las películas junto con la duración
        try:
            try:
                img = self.finder_soup.find('div', {'id' : 'main'}).find('div', {'class' : 'img-holder'}).img.get('src')
            except:
                img = self.s3.find('div', {"id": "main"}).find('div', {'class' : 'visual add'}).img.get('src')
        except:
            img = None
        print(img)
        return img


    def get_duration(self, url):
        # Buscar película con el id del deeplink
        fin = url.split('/')[-1]
        finder = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q={0}'.format(fin)
        r = requests.get(finder, 'html.parser')
        self.finder_soup = BS(r.text, 'html.parser')
        duration = self.finder_soup.find('div', {'id' : 'main'}).find('div', {'class' : 'holder'}).find('time').get_text().split(':')
        if int(duration[-1]) >= 30:
            duration = int(duration[1]) + 1
        else:
            duration = int(duration[1])
        print(duration)


    def get_id(self):
        id = hashlib.md5((self.title + self.deeplink).encode('utf-8')).hexdigest()
        print(id)
        return id


    def get_epis(self):
        self.epis_list = self.s3.find('div', {'id' : 'main'}).find('div', {'class' : 'tab-content'}).find_all('li')
        print(self.epis_list)
    

    def validar_epis(self):
        for url in self.movies_urls:
            if self.deeplink == url:
                print('Es una película! Contenido descartado')
                continue
            else:
                self.epis_urls.append(self.deeplink)