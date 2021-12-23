import time
from bs4.element import ProcessingInstruction
import requests
from handle.replace import _replace
from common import config
from datetime import datetime
from bs4 import BeautifulSoup as BS
from handle.mongo import mongo
from handle.datamanager import Datamanager
from updates.upload import Upload 
import hashlib

class Shoutfactorytv():

    def __init__(self, ott_site_uid, ott_site_country, type):
        self.url="https://www.shoutfactorytv.com/"
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

        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}
        self.list_db  = Datamanager._getListDB(self, self.titanScraping)
        self.list_db_epi=Datamanager._getListDB(self, self.titanScrapingEpisodios)

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
        response= requests.get(self.url)
        soup= BS(response.text, 'html.parser')

        categorias=soup.find_all("div",{"class","drop-holder"})

        pelis_cat_links= categorias[0].find_all("a")
        series_cat_links=categorias[1].find_all("a")

        self.categories_pelis_links= self.get_link_categories(pelis_cat_links)
        self.categories_pelis_list=self.get_lista_categories(pelis_cat_links)

        self.categories_series_links= self.get_link_categories(series_cat_links)
        self.categories_series_list=self.get_lista_categories(series_cat_links)
              

        self.get_payload_movies(self.categories_pelis_links,self.categories_pelis_list)
        self.get_payload_serie(self.categories_series_links,self.categories_series_list)

        Upload(self._platform_code, self._created_at, testing = self.testing)

    def get_payload_movies(self,links,list):
        self.payloads = []
        for categories in links:
            indice=0
            web_categories= requests.get(self.url+categories)
            movies_list=BS(web_categories.text,"html.parser")
            movies_list=movies_list.find_all("div",{"class","img-holder"})
            for movie in movies_list:
                self.payload_movie(movie,list[indice])
            indice+1

        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)

    def get_payload_serie(self,links,list):
        self.payloads=[]
        for categories in links:
            indice=0
            web_categories= requests.get(self.url+categories)
            series_list=BS(web_categories.text,"html.parser")
            series_list=series_list.find_all("div",{"class","img-holder"})
            for serie in series_list:
                self.payload_serie(serie,list[indice])
                self.get_payload_episodes(serie.a["href"],list[indice])
            indice+1
            print("Serie scrapeada")
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)
        

    def get_payload_episodes(self,serie_link,genero):
        self.payload_epi=[]
        web_serie=requests.get(self.url+serie_link)
        serie_soup=BS(web_serie.text,"html.parser")
        episode_list=serie_soup.find_all("ul",{"class","thumbnails add series series"})
        for episode_line in episode_list:
            episodes=episode_line.find_all("a")
            for episode in episodes:
                self.payload_episodes(episode,genero)



    def payload_movie(self, movie,genero):
        str_to_hash=str(movie.img['title'])+ str(self.url+movie.a['href'])
        id=hashlib.md5(str_to_hash.encode('utf-8')).hexdigest()
        payload_contenidos = { 
            'PlatformCode':  self._platform_code, #Obligatorio   
            "Id":            str(id), #Obligatorio
            "Crew":          None,
            "Title":         movie.img['title'], #Obligatorio      
            "CleanTitle":    _replace(movie.img['title']), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          'movie',     #Obligatorio  #movie o serie     
            "Year":          None,     #Important!  1870 a año actual   
            "Duration":      None,     #en minutos   
            "ExternalIds":   None,    
            "Deeplinks": {
                "Web":       self.url+movie.a['href'],       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },
            "Synopsis":      self.get_sinopsis(self.url+movie.a['href']),      
            "Image":         movie.img['src'],      
            "Subtitles":     None,
            "Dubbed":        None,
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        genero,    #Important!      
            "Cast":          None,    #Important!        
            "Directors":     None,    #Important!      
            "Availability":  None,     #Important!      
            "Download":      None,      
            "IsOriginal":    None,    #Important!        
            "IsAdult":       None,    #Important!   
            "IsBranded":     None,    #Important!   (ver link explicativo)
            "Packages":      [{'Type': 'tv-everywhere'}],    #Obligatorio      
            "Country":       None,
            "Timestamp":     datetime.now().isoformat(), #Obligatorio
            "CreatedAt":     self._created_at, #Obligatorio
            }
        Datamanager._checkDBandAppend(self, payload_contenidos, self.list_db, self.payloads)

    def payload_serie(self,serie,genero):
        payload_contenido_series = { 
            "PlatformCode":  self._platform_code, #Obligatorio   
            "Id":            str(self.get_id(serie.img['title'],self.url+serie.a['href'])), #Obligatorio
            "Seasons":       [ #Unicamente para series
                                None
            ],
            "Crew":          [ #Importante
                                None
            ],
            "Title":         serie.img['title'], #Obligatorio      
            "CleanTitle":    _replace(serie.img['title']), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          'serie',     #Obligatorio  #movie o serie     
            "Year":          None,     #Important!  1870 a año actual   
            "Duration":      None,      
            "ExternalIds":   None,       
            "Deeplinks": {
                "Web":       self.url+serie.a['href'],       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },
            "Synopsis":      self.get_sinopsis(self.url+serie.a['href']),      
            "Image":         serie.img['src'],      
            "Subtitles":     None,
            "Dubbed":        None,
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        genero,    #Important!      
            "Cast":          None,    #Important!        
            "Directors":     None,    #Important!      
            "Availability":  None,     #Important!      
            "Download":      None,      
            "IsOriginal":    None,    #Important!        
            "IsAdult":       None,    #Important!   
            "IsBranded":     None,    #Important!   (ver link explicativo)
            "Packages":      [{'Type': 'tv-everywhere'}],    #Obligatorio      
            "Country":       None,
            "Timestamp":     datetime.now().isoformat(), #Obligatorio
            "CreatedAt":     self._created_at, #Obligatorio
            }
        Datamanager._checkDBandAppend(self, payload_contenido_series, self.list_db, self.payloads)

    def payload_episodes(self,episode,genero):
        numepi=int
        season=int
        self.get_epiyseason(episode,season,numepi)
        str_to_hash=str(episode.img['title'])+ str(numepi)+ str(season)+ str(self.url+episode.a['href'])
        id=hashlib.md5(str_to_hash.encode('utf-8')).hexdigest()
        payload_episodios = {      
                "PlatformCode":  self._platform_code, #Obligatorio      
                "Id":            str(id), #Obligatorio
                "ParentId":      str(self.get_id(episode.img['title'])), #Obligatorio #Unicamente en Episodios
                "ParentTitle":   self.get_serie_title(episode.img['alt']), #Unicamente en Episodios 
                "Episode":       int(numepi), #Unicamente en Episodios  
                "Season":        int(season), #Obligatorio #Unicamente en Episodios
                "Crew":          None, #important
                "Title":         episode.img['title'], #Obligatorio      
                "OriginalTitle": None,                          
                "Year":          None,     #Important!     
                "Duration":      None,      
                "ExternalIds":   None,     
                "Deeplinks": {          
                    "Web":       self.url+episode.a['href'],       #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },      
                "Synopsis":      None,      
                "Image":         episode.img['src'],     
                "Subtitles":     None,
                "Dubbed":        None,
                "Rating":        None,     #Important!      
                "Provider":      None,      
                "Genres":        genero,    #Important!      
                "Cast":          None,    #Important!        
                "Directors":     None,    #Important!      
                "Availability":  None,     #Important!      
                "Download":      None,      
                "IsOriginal":    None,    #Important!      
                "IsAdult":       None,    #Important!   
                "IsBranded":     None,    #Important!   (ver link explicativo)
                "Packages":      [{'Type': 'tv-everywhere'}],    #Obligatorio      
                "Country":       None,      
                "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                "CreatedAt":     self._created_at, #Obligatorio 
            }
            
        Datamanager._checkDBandAppend(self, payload_episodios, self.list_db_epi, self.payloads_epis, isEpi=True)

    def get_link_categories(self,genres):
        cat=[]
        for item in genres:
            cat.append(item["href"])
        return cat
    
    def get_lista_categories(self,genres):
        cat=[]
        for item in genres:
            cat.append(item.text)
        return cat

    def get_sinopsis(self,link):
        try:
            web=requests.get(link)
            text=BS(web.text,'html.parser')
            sinopsis=text.find('div',{'class','text-holder'})
            return str(sinopsis.p)[3:-3]
        except:
            return None

    def get_epiyseason(self,episode,season,numepi):
        episeason=episode.find_all("span")[1].text
        episeason=episeason.split(",")
        season=episeason[0].replace("\n","")
        season=season.replace("Season: ","")
        season=season.strip()
        numepi=episeason[0].replace("\n","")
        numepi=numepi.replace("Episode: ","")
        numepi=numepi.strip()

    def get_serie_title(self,episode):
        title=episode.split(":")
        return title[0]

    def get_id(self,title):
        deeplink=title.replace(" ","-")
        str_to_hash=str(title)+ str(deeplink)
        id=hashlib.md5(str_to_hash.encode('utf-8')).hexdigest()
