import time
from types import NoneType
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
        self.list_id=[]
        self.list_id_epi=[]

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
        indice=0
        for categories in links:
            print(list[indice])
            web_categories= requests.get(self.url+categories)
            movies_list=BS(web_categories.text,"html.parser")
            movies_list=movies_list.find_all("div",{"class","img-holder"})
            for movie in movies_list:
                if movie.img != None:
                    if self.get_id(movie.img['title'],movie.a['href']) not in self.list_id:
                        self.payload_movie(movie,list[indice])
                        self.list_id.append(self.get_id(movie.img['title'],movie.a['href']))
                    else:
                        pass
                else:
                    web=requests.get(self.url+movie.a['href'])
                    soup=BS(web.text,"html.parser")
                    web_movie=soup.find("div",{"class","text-holder"})
                    if self.get_id(web_movie.span.text,movie.a['href']) not in self.list_id:
                        self.payload_movie_noimg(web_movie,list[indice],movie.a['href'])
                        self.list_id.append(self.get_id(web_movie.span.text,movie.a['href']))
                    else:
                        pass
                    
            indice+=1

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
                title=self.get_title(serie)
                serie_id=self.get_id(serie.img['title'],serie.a['href'])
                self.get_payload_episodes(serie.a["href"],list[indice],title,serie_id)
            indice+=1
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)
        

    def get_payload_episodes(self,serie_link,genero,serie_title,serie_id):
        self.payload_epi=[]
        web_serie=requests.get(self.url+serie_link)
        serie_soup=BS(web_serie.text,"html.parser")
        episode_list=serie_soup.find_all("ul",{"class","thumbnails add series series"})
        for episode_line in episode_list:
            episodes=episode_line.find_all("a")
            for episode in episodes:
                if self.get_epi_id(episode) not in self.list_id_epi:
                    self.payload_episodes(episode,genero,serie_title,serie_id)
                    self.list_id_epi.append(self.get_epi_id(episode))
        
        Datamanager._insertIntoDB(self, self.payload_epi, self.titanScrapingEpisodios)

    def payload_movie_noimg(self, movie,genero,href):
            payload_contenidos = { 
                'PlatformCode':  self._platform_code, #Obligatorio   
                "Id":            str(self.get_id(movie.span.text,href)), #Obligatorio
                "Crew":          None,
                "Title":         movie.span.text, #Obligatorio      
                "CleanTitle":    _replace(movie.span.text), #Obligatorio      
                "OriginalTitle": None,                          
                "Type":          'movie',     #Obligatorio  #movie o serie     
                "Year":          None,     #Important!  1870 a año actual   
                "Duration":      int(self.get_duration(movie.span.text,href)),     #en minutos   
                "ExternalIds":   None,    
                "Deeplinks": {
                    "Web":       self.url+href,       #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },
                "Synopsis":      movie.p.text,      
                "Image":         None,      
                "Subtitles":     None,
                "Dubbed":        None,
                "Rating":        None,     #Important!      
                "Provider":      None,      
                "Genres":        [genero],    #Important!      
                "Cast":          None,    #Important!        
                "Directors":     None,    #Important!      
                "Availability":  None,     #Important!      
                "Download":      None,      
                "IsOriginal":    None,    #Important!        
                "IsAdult":       None,    #Important!   
                "IsBranded":     None,    #Important!   (ver link explicativo)
                "Packages":      [{'Type': 'free-vod'}],    #Obligatorio      
                "Country":       None,
                "Timestamp":     datetime.now().isoformat(), #Obligatorio
                "CreatedAt":     self._created_at, #Obligatorio
                }
            Datamanager._checkDBandAppend(self, payload_contenidos, self.list_db, self.payloads)

    def payload_movie(self, movie,genero):
        payload_contenidos = { 
            'PlatformCode':  self._platform_code, #Obligatorio   
            "Id":            str(self.get_id(movie.img['title'],movie.a['href'])), #Obligatorio
            "Crew":          None,
            "Title":         movie.img['title'], #Obligatorio      
            "CleanTitle":    _replace(movie.img['title']), #Obligatorio      
            "OriginalTitle": None,                          
            "Type":          'movie',     #Obligatorio  #movie o serie     
            "Year":          None,     #Important!  1870 a año actual   
            "Duration":      self.get_duration(movie.img['title'],movie.a['href']),     #en minutos   
            "ExternalIds":   None,    
            "Deeplinks": {
                "Web":       self.url+movie.a['href'],       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },
            "Synopsis":      self.get_sinopsis(self.url+movie.a['href']),      
            "Image":         [movie.img['src']],      
            "Subtitles":     None,
            "Dubbed":        None,
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        [genero],    #Important!      
            "Cast":          None,    #Important!        
            "Directors":     None,    #Important!      
            "Availability":  None,     #Important!      
            "Download":      None,      
            "IsOriginal":    None,    #Important!        
            "IsAdult":       None,    #Important!   
            "IsBranded":     None,    #Important!   (ver link explicativo)
            "Packages":      [{'Type': 'free-vod'}],    #Obligatorio      
            "Country":       None,
            "Timestamp":     datetime.now().isoformat(), #Obligatorio
            "CreatedAt":     self._created_at, #Obligatorio
            }
        Datamanager._checkDBandAppend(self, payload_contenidos, self.list_db, self.payloads)

    def payload_serie(self,serie,genero):      
        payload_contenido_series = { 
            "PlatformCode":  self._platform_code, #Obligatorio   
            "Id":            str(self.get_id(serie.img['title'],serie.a['href'])), #Obligatorio
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
            "Image":         [serie.img['src']],      
            "Subtitles":     None,
            "Dubbed":        None,
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        [genero],    #Important!      
            "Cast":          None,    #Important!        
            "Directors":     None,    #Important!      
            "Availability":  None,     #Important!      
            "Download":      None,      
            "IsOriginal":    None,    #Important!        
            "IsAdult":       None,    #Important!   
            "IsBranded":     None,    #Important!   (ver link explicativo)
            "Packages":      [{'Type': 'free-vod'}],    #Obligatorio      
            "Country":       None,
            "Timestamp":     datetime.now().isoformat(), #Obligatorio
            "CreatedAt":     self._created_at, #Obligatorio
            }
        Datamanager._checkDBandAppend(self, payload_contenido_series, self.list_db, self.payloads)

    def payload_episodes(self,episode,genero,serie_title,serie_id):
        season,numepi=self.get_epiyseason(episode)
        payload_episodios = {      
                "PlatformCode":  self._platform_code, #Obligatorio      
                "Id":            str(self.get_epi_id(episode)), #Obligatorio
                "ParentId":      str(serie_id), #Obligatorio #Unicamente en Episodios
                "ParentTitle":   serie_title, #Unicamente en Episodios 
                "Episode":       int(numepi), #Unicamente en Episodios  
                "Season":        int(season), #Obligatorio #Unicamente en Episodios
                "Crew":          None, #important
                "Title":         episode.img['title'], #Obligatorio      
                "OriginalTitle": None,                          
                "Year":          None,     #Important!     
                "Duration":      self.get_duration(episode.img['title'],episode['href']),      
                "ExternalIds":   None,     
                "Deeplinks": {          
                    "Web":       self.url+episode['href'],       #Obligatorio          
                    "Android":   None,          
                    "iOS":       None,      
                },      
                "Synopsis":      None,      
                "Image":         [episode.img['src']],     
                "Subtitles":     None,
                "Dubbed":        None,
                "Rating":        None,     #Important!      
                "Provider":      None,      
                "Genres":        [genero],    #Important!      
                "Cast":          None,    #Important!        
                "Directors":     None,    #Important!      
                "Availability":  None,     #Important!      
                "Download":      None,      
                "IsOriginal":    None,    #Important!      
                "IsAdult":       None,    #Important!   
                "IsBranded":     None,    #Important!   (ver link explicativo)
                "Packages":      [{'Type': 'free-vod'}],    #Obligatorio      
                "Country":       None,      
                "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                "CreatedAt":     self._created_at, #Obligatorio 
            }
            
        Datamanager._checkDBandAppend(self, payload_episodios, self.list_db_epi, self.payload_epi, isEpi=True)

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

    def get_epiyseason(self,episode):
        episeason=episode.find_all("span")[1].text
        episeason=episeason.split(",")

        season=episeason[0].replace("Season: ","")
        season=season.strip()

        numepi=episeason[1]
        numepi=numepi.replace("Episode: ","")
        numepi=numepi.strip()

        return season,numepi

    def get_title(self,serie):
        title=serie.img['title']
        return str(title)

    def get_id(self,title,link):
        str_to_hash=str(title)+ str(self.url+link)
        id=hashlib.md5(str_to_hash.encode('utf-8')).hexdigest()
        return id

    def get_epi_id(self,episode):
        season,numepi=self.get_epiyseason(episode)
        str_to_hash=str(episode.img['title'])+ str(numepi)+ str(season)+ str(self.url+episode['href'])
        id=hashlib.md5(str_to_hash.encode('utf-8')).hexdigest()
        return id

    def get_duration(self,title,href):
        try:
            duration=""
            response= requests.get('https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q='+href)
            soup= BS(response.text, 'html.parser')
            content_list=soup.find_all("div",{"class","video-container"})
            for item in content_list:
                if item.a['href'] == href:
                    duration=item.time.text
                    duration=duration.split(':')[1].strip()
            if duration!="": return int(duration) 
            else: return None
        except:
            return None