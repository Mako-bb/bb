from turtle import title
from urllib import response
from wsgiref.simple_server import demo_app
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
        #self.get_payload_series(serie_data)
        #self.get_payload_episodes(episode_data)

        self.url = "https://www.shoutfactorytv.com"
        response = requests.get(self.url) #Enviamos una solicitud a la pag
        content = response.text #Lo transforma en texto 
        soup = BeautifulSoup(content, 'lxml')
        section = soup.find_all("div", {"class", "drop-holder"}) #Por categorias
        movies_categories = section[0]
        series_categories = section[1]

        self.get_movies(movies_categories)
        self.get_series(series_categories)

    # Scripts para traer todas las peliculas
    def get_movies(self, movies_categories):
        categ = movies_categories.find_all("a")
        for item in categ:
            url_categ = self.url + item['href']
            print(url_categ)
            content = requests.get(url_categ)
            self.get_content(content)
            print('---------------------URL MOVIES--------------------')

    def get_series(self, series_categories):
        categ = series_categories.find_all("a")
        for item in categ:
            url_categ = self.url + item['href']
            print(url_categ)
        
            
        print('------------URL SERIES-----------')
    
    def get_content(self, content):
        content_movies = BeautifulSoup(content.text, 'lxml')
        img_holder = content_movies.find_all('div', attrs={'class': 'img-holder'})
        for content in img_holder:
            #title = self.get_title(content)
            #deeplink = self.get_deeplink(content)
            #src = self.get_src(content)
            data = self.get_data(content)
            #print(title)
            #print(deeplink)
            #print(src)
            print(data)
            print('-------------------------')
        
    def get_data(self, content):
        url_content =  'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q={title}'.format(title = self.get_title(content))
        content = requests.get(url_content)
        soup = BeautifulSoup(content.text, 'lxml')
        article = soup.find_all('article', {'class': 'post'})
        #print(url_content)
        for item in article:
            link = item.find('a')['href'] 
            link_movies = self.url + link
            if link_movies == self.get_deeplink: 
                data = link_movies.find('div', {'class', 'holder'})
                for i in data:
                    synopsis = i.find('p')
                    print(synopsis)

                
                '''holder = item.find('div', {'class', 'holder'})
                sinopsis = holder.find('p')
                print(sinopsis)'''
                

            '''#duration = holder.find('time', {'class', 'duration'})
            #duration = item.find('p')
            print(holder)'''
        
        
        
        
        '''title = self.get_title 
        URLmovies = 'https://www.shoutfactorytv.com/videos?utf8=%E2%9C%93&commit=submit&q={title}'.format(name=title)
        response = requests.get(URLmovies) #Enviamos una solicitud a la pag
        #content = response.text #Lo transforma en texto 
        soup = BeautifulSoup(response.text, 'lxml')
        print(soup)'''



        '''url_content = self.url + '/videos?utf8=✓&commit=submit&q=' + self.get_title
        content = requests.get(url_content)
        for item in content:
            print(item)
            '''
            


        '''response = requests.get(self.url)
        soup = BeautifulSoup(response.text, 'lxml')
        search = soup.find('input', {'placeholder', 'Search'})
        print(search)
        '''

        '''for item in URLmovies:
            title = self.get_title 
            url = URLmovies + title 
            holder = soup.find_all('div', attrs={'class': 'holder'})
            print(holder)
        #for item in holder:
         #   pass'''

    def get_title(self, content):
        if content.img != None:
            title = content.img.get('title')
        else: 
            title = content.a.get('href').split('/')[1].replace('-', ' ').capitalize
        return title

    def get_deeplink(self,content):
        deeplink = content.a
        deeplink = self.url + deeplink['href']
        return deeplink

    def get_src(self, content):
        if content.img != None:
            src = content.img.get('src')
        else:
            src = None
        return src



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