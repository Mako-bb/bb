# -*- coding: utf-8 -*-
import time
from pymongo import response
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from bs4                    import BeautifulSoup as BS
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class Shoutfactorytv():
    """
    Shoutfactorytv es una ott de Estados Unidos.

    DATOS IMPORTANTES:
    - Versión Final: Si.
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: NO.
    - ¿Usa BS4?: Sí.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? 1 min
    - ¿Cuanto contenidos trajo la ultima vez? TS:163 TSE: 960 07/10/21

    OTROS COMENTARIOS:
        Contenia series sin episodios, se modificó el script para excluirlas.

    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.skippedEpis = 0
        self.skippedTitles = 0
        ################# URLS  #################
        self._url = self._config['url']
        #self._movies_url = self._config['movie_url']
        #self._show_url = self._config['show_url']
        #Url para encontrar la información de los contenidos por separado
        #self._format_url = self._config['format_url'] 
        #self._episode_url = self._config['episode_url']
        self.testing = False
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}
                    
        self._payload_template = { 
            "PlatformCode":  None, #Obligatorio   
            "Id":            None, #Obligatorio
            "Title":         None, #Obligatorio      
            "CleanTitle":    None, #Obligatorio      
            "OriginalTitle": None,          
            "Type":          None,     #Obligatorio  #movie o serie     
            "Year":          None,     #Important!  1870 a año actual  
            "Duration":      None,     #en minutos   
            "ExternalIds":   "list", #*      
            "Deeplinks": {
                "Web":       "str",       #Obligatorio          
                "Android":   "str",          
                "iOS":       "str",      
            },
            "Synopsis":      "str",      
            "Image":         "image",    
            "Rating":        "str",     #Important!      
            "Provider":      "list",      
            "Genres":        "movie.text",    #Important!      
            "Cast":          "list",    #Important!  
            "Directors":     "list",    #Important!      
            "Availability":  "str",     #Important!      
            "Download":      "bool",      
            "IsOriginal":    "bool",    #Important!  
            "IsAdult":       "bool",    #Important!   
            "IsBranded":     "bool",    #Important!   (ver link explicativo)
            "Packages":      [
                    {
                        "asd":"asd"
                    }
            ],    #Obligatorio      
            "Country":       "list",
            "Timestamp":     "str", #Obligatorio
            "CreatedAt":     self._created_at #Obligatorio
        }

        self.list_db_episodes = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)

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

        print(self._url)
        page = requests.get(self._url)

        self.get_payload_movies(page)
        #self.get_payload_series(page)
        #page = requests.get(self._url)

        #if page.status_code == 200:
        #    print('La pagina se descargó correctamente')
        #    soup = BS(page.content, 'html.parser')
        #    categories = soup.find_all('div', class_='divRow')


        #    print("######################################################################")
        #    films = categories[0]
        #    films = films.find_all('a')

        #    for film in films:
        #        print(film.text)


        #    print("######################################################################")
        #    series = categories[1]
        #    series = series.find_all('a')

        #    for serie in series:
        #        print(serie.text)

        #    print("######################################################################")

    def get_category_link(self, content):
        return self._url + content['href']

    def get_title(self, content):
        return content['title']
    
    def get_image(self, content):
        return content['src']

    def get_deeplink(self, content):
        return self._url + content.find('a')['href']

    def get_id(self, title, deeplink):
        return hashlib.md5((title + deeplink).encode('utf-8')).hexdigest
    
    def get_soup_from_category(self, category):
        category_page = requests.get(category)
        soup = BS(category_page.content, 'html.parser')
        return soup.find_all('div', class_='img-holder')

    def get_synopsis(self, deeplink):
        page_content = requests.get(deeplink)
        soup = BS(page_content.content, 'html.parser')
        return soup.find('p').getText()

    def get_content_soup(self, page):
        soup = BS(page.content, 'html.parser')
        return soup.find_all('div', class_='divRow') 

    def get_payload_movies(self, page):

        if page.status_code == 200:
            print('La pagina se descargó correctamente')
            categories = self.get_content_soup(page)

        temp = categories[0]
        movie_category = temp.find_all('a') 
        movie_list = []
        movie_counter = 0
        movie_counter_repetidos = 0

        for movie in movie_category:
            print(" ################################## " + movie.text +  " ##################################")

            category_link = self.get_category_link(movie)                           # obtenemos el enlace de cada categoria para empezar a scrapear peliculas en ese orden

            temp = self.get_soup_from_category(category_link) 

            for elem in temp:
                movie_info = elem.find('img')
                
                try:
                    title = self.get_title(movie_info)                                      # título de la película
                    image = self.get_image(movie_info)                                      # imagen promocional
                    deeplink = self.get_deeplink(elem)                                      # deeplink a la película
                    id = self.get_id(title, deeplink)                                       # id generado con hashlib con md5, estoy seguro que con el título 
                                                                                            # y el deeplink no va a generar uno repetido

                    synopsis = self.get_synopsis(deeplink)                                  # la siguiente request es para conseguir                                   
                                                                                            # la synopsis de la película
                    movie_counter_repetidos += 1
                except: 
                    #print("################ FALTAN DATOS PARA DE ESTA PELICULA ################")
                    title = None                                                            # por las dudas devolvemos None
                    image = None                                                            # si alguno falla en el try except
                    deeplink = None
                    id = None

                # La siguiente condición es para asegurarnos de no guardarnos repetidos

                if title not in movie_list and title != None:
                    print("titulo: ", title)
                    movie_list.append(title)

                    payload_movies = self._payload_template

                    payload_movies["PlatformCode"] = self._platform_code
                    payload_movies["Id"] = id
                    payload_movies["Title"] = title
                    payload_movies["CleanTitle"] = _replace(title)
                    payload_movies["Type"] = "movie"
                    payload_movies["Deeplinks"]["Web"] = deeplink
                    payload_movies["Synopsis"] = synopsis
                    payload_movies["Image"] = image
                    payload_movies["Genre"] = movie.text
                    payload_movies["CreatedAt"] = self._created_at

                    ##payload_movies = { 
                        ##"PlatformCode":  self._platform_code, #Obligatorio   
                        ##"Id":            id, #Obligatorio
                        ##"Crew":          [ #Importante
                                            ##{
                                                ##"Role": "str", 
                                                ##"Name": "str"
                                            ##},
                                            ##...
                        ##],
                        ##"Title":         title, #Obligatorio      
                        ##"CleanTitle":    _replace(title), #Obligatorio      
                        ##"OriginalTitle": "str",                          
                        ##"Type":          "movie",     #Obligatorio  #movie o serie     
                        ##"Year":          "int",     #Important!  1870 a año actual   
                        ##"Duration":      "int",     #en minutos   
                        ##"ExternalIds":   "list", #*      
                        ##"Deeplinks": {
                            ##"Web":       deeplink,       #Obligatorio          
                            ##"Android":   "str",          
                            ##"iOS":       "str",      
                        ##},
                        ##"Synopsis":      synopsis,      
                        ##"Image":         image,      
                        ##"Subtitles": "list",
                        ##"Dubbed": "list",
                        ##"Rating":        "str",     #Important!      
                        ##"Provider":      "list",      
                        ##"Genres":        movie.text,    #Important!      
                        ##"Cast":          "list",    #Important!        
                        ##"Directors":     "list",    #Important!      
                        ##"Availability":  "str",     #Important!      
                        ##"Download":      "bool",      
                        ##"IsOriginal":    "bool",    #Important!        
                        ##"IsAdult":       "bool",    #Important!   
                        ##"IsBranded":     "bool",    #Important!   (ver link explicativo)
                        ##"Packages":      "list",    #Obligatorio      
                        ##"Country":       "list",
                        ##"Timestamp":     "str", #Obligatorio
                        ##"CreatedAt":     self._created_at #Obligatorio
                    ##}

                    movie_counter += 1

        print("Cantidad de peliculas: ", movie_counter) 
        print("Cantidad de peliculas repetidas", movie_counter_repetidos)
        
    def get_payload_series(self, page):

        if page.status_code == 200:
            print('La pagina se descargó correctamente')
            categories = self.get_content_soup(page)

        temp = categories[1]
        series_category = temp.find_all('a') 
        serie_list = []
        serie_counter = 0
        serie_counter_repetidos = 0

        for serie in series_category:
            print(" ################################## " + serie.text +  " ##################################")

            category_link = self.get_category_link(serie)

            temp = self.get_soup_from_category(category_link)

            for elem in temp:
                serie_info = elem.find('img')

                print(serie_info['title'])

                try:
                    title = self.get_title(serie_info)                                      # título de la serie
                    image = self.get_image(serie_info)                                      # imagen promocional
                    deeplink = self.get_deeplink(elem)                                      # deeplink a la serie
                    id = self.get_id(title, deeplink)                                       # id generado con hashlib con md5, estoy seguro que con el título 
                                                                                            # y el deeplink no va a generar uno repetido

                    #synopsis = self.get_synopsis(deeplink)                                # la siguiente request es para conseguir  
                                                                                           # la synopsis de la película
                    serie_counter_repetidos += 1                                           # POR AHORA NO HAY SYNOPSYS PARA SERIES
                except: 
                    #print("################ FALTAN DATOS PARA DE ESTA PELICULA ################")
                    title = None                                                            # por las dudas devolvemos None
                    image = None                                                            # si alguno falla en el try except
                    deeplink = None
                    id = None

                # La siguiente condición es para asegurarnos de no guardarnos repetidos
                if title not in serie_list and title != None:
                    print(title)
                    serie_list.append(title)

                    payload_serie = self._payload_template

                    payload_serie["PlatformCode"] = self._platform_code
                    payload_serie["Id"] = id                    
                    payload_serie["Title"] = title
                    payload_serie["CleanTitle"] = _replace(title)
                    payload_serie["Type"] = "serie"             
                    payload_serie["Deeplinks"]["Web"] = deeplink
                    #payload_serie["Synopsis"] = synopsis  
                    payload_serie["Image"] = image
                    payload_serie["Genre"] = serie.text
                    payload_serie["CreatedAt"] = self._created_at
#                        "Title":         title, #Obligatorio      
#                        "CleanTitle":    _replace(title), #Obligatorio      
#                        "OriginalTitle": "str",                          
#                        "Type":          "movie",     #Obligatorio  #movie o serie     
#                        "Year":          "int",     #Important!  1870 a año actual   
#                        "Duration":      "int",     #en minutos   
#                        "ExternalIds":   "list", #*      
#                        "Deeplinks": {
#                            "Web":       deeplink,       #Obligatorio          
#                            "Android":   "str",          
#                            "iOS":       "str",      
#                        },
#                        "Synopsis":      "str",      
#                        "Image":         image,      
#                        "Subtitles": "list",
#                        "Dubbed": "list",
#                        "Rating":        "str",     #Important!      
#                        "Provider":      "list",      
#                        "Genres":        serie.text,    #Important!      
#                        "Cast":          "list",    #Important!        
#                        "Directors":     "list",    #Important!      
#                        "Availability":  "str",     #Important!      
#                        "Download":      "bool",      
#                        "IsOriginal":    "bool",    #Important!        
#                        "IsAdult":       "bool",    #Important!   
#                        "IsBranded":     "bool",    #Important!   (ver link explicativo)
#                        "Packages":      "list",    #Obligatorio      
#                        "Country":       "list",
#                        "Timestamp":     "str", #Obligatorio
#                        "CreatedAt":     self._created_at #Obligatorio
#                    }
#
                    serie_counter += 1

        print("Cantidad de series peliculas: ", serie_counter) 
        print("Cantidad de series repetidas",serie_counter_repetidos)
        
        

            #for tags in categories:
                #print(tags.find_all('a'))
                #category_names = tags.find_all('a')
                #print(tags.find(''))
                #for name in category_names:
                #   print(name.text)
    
