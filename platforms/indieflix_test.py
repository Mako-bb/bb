# -*- coding: utf-8 -*-
import time
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
from bs4                    import BeautifulSoup
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class Indieflix_test():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.test = True if type == "testing" else False
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios  = config()['mongo']['collections']['episode']
        self.currentSession = requests.session()
        self.payloads = []
        self.payloads_epi = []
        self.payloads_db = Datamanager._getListDB(self, self.titanScraping)
        self.payloads_epi_db = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        self.skippedTitles = 0
        self.skippedEpis = 0
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
                    
            self._scraping()
        
        if type == 'scraping': #or self.testing :
            self._scraping()


    def __query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}

        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at,
        }

        find_filter.update(extra_filter)

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )

        query = {item[field] for item in query}

        return query

    def _scraping(self):
        #comenzamos haciendo request a una API que contiene
        #las categorias que hay en la pagina
        api_categories = "https://api.unreel.me/api/assets/5ee3f7a04d0e1b0999ec1511/discover?__site=indieflix&__source=web&onlyEnabledChannels=true"
        api_categories = self.currentSession.get(api_categories).json()["channels"] #self.currentSession
        ids_ = []
        pagina = 0
        
        #acá hacemos un loop para guardar cada una de las categorias
        #en la variable ids_ como diccionario indicando categoria y tipo (serie/movie)
        for category in api_categories:
            if category["contentType"] == "movies" or category["contentType"] == "series":
                ids_.append({"id":category["channelId"].lower(),"type":category["contentType"]})
        
        #hacemos un loop para ingresar a la api que contiene la info de cada categoria
        for id_link in ids_:
            while True:
                url = "https://api.unreel.me/v2/sites/indieflix/channels/{}/{}?__site=indieflix&__source=web&page={}&pageSize=20".format(id_link["id"],id_link["type"],pagina)
                

                request_api = self.currentSession.get(url).json() #self.currentSession = requests
                items = request_api["items"]
                
                if not request_api["items"]:
                    pagina = 0
                    break

                print(id_link["id"])
                pagina = pagina+1
                #comenzamos a armar el payload de las series
                if id_link["type"] == "series":
                    for item in items:
                        id_ = item["uid"]
                        title = item["title"] 
                        cleantitle = _replace(title)
                        type_ = "serie"

                        if item.get("releaseYear"):
                            year = int(item["releaseYear"])
                        else:
                            year = None

                        deeplink = "https://watch.indieflix.com/"+id_link["type"]+"/"+item["uid"]
                        synopsis = item["description"]
                        image = item["poster"]

                        if item.get("genres"):
                            genres = item["genres"]
                        else:
                            genres = None
                        
                        #acá limpiamos el cast dependiendo de los errores que traia cada el cast de cada serie
                        if item.get("cast"):
                            cast = item["cast"]
                            clean_cast = []
                            for actor in cast:
                                #acá entre 2 actores no usaron , para separar la lista sino ;
                                if "; " in actor:
                                    clean_cast.extend(actor.split("; ")) 
                                else:
                                    clean_cast.append(actor)

                            cast = []
                            for actor in clean_cast:
                                #separamos los nombres del actor y el personaje
                                if " as " in actor:
                                    cast.append(actor.split(" as ")[0])
                                elif "(" in actor:
                                    cast.append(actor.split("(")[0])
                                    #acá separamos unos actores que contenian su apodo entre comillas
                                elif ('"') in actor:
                                    split_actor = []
                                    split_actor.append(actor.split('"')[0])
                                    split_actor.append(actor.split('"')[2])
                                    clean_actor = "".join(split_actor)
                                    cast.append(clean_actor.replace("  ", " "))
                                else:
                                    cast.append(actor)
                                    
                        else:
                            cast = None

                        if item.get("directors"):
                            directors = item["directors"]
                            if "O&#39;" in directors:
                                directors = directors.replace("O&#39;","'")
                        else:
                            directors = None

                        packages = [{"Type":"subscription-vod"}]

                        payload = {
                            "PlatformCode":  self._platform_code, #Obligatorio 
                            "Id":            id_,
                            "Title":         title, 
                            "CleanTitle":    cleantitle,      
                            "OriginalTitle": None,                      
                            "Type":          type_,     
                            "Year":          year,     
                            "Duration":      None,   #si es serie se pone cantidad temporadas? No  
                            "ExternalIds":   None,      
                            "Deeplinks": {          
                                "Web":       deeplink, #Obligatorio          
                                "Android":   None,          
                                "iOS":       None,      
                            },      
                            "Synopsis":      synopsis,      
                            "Image":         [image],      
                            "Rating":        None,     #Important!      
                            "Provider":      None,      
                            "Genres":        genres,    #Important!      
                            "Cast":          cast,      
                            "Directors":     directors,    #Important!      
                            "Availability":  None,     #Important!      
                            "Download":      None,      
                            "IsOriginal":    None,    #Important!      
                            "IsAdult":       None,    #Important!   
                            "IsBranded":     None,    #Important!   
                            "Packages":      packages,    #Obligatorio      
                            "Country":       None,      
                            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                            "CreatedAt":     self._created_at, #Obligatorio
                        }
                        Datamanager._checkDBandAppend(self, payload, self.payloads_db, self.payloads)
                        
                        #Accedemos a la API de los episodios
                        api_epi = "https://api.unreel.me/v2/sites/indieflix/series/{}/episodes?__site=indieflix&__source=web".format(item["uid"])
                        
                        data_epi = self.currentSession.get(api_epi).json()

                        #acá loopemos una lista dentro de una lista siendo la primera la temporada y la segunda la serie
                        #en caso de no tener se salta ya que est vacio
                        for tempo in data_epi:
                            if not tempo:
                                continue
                            for epi in tempo:
                                if not epi:
                                    continue
                                else:
                                    Id_epi = epi["uid"]
                                    
                                    if not epi.get("episode"):
                                        episode = epi["series"]["episode"]
                                        season = epi["series"]["season"] 
                                    
                                    title_epi = epi["title"]

                                    if " - " in title_epi:
                                        title_epi = title_epi.split(" - ")[-1]

                                    if ": " in title_epi and title != "Green Paradise":
                                        title_epi = title_epi.split(": ")[-1]

                                        if ")" in title_epi:
                                            title_epi = title_epi.replace(")","")

                                        elif '"' in title_epi:
                                            title_epi = title_epi.replace('"',"")
                                        
                                    if ' "' in title_epi:
                                        title_epi = title_epi.split(' "')[-1]

                                        if '"' in title_epi:
                                            title_epi = title_epi.replace('"',"")
                                    
                                    if "(HEBREW)" or "(Hebrew)" in title_epi:
                                        title_epi =title_epi.split(" (")[0]

                                    cleantitle_epi = _replace(title_epi)
                                    duration_epi = epi["contentDetails"]["duration"]//60
                                    deeplink_epi = "https://watch.indieflix.com/watch/channel/{}/series/{}/episode/{}?t=0".format(id_link["id"],id_,Id_epi)
                                    synopsis_epi = epi["description"]

                                    payload_epi = {

                                    "PlatformCode":  self._platform_code, #Obligatorio      
                                    "Id":            Id_epi, #Obligatorio
                                    "ParentId":      id_, #Obligatorio #Unicamente en Episodios
                                    "ParentTitle":   title, #Unicamente en Episodios 
                                    "Episode":       episode, #Obligatorio #Unicamente en Episodios  
                                    "Season":        season, #Obligatorio #Unicamente en Episodios
                                    "Title":         title_epi, #Obligatorio      
                                    "CleanTitle":    cleantitle_epi, #Obligatorio      
                                    "OriginalTitle": None,                                
                                    "Year":          None,     #Important!     
                                    "Duration":      duration_epi,            
                                    "Deeplinks": {          
                                        "Web":       deeplink_epi,       #Obligatorio          
                                        "Android":   None,          
                                        "iOS":       None,      
                                    },      
                                    "Synopsis":      synopsis_epi,      
                                    "Image":         None,      
                                    "Rating":        None,     #Important!      
                                    "Provider":      None,      
                                    "Genres":        None,    #Important!      
                                    "Cast":          None,      
                                    "Directors":     None,    #Important!    directores si  
                                    "Availability":  None,     #Important!      
                                    "Download":      False,      
                                    "IsOriginal":    False,    #Important!      
                                    "IsAdult":       None,    #Important!   
                                    "IsBranded":     None,    #Important!   
                                    "Packages":      packages,    #Obligatorio      
                                    "Country":       None,      
                                    "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                                    "CreatedAt":     self._created_at, #Obligatorio
                                    }
                                    Datamanager._checkDBandAppend(self, payload_epi, self.payloads_epi_db, self.payloads_epi, isEpi=True)
                else:
                    #armamos payload de las peliculas
                    for item in items:
                        id_ = item["_id"]["uid"]
                        title = item["title"] 
                        cleantitle = _replace(item["title"])
                        type_ = "movie"
                    
                        if item.get("releaseYear"):
                            year = int(item["releaseYear"])
                        else:
                            year = None
                        
                        duration = item["contentDetails"]["duration"]//60
                        deeplink = "https://watch.indieflix.com/"+type_+"/"+item["uid"]
                        synopsis = item["description"]

                        if item["movieData"].get("poster"):
                            image = item["movieData"]["poster"]
                        else:
                            image = None
                        
                        if item["movieData"].get("genres"):
                            genres = item["movieData"]["genres"]
                        else:
                            genres = None
                        
                        #acá limpiamos los cast segun sea el caso
                        if item["movieData"].get("cast"):
                            dirtcast = item["movieData"]["cast"]
                            cast = []
                            for actor in dirtcast:
                                if " as " in actor:
                                    cast.append(actor.split(" as ")[0])

                                elif "(" in actor:
                                    cast.append(actor.split("(")[0])

                                #acá limpiamos el cast de una pelicula arabe con problemas muy especificos
                                elif "\n" in actor:
                                    for actor in actor.split("\n"):
                                        if " - " in actor:
                                            cast.append(actor.split(" - ")[1].strip(r"\`"))

                                elif " - " in actor:
                                    cast.append(actor.split(" - ")[0])

                                elif "O&#39;" in actor:
                                    cast.append(actor.replace("O&#39;","'"))

                                elif "\t" in actor:
                                    cast.append(actor.replace("\t",""))

                                else:
                                    cast.append(actor)
                                    
                        else:
                            cast = None

                        if item["movieData"].get("directors"):
                            directors = item["movieData"]["directors"]

                            if "O&#39;" in directors:
                                directors = directors.replace("O&#39;","'")
                        else:
                            directors = None

                        packages = [{"Type":"subscription-vod"}]
                        

                        payload = {
                            "PlatformCode":  self._platform_code,
                            "Id":            id_,
                            "Title":         title, 
                            "CleanTitle":    cleantitle,      
                            "OriginalTitle": None,                      
                            "Type":          type_,     
                            "Year":          year,     
                            "Duration":      duration,   
                            "ExternalIds":   None,      
                            "Deeplinks": {          
                                "Web":       deeplink, #Obligatorio          
                                "Android":   None,          
                                "iOS":       None,      
                            },      
                            "Synopsis":      synopsis,      
                            "Image":         [image],      
                            "Rating":        None,     #Important!      
                            "Provider":      None,      
                            "Genres":        genres,    #Important!      
                            "Cast":          cast,      
                            "Directors":     directors,    #Important!      
                            "Availability":  None,     #Important!      
                            "Download":      None,      
                            "IsOriginal":    None,    #Important!      
                            "IsAdult":       None,    #Important!   
                            "IsBranded":     None,    #Important!   
                            "Packages":      packages,    #Obligatorio      
                            "Country":       None,      
                            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                            "CreatedAt":     self._created_at, #Obligatorio
                        }
                        Datamanager._checkDBandAppend(self, payload, self.payloads_db, self.payloads)
        
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)
        Datamanager._insertIntoDB(self, self.payloads_epi, self.titanScrapingEpisodios)

        if self.test:
            Upload(self._platform_code, self._created_at,testing=True)
        else:
            Upload(self._platform_code, self._created_at,testing=True)