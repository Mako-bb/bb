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

class Indieflix():

    """  
    DATOS IMPORTANTES:
    ¿Necesita VPN? -> NO
    ¿HTML, API, SELENIUM? -> API
    Cantidad de contenidos (ultima revisión): Series y peliculas = 1934, Episodios = 1072
    Tiempo de ejecucíon de Script = 2-5 Minutos
    """
    
    def __init__(self, ott_site_uid, ott_site_country, type):
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
        
        if type == 'scraping':
            self._scraping()
        if type == 'testing':
            self._scraping(testing=True)

    def _query_field(self, collection, field, extra_filter=None):
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

    def _scraping(self, testing=False):

        # Set de Clean Titles
        self.clean_titles = self._query_field(self.titanScraping, field='CleanTitle')
        # Set de Ids
        self.ids = self._query_field(self.titanScraping, field='Id')

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
                        id_ = hashlib.md5(item["uid"].encode('utf-8')).hexdigest()
                    
                        title = item["title"] 
                        cleantitle = _replace(title)
                        type_ = "serie"

                        if item.get("releaseYear"):
                            year = int(item["releaseYear"])
                        else:
                            year = None

                        deeplink = "https://watch.indieflix.com/"+id_link["type"]+"/"+item["uid"]

                        synopsis = item["description"]
                        if "\r\n" in synopsis:
                            synopsis = synopsis.replace("\r\n"," ").strip()
                        elif "\n" in synopsis:
                            synopsis = synopsis.replace("\n"," ").strip()

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
                        if id_ in self.ids:
                            print("Ingresado")                            
                            pass
                        else:
                            self.ids.add(id_)                            
                            check = self.check_payload(payload)
                            if check:
                                print("Duplicado")
                                id_ = check
                            else:                                
                                self.mongo.insert(self.titanScraping, payload)                 
                        # Datamanager._checkDBandAppend(self, payload, self.payloads_db, self.payloads)

                        # check -> Sería el id ingresado. 
                        
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
                                    Id_epi = hashlib.md5(epi["uid"].encode('utf-8')).hexdigest()

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
                                    if "\r\n" in synopsis_epi:
                                        synopsis_epi = synopsis_epi.replace("\r\n"," ").strip()
                                    elif "\n" in synopsis_epi:
                                        synopsis_epi = synopsis_epi.replace("\n"," ").strip()

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
                                    "Download":      None,      
                                    "IsOriginal":    None,    #Important!      
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
                        id_ = hashlib.md5(item["_id"]["uid"].encode('utf-8')).hexdigest()
                        

                        #fix para no duplicar la pelicula con ids distitno
                        # 9c88225457856ad8f19a6952c950ab32 == "The Andalusian Dog"
                        # 0a1e1d8da51f5dd7077a7f2748b10d58 == "M"
                        # 4fbdc7de3e31de33862586c8db456f53 == "Dawn"
                        #if (id_ == "9c88225457856ad8f19a6952c950ab32") or (id_ == "0a1e1d8da51f5dd7077a7f2748b10d58") or (id_ == "4fbdc7de3e31de33862586c8db456f53") :
                            #continue

                        title = item["title"]

                        #fix especifico para pelicula donde se debe eliminar
                        #del titulo lo que hay entre parentesis
                        #f27900bdac7a82614a4931f309704d69 == Brooks McBeth: This Ain't Shakespeare
                        if id_ == "f27900bdac7a82614a4931f309704d69": 
                            #separamos el titulo a partir del (
                            split_title = title.split(" (")
                            title = split_title[0]
                            originaltitle = None

                        #fix especifico para pelicula donde del titulo
                        # solo se debe eliminar parentesis
                        #bc4d9b0e9bdbd3186592452785c479cc == The Amateur: or Revenge of the Quadricorn
                        if id_ == "bc4d9b0e9bdbd3186592452785c479cc":
                            title = title.replace("(","")
                            title = title.replace(")","")
                            originaltitle = None
                        
                        #ef5fa3e7b0c6b2383740f768ed209602 == (beau)strosity / debe quedar asi
                        #9e57be5389ed6ba4e95ec7bdd5ff9f2f == Salad Days: A Decade of Punk in Washington, DC (1980-90) / debe quedar asi
                        elif id_ == "ef5fa3e7b0c6b2383740f768ed209602" or id_ == "9e57be5389ed6ba4e95ec7bdd5ff9f2f":
                            originaltitle = None

                        #hay ocasiones en las que el titulo viene en formato "orginaltitle (title)"
                        #por lo que hay que validarlo y separarlo
                        elif "(" in title:
                            #separamos el titulo a partir del (
                            split_title = title.split("(")
                            #validamos si a partir del ) tiene mas de 3 letras
                            #para confirmar que sea un titulo original y no parte del titulo
                            if len(split_title[-1].split(")")[0]) > 3:
                                #en ese caso la primera parte seria el titulo original
                                originaltitle = split_title[0]
                                #y la segunda parte seria el titulo traducido
                                title = split_title[-1].replace(")","")
                        else:
                            originaltitle = None

                        cleantitle = _replace(title)
                        type_ = "movie"
                    
                        if item.get("releaseYear"):
                            year = int(item["releaseYear"])
                        else:
                            year = None
                        
                        duration = item["contentDetails"]["duration"]//60
                        deeplink = "https://watch.indieflix.com/"+type_+"/"+item["uid"]

                        synopsis = item["description"]
                        if "\r\n" in synopsis:
                            synopsis = synopsis.replace("\r\n"," ").strip()
                        elif "\n" in synopsis:
                            synopsis = synopsis.replace("\n"," ").strip()


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

                        #acá validamos que el nombre del director no se Unknown
                        #y reemplazamos O&#39 por su valor ASCII que es '
                        if item["movieData"].get("directors") and not "Unknown" in item["movieData"]["directors"]:
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
                            "OriginalTitle": originaltitle,                      
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
                        } #TODO: aplicar algoritmo juan
                        if id_ in self.ids:
                            print("Ingresado")                            
                            pass
                        else:
                            self.ids.add(id_)
                            check = self.check_payload(payload)
                            if check:
                                print("Duplicado")
                            else:                                
                                self.mongo.insert(self.titanScraping, payload)
                        # Datamanager._checkDBandAppend(self, payload, self.payloads_db, self.payloads)
        
        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)
        Datamanager._insertIntoDB(self, self.payloads_epi, self.titanScrapingEpisodios)

        Upload(self._platform_code, self._created_at,testing=testing)

    def check_payload(self, payload):
        """Método que verifica si se ingresó el mismo contenido.
        Puede haber casos en que un contenido tenga distinto Id
        o que las series estén separadas.

        Args:
            payload (str): El diccionario del payload.

        Returns:
            str or None: Devuelve el Id ingresado. Si no está
            duplicado, devuelve None.
        """
        clean_title = payload['CleanTitle']
        synopsis = payload['Synopsis']

        if clean_title in self.clean_titles:
            query_1 = {
                "PlatformCode": self._platform_code,
                "CreatedAt": self._created_at,
                "CleanTitle": clean_title,
                "Synopsis" : synopsis
            }
            consulta = self.mongo.search(self.titanScraping, query_1)
            if consulta:
                for ingresado in consulta:
                    synopsis_ingresada = ingresado['Synopsis']
                    
                    if synopsis == synopsis_ingresada:
                        print("¡Contenido Duplicado!\n")
                        return ingresado['Id']

        self.clean_titles.add(clean_title)
        return None