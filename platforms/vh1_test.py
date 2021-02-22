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
from bs4                    import BeautifulSoup as BS
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class Vh1_test():

    """  
    DATOS IMPORTANTES:
    ¿Necesita VPN? -> NO
    ¿HTML, API, SELENIUM? -> API
    Cantidad de contenidos (ultima revisión): Series y peliculas = 98, Episodios = 2748
    Tiempo de ejecucíon de Script = 14 Minutos
    """

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
        
        api_series = "http://www.vh1.com/feeds/ent_m150/de947f9a-0b22-4d65-a3ea-5d39a6d0e4f5"
        api_series_request = self.currentSession.get(api_series).json()

        items = api_series_request["result"]["data"]["items"]

        for item in items:
            for serie in item["sortedItems"]:

                id_serie = serie["itemId"]
                title_serie = serie["title"]
                cleantitle_serie = _replace(serie["title"])
                deeplink_serie = serie["url"]
                type_serie = "serie"
                 #hacemos una request para encontrar la descripcion dentro de la pagina
                request_serie = self.currentSession.get(deeplink_serie)
                html_serie = BS(request_serie.text,features="lxml")
                
                #la sinopsis puede estar en 2 ubicaciones, aqui validamos ambas opciones
                if html_serie.find("div",{"id":"t5_lc_promo1"}) and html_serie.find("div",{"id":"t5_lc_promo1"}).find("div",{"class":"info"}):
                    contenedor_synopsis = html_serie.find("div",{"id":"t5_lc_promo1"})
                    synopsis_serie = contenedor_synopsis.find("div",{"class":"info"}).text

                elif html_serie.find("div",{"class":"ent_m202-copy"}):
                    synopsis_serie = html_serie.find("div",{"class":"ent_m202-copy"}).text
                
                else:
                    synopsis_serie = None

                contenedor_image = html_serie.find("div",{"class":"image_holder"})
                image_serie = [contenedor_image["data-info"].split(",")[2].split(": ")[-1].replace('"',"")]

                #hacemos un request para traer el cast ya que hay que acceder a una url especifica
                cast_url = deeplink_serie+"/cast"
                request_cast = self.currentSession.get(cast_url)
                #en caso de que el status no sea 200 el cast es None ya que no tiene
                if request_cast.status_code != 200:
                    cast_serie = None
                else:
                    #Soupeamos la request para acceder al contenedor de los actores y procedemos a ingresarlos a la lista del cast
                    cast_serie = []
                    cast_bs = BS(request_cast.text,features="lxml")
                    if cast_bs.find_all("ul",{"class":"L001_line_list"}):
                        cast_list = cast_bs.find("ul",{"class":"L001_line_list"}).find_all("span",{"class":"headline"})
                        for actor in cast_list:
                            cast_serie.append(actor.text)
                    else:
                        cast_serie = None
                
                package_serie = [{"Type": "tv-everywhere"}]

                payload = {
                    "PlatformCode":  self._platform_code, #Obligatorio 
                    "Id":            id_serie,
                    "Title":         title_serie, 
                    "CleanTitle":    cleantitle_serie,      
                    "OriginalTitle": None,                      
                    "Type":          type_serie,     
                    "Year":          None,     
                    "Duration":      None,  
                    "ExternalIds":   None,      
                    "Deeplinks": {          
                        "Web":       deeplink_serie, #Obligatorio          
                        "Android":   None,          
                        "iOS":       None,      
                    },      
                    "Synopsis":      synopsis_serie,      
                    "Image":         image_serie,      
                    "Rating":        None,     #Important!      
                    "Provider":      None,      
                    "Genres":        None,    #Important!      
                    "Cast":          cast_serie,      
                    "Directors":     None,    #Important!      
                    "Availability":  None,     #Important!      
                    "Download":      None,      
                    "IsOriginal":    None,    #Important!      
                    "IsAdult":       None,    #Important!   
                    "IsBranded":     None,    #Important!   
                    "Packages":      package_serie,    #Obligatorio      
                    "Country":       None,      
                    "Timestamp":     datetime.now().isoformat(), #Obligatorio      
                    "CreatedAt":     self._created_at, #Obligatorio
                }
                Datamanager._checkDBandAppend(self, payload, self.payloads_db, self.payloads)

                pagina = 1 #contador de pagina para la url de la api_episodes
                while True:   
                    api_episodes = "http://www.vh1.com/feeds/ent_m112/3daea531-6a5c-4e72-83af-85c9c88b4ffe/{}?allSeasonsSelected=1&allEpisodes=1&pageNumber={}".format(id_serie,pagina)
                    request_epi = self.currentSession.get(api_episodes)
                    json_episodes = request_epi.json()
                    
                    #Aqui validamos que el item en la api tenga capitulos
                    if not json_episodes["result"]["data"]["items"]:
                        pagina = 1
                        break
                    
                    #contador que incrementa para pasar a la siguiente temporada luego de hacer request
                    pagina = pagina + 1

                    #hacemos un bucle para extraer los capitulos
                    for item in json_episodes["result"]["data"]["items"]:
                        #Aqui validamos que el item sea un capitulo y no un ad y extraemos los datos
                        if item.get("id"):
                            id_episode = item["id"]
                            
                            #aqui confirmamos que el capitulo * no se incluya (leer analisis)
                            if item["title"] == "*":
                                continue
                            title_episode = item["title"]
                            cleantitle_epi = _replace(title_episode)
                            parent_id = id_serie
                            parent_title = title_serie
                            deeplink_epi = item["url"]
                            #acá validamos que tenga temporada ya que 
                            #de forma contraria es un especial
                            if item.get("season"):
                                episode_num = item["season"]["episodeAiringOrder"]
                            else:
                                episode_num = 0
                            #acá validamos igual que los capitulos para ver si es un especial
                            if item.get("season"):
                                season_num = item["season"]["seasonNumber"]
                            else:
                                season_num = 0

                            type_epi = item["type"]
                            #validamos que tenga fecha ya que sino esta no tiene año
                            if item.get("airDateNY"):
                                year_epi = item["airDateNY"]["year"]
                            else:
                                year_epi = None

                            #validamos que tenga duration
                            if item.get("duration"):
                                #si la duracion es 0 le ponemos None
                                if item["duration"] == 0:
                                    duration_epi = None
                                else: 
                                    #si tiene duracion la asignamos
                                    duration_epi = item["duration"]
                            else:
                                #si la api no trae duracion es igual a None
                                duration_epi = None

                            
                            synopsis_epi = item["description"]

                            #validamos que tenga imagen
                            image_epi = []
                            if item.get("images"):
                                #aqui usamos un try ya que en algunos casos la imagen
                                #viene en una lista y en otros como diccionario
                                try:
                                    image_epi = [item["images"]["url"]]
                                except TypeError :
                                    for imagen in item["images"]:
                                        image_epi.append(imagen["url"])       
                            else:
                                image_epi = None
                            
                            packages_epi = [{"Type": "tv-everywhere"}]

                            payload_epi = {
                                "PlatformCode":  self._platform_code, #Obligatorio      
                                "Id":            id_episode, #Obligatorio listo               
                                "ParentId":      parent_id, #Obligatorio #Unicamente en Episodios
                                "ParentTitle":   parent_title, #Unicamente en Episodios 
                                "Episode":       episode_num, #Obligatorio #Unicamente en Episodios  
                                "Season":        season_num, #Obligatorio #Unicamente en Episodios
                                "Title":         title_episode, #Obligatorio,      
                                "CleanTitle":    cleantitle_epi, #Obligatorio      
                                "OriginalTitle": None,                          
                                "Type":          type_epi,     #Obligatorio      
                                "Year":          year_epi,     #Important!     
                                "Duration":      duration_epi,      
                                "ExternalIds":   None,      
                                "Deeplinks": {          
                                    "Web":       deeplink_epi,       #Obligatorio          
                                    "Android":   None,          
                                    "iOS":       None,      
                                },      
                                "Synopsis":      synopsis_epi,      
                                "Image":         image_epi,      
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
                                "Packages":      packages_epi,    #Obligatorio      
                                "Country":       None,      
                                "Timestamp":     datetime.now().isoformat(), #Obligatorio
                                "CreatedAt":     self._created_at, #Obligatorio
                            }

                            Datamanager._checkDBandAppend(self, payload_epi, self.payloads_epi_db, self.payloads_epi, isEpi=True)

        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)   
        Datamanager._insertIntoDB(self, self.payloads_epi, self.titanScrapingEpisodios)
        if self.test:
            Upload(self._platform_code, self._created_at,testing=True)
        else:
            Upload(self._platform_code, self._created_at,testing=True)           

                
                

                
