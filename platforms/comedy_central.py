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
from time                   import sleep
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager  import Datamanager
from updates.upload         import Upload

class Comedy_central():

    """  
    DATOS IMPORTANTES:
    ¿Necesita VPN? -> NO
    ¿HTML, API, SELENIUM? -> API
    Cantidad de contenidos (ultima revisión): Series = 185, Episodios = 4093
    Tiempo de ejecucíon de Script = 15 Minutos
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

    def _scraping(self, testing=False):

        ids_series_guardados = Datamanager._getListDB(self,self.titanScraping)
        ids_epi_guardados = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        parent_title_id_list = []
        series = []
        episodios = []
        pagina = 0       
        while True: 
            link = "https://www.cc.com/api/search?q=&activeTab=All&searchFilter=site&pageNumber={}&rowsPerPage=100".format(pagina)
            api_request = self.currentSession.get(link)
            api = api_request.json()
                #este if not es para romper el while en caso de que la siguiente request no traiga items, que serian las series/episodios
            if not api["response"]["items"]:
                break
            for item in api["response"]["items"]:
                #insertamos las series en la lista de series y se corrobora que no haya repetidos
                if item["type"] == "series" and item not in series:
                    series.append(item)
                    print("serie insertada")
                #insertamos los episodios en la lista de episodios y se corrobora que no haya repetidos
                elif item["type"] == "episode" and item not in episodios:
                    episodios.append(item)
                    print("episodio insertado")
            #se incrementa el contador en uno para que la siguiente request a la api sea la pagina siguiente
            pagina = pagina+1

        #por cada serie sacamos los datos y hacemos el payload
        for serie in series:
            id_serie = hashlib.md5(serie["id"].encode('utf-8')).hexdigest()
            title_serie = serie["meta"]["header"]["title"]

            #agregamos el titulo y id de la serie a una lista para mas tarde
            #poder asignar a los capitulos el parentId si hace match con el nombre
            parent_title_id_list.append({"Title":title_serie,"Id":id_serie}) #solo esto cambie

            clean_title_serie = _replace(title_serie)
            type_serie = "serie"
            year_serie = None
            duration_serie = None
            deeplinks_serie = serie["url"]
            image_serie = [serie["media"]["image"]["url"]]
            cast_serie = None
            director_serie = None
            packages_serie = [{"Type": "tv-everywhere"}]
            #este try es porque el link del show de john olive
            #entraba en loop de redirecciones y no se podia hacer request
            try:
                request_serie = self.currentSession.get(deeplinks_serie)
            except requests.exceptions.TooManyRedirects:
                synopsis_serie = None
            #acá checkeamos si el status de la serie es 200 ya que hay show que al entrar son error 404
            #y por ende no se le puede sacar la synopsis
            if request_serie.status_code == 200:
                #Request a la serie para ver la synopsis si no tiene es none
                html_serie = BS(request_serie.text,features="lxml")
                if html_serie.find("div",{"class":"css-7cmkcr-Box-StyledMainContent"}):
                    synopsis_serie = html_serie.find("div",{"class":"css-7cmkcr-Box-StyledMainContent"}).text
                else:
                    synopsis_serie = None
            elif request_serie.status_code == 404:
                synopsis_serie = None
            else:
                continue
            payload = {      
            "PlatformCode":  self._platform_code, #Obligatorio  
            "Id":            id_serie, #Obligatorio    
            "Seasons":       None,
            "Title":         title_serie, #Obligatorio      
            "CleanTitle":    clean_title_serie, #Obligatorio  
            "OriginalTitle": None,                              
            "Type":          type_serie,     #Obligatorio      
            "Year":          year_serie,     #Important!     
            "Duration":      duration_serie,      
            "ExternalIds":   None,      
            "Deeplinks": {          
                "Web":       deeplinks_serie,       #Obligatorio          
                "Android":   None,          
                "iOS":       None,      
            },      
            "Synopsis":      synopsis_serie,      
            "Image":         image_serie,      
            "Rating":        None,     #Important!      
            "Provider":      None,      
            "Genres":        None,    #Important!      
            "Cast":          cast_serie,      
            "Directors":     director_serie,    #Important!      
            "Availability":  None,     #Important!      
            "Download":      False,      
            "IsOriginal":    True,    #Important!      
            "IsAdult":       None,    #Important!   
            "IsBranded":     None,    #Important!   
            "Packages":      packages_serie,    #Obligatorio      
            "Country":       None,      
            "Timestamp":     datetime.now().isoformat(), #Obligatorio      
            "CreatedAt":     self._created_at, #Obligatorio
            }
            Datamanager._checkDBandAppend(self, payload, ids_series_guardados, self.payloads)
        
        for epi in episodios:
            parent_title = epi["meta"]["label"]
            #acá checkeamos que el episodio tenga id, ya que en caso de no tenerlo tampoco tiene URL y es inutil
            if epi.get("id") == "[]":
                continue
            elif epi.get("id"): 
                id_epi = hashlib.md5(epi["id"].encode('utf-8')).hexdigest()

            else:
                continue
            #acá accedemos a lista que contiene contiene el id y el titulo de las series
            #si coincide el parentTitle que figura en el item episodio con el de la lista
            #entonces se asigna ese parentId al episodio
            for payload in parent_title_id_list: 
                if payload["Title"] == parent_title:
                    parent_Id = payload["Id"]
                    print("COINCIDE")
                    break
                else:
                    print("NO COINCIDE")
                    parent_Id = hashlib.md5(parent_title.encode('utf-8')).hexdigest()

            title_epi = epi["meta"]["subHeader"]

            if "- Uncensored" in title_epi:
                title_epi = title_epi.split(" - Uncensored")[0]

            if " - Up Next" in title_epi:
                title_epi = title_epi.split(" - Up Next")[0]

            if "Extended -" in title_epi:
                title_epi = title_epi.split("Extended - ")[-1]
            
            if ("-" in title_epi) and ("," in title_epi) and ("2" in title_epi) and (parent_title != "This Week at the Comedy Cellar") and (parent_title != "@midnight with Chris Hardwick"):
                title_epi = title_epi.split(" - ")[-1]
            
            if title_epi == "Kate Berlant Teaches":
                title_epi = title_epi.split("Kate Berlant Teaches - ")[-1]

            clean_title_epi = _replace(title_epi)
            original_title = None
            type_epi = "episode"

            #acá checkeamos que tenga fecha y como esta en formato DD/MM/YY solo le asignamos el YY que seria el año
            if epi.get("meta",{}).get("date"):
                dirty_year = epi["meta"]["date"]
                year_epi = dirty_year.split("/")[-1]
            else:
                year_epi = None

            #verificamos que tenga duracion del episodio y como esta en str con formato "22:50" lo spliteamos 
            #a partir del : y asignamos la primera parte
            if epi["media"]["duration"] == None:
                duration_epi = None 

            elif ":" in epi["media"]["duration"]:
                duration_epi = int(epi["media"]["duration"].split(":")[0])

            deeplink_epi = epi["url"]   
            
            synopsis_epi = epi["meta"]["description"]
            
            #para la seccion imagen habian dos ubicaciones distintas segun como viniera, acá validamos que este
            #en alguno de esos 2 lados, de lo contrario o tiene imagen y se asigna None
            if epi.get("image",{}).get("url"):
                image_epi = [epi["image"]["url"]]

            elif epi.get("media",{}).get("image",{}).get("url"):
                image_epi = [epi["media"]["image"]["url"]]

            else:
                image_epi = None
            
            packages_epi =  [{"Type": "tv-everywhere"}]

            #Acá nos saltamos los "episodios" que realmente son un preview de la temporada
            if "preview" in deeplink_epi:
                continue

            #acá asignamos el numero de episodio y temporada. Si el Deeplink indica que es un episodio especial
            #se le asigna el numero 0 a la tempo y el episodio es igual a None
            if ("ep-special" in deeplink_epi) or ("not-special" in deeplink_epi) :
                season_num = 0
                episode_num = None

            else:
                #si no es especial, tenemos que sacar el numero de episodio y temporada haciendo request :(
                request = self.currentSession.get(deeplink_epi)

                #accedemos solo a lo episodios que se pueden ver:
                if request.status_code == 200:
                    soup = BS(request.text, features="lxml")
                    contenedor_epi_sea = soup.find("div",{"class":"sub-header"})
                    sea_epi_num = contenedor_epi_sea.span.text # == Season 25 E 69 • 03/03/2020
                    split = sea_epi_num.split("•")[0] # ==  "Season 25 E 69 " o  "Season 4 " o " E 2 "

                    #si al dividirlo por un espacio viene asi (['Season', '9', '']) hacemos esto:
                    if len(split.split(" ")) == 3:
                        season_num = int(split.split(" ")[1])
                        episode_num = None

                    #si al dividirlo por un espacio viene asi (['', 'E', '1', '']) hacemos esto: 
                    elif len(split.split(" ")) == 4:
                        season_num = None
                        episode_num = int(split.split(" ")[2])

                    #si al dividirlo por un espacio viene asi (['Season', '25', 'E', '69', '']) hacemos esto: 
                    elif len(split.split(" ")) == 5:
                        season_num = int(split.split(" ")[1])
                        episode_num = int(split.split(" ")[3])
                        
                else:
                    continue

            #armamos el payload
            payload_epi = {
                "PlatformCode":  self._platform_code, #Obligatorio      
                "Id":            id_epi, #Obligatorio listo               
                "ParentId":      parent_Id, #Obligatorio #Unicamente en Episodios
                "ParentTitle":   parent_title, #Unicamente en Episodios 
                "Episode":       episode_num, #Obligatorio #Unicamente en Episodios  
                "Season":        season_num, #Obligatorio #Unicamente en Episodios
                "Title":         title_epi, #Obligatorio,      
                "CleanTitle":    clean_title_epi, #Obligatorio      
                "OriginalTitle": original_title,                          
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
            
            Datamanager._checkDBandAppend(self, payload_epi, ids_epi_guardados, self.payloads_epi, isEpi=True)

        Datamanager._insertIntoDB(self, self.payloads, self.titanScraping)   
        Datamanager._insertIntoDB(self, self.payloads_epi, self.titanScrapingEpisodios)

        Upload(self._platform_code, self._created_at,testing=testing)