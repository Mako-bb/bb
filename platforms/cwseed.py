import json
import time
import requests
import hashlib   
from common                 import config
from bs4                    import BeautifulSoup
from datetime               import datetime
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.datamanager  import Datamanager
from handle.replace         import _replace
import re

class CwSeed():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)  
            
    def _scraping(self, testing = False):
        URL = "https://www.cwseed.com/shows/genre/shows-a-z/"
        soup = Datamanager._getSoup(self, URL)

        # Trae lista de Data Base & declara lista payload episodios
        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        listPayload = []
        listPayloadEpi = []

        # Declaramos tipo de package
        packages = [
            {
                "Type": "free-vod"
            }
        ]

        # Traeme todos los contenidos
        all_titles = soup.find("div", {"id": "show-hub"})
        
        # Iteramos para extraer la data
        for item in all_titles.findAll("li",{"class":"showitem"}):
            deeplink = item.find("a").get("href")       #No es link, falta rellener con el ID de cada episodio
            titulo = item.find("p").text                #Title del TV SHOW
            
            #ID de serie
            id_link = item.find("a").get("data-slug")
           
            # Link de cada serie (por default redirige al primer episodio de la serie)
            arma_link = 'https://www.cwseed.com/shows/{}'.format(id_link)
            


            # Armar un soup del armalink y extraer data

            epi_soup = Datamanager._getSoup(self,arma_link)

            # Extraer la data ABOUT THE SHOW
            about = epi_soup.find("div",{"class": "synopsis"})
            
            
            all_data = about.findAll("p")             
            sinopsis = all_data[0].text             # Sinopsis
                             
            long_prod = len(all_data)
            #print(long_prod)


            # Cast & Producers sin limpiar
            if long_prod == 3:
                cast = all_data[1].text
                prod = all_data[2].text             
            elif long_prod == 4:
                prod = all_data[3].text
                cast = all_data[2].text
            else:
                prod = None
                cast = all_data[1].text
                

            # Limpia el Directors
            if prod != None:
                while True:
                    if prod.find('(') != -1:
                        aux = prod[prod.index('('):prod.index(')') + 1]
                        prod = prod.replace(aux, '')
                        prod_def = prod.split("  ")
                        if "/r" in prod_def:
                            prod_def = prod_def.replace("\\\\r",", ")
                        elif "Herself" in prod_def:
                            prod_def = prod_def.replace(" as Herself","")
                        elif "Himself" in prod_def:
                            prod_def = prod_def.replace(" as Himself","")
                    else:
                        break
            else:
                prod_def = None

        
            print(prod_def)

            #FALTA LIMPIAR ALGUNOS COMO /R reemplazar por "," O "AS HIMSELF" reemplaza x ""
             # FALTA LIMPIAR EL RATING sacar "l,v"
            
    
            # Limpia el CAST 
            while True:
                if cast.find('(') != -1:
                    aux = cast[cast.index('('):cast.index(')') + 1]
                    cast = cast.replace(aux, '')
                else:
                    break
            cast2 = cast.split("  ")
            print(cast2)                    # Cast definitivo
            
            #RATING
            rating = epi_soup.find("span",{"class":"rating"}).text
            print(rating)
            
            
            

            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            str(id_link),
                'Title':         titulo,
                'OriginalTitle': None,
                'CleanTitle':    _replace(titulo),
                'Type':          "serie",
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       arma_link,
                    'Android':   None,
                    'iOS':       None,
                        },
                'Playback':      None,
                'Synopsis':      sinopsis,
                'Image':         None,
                'Rating':        rating,
                'Provider':      None,
                'Genres':        None,
                'Cast':          cast2,
                'Directors':     prod_def,
                'Availability':  None,
                'Download':      None,
                'IsOriginal':    None,
                'IsAdult':       None,
                'Packages':      packages,
                'Country':       None,
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }
            #Datamanager._checkDBandAppend(self,payload,listDBMovie,listPayload)
            


#           ---------------------------------------------------------------------------
#                                       EPISODIOS

            all_episodes = epi_soup.find("div",{"class": "secondary-videos video-thumbnail-list"})

            for epi in all_episodes.findAll("li"):
                epi_dataurl = epi.get("data-videourl") #Links a todos los episodios de cada serie
                epi_deeplink = 'https://www.cwseed.com{}'.format(epi_dataurl)

                epi_id = epi.get("data-videoguid")      #ID -guid- de cada episodio

                epi_titulo = epi.find("span",{"class":"et"}).text

                #Consigue el numero de la Season
                get_season = epi.find("span",{"class":"en"}).text
                get_season2 = get_season.replace("S","")
                get_season3 = get_season2.split(":")
                nroSeason = get_season3[0]              #nro de Season
                

                #Consigue el numero de Episodio
                get_episode = epi.find("span",{"class":"en"}).text
                get_episode = get_episode.replace("E","")
                get_episode = get_episode.split(": ")
                nroEpi = get_episode[1]
                

                #Consigue la duracion del episode
                duration = epi.find("span",{"class":"dura"}).text
                duration_epi = duration.replace(" min", "")     #Duracion
                

                       
                
                payloadEpi = {
                    'PlatformCode'  : self._platform_code,
                    'ParentId'      : str(id_link),
                    'ParentTitle'   : titulo,
                    'Id'            : epi_id,
                    'Title'         : epi_titulo,
                    'Episode'       : int(nroEpi),
                    'Season'        : int(nroSeason),
                    'Year'          : None,
                    'Duration'      : int(duration_epi),
                    'Deeplinks'     : {
                        'Web': epi_deeplink,
                        'Android': None,
                        'iOS': None
                    },
                    'Synopsis'      : None,
                    'Rating'        : None,
                    'Provider'      : None,
                    'Genres'        : None,
                    'Cast'          : None,
                    'Directors'     : None,
                    'Availability'  : None,
                    'Download'      : None,
                    'IsOriginal'    : None,
                    'IsAdult'       : None,
                    'Country'       : None,
                    'Packages'      : packages,
                    'Timestamp'     : datetime.now().isoformat(),
                    'CreatedAt'     : self._created_at
                }
                #Datamanager._checkDBandAppend(self,payloadEpi,listDBEpi,listPayloadEpi,isEpi=True)
                                    
            #Datamanager._insertIntoDB(self,listPayload,self.titanScraping)
            #Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios) 
            
            

                

            
            
