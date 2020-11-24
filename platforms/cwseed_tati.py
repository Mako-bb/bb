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

class CwSeed_Tati():
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
            print(long_prod)


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
                print("NO hay data")

            # Limpia el Directors
            while prod != None:
                if prod.find('(') != -1:
                        aux = prod[prod.index('('):prod.index(')') + 1]
                        prod = prod.replace(aux, '')
                        prod_def = prod.split("  ")
                        print(prod_def)
                else:
                    break
            
            
    
            # Limpia el CAST 
            while True:
                if cast.find('(') != -1:
                    aux = cast[cast.index('('):cast.index(')') + 1]
                    cast = cast.replace(aux, '')
                else:
                    break
            cast2 = cast.split("  ")
            print(cast2)                    # Cast definitivo
            
            
            
            #FALTAN PRODUCTORES , rating y cast!!!!!!!!!!!!!!!!!!!!!!

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
                'Rating':        None,
                'Provider':      None,
                'Genres':        None,
                'Cast':          cast2,
                'Directors':     None,
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
            

    
            all_episodes = epi_soup.find("div",{"class": "secondary-videos video-thumbnail-list"})

            for epi in all_episodes.findAll("li"):
                epi_deeplink = epi.get("data-videourl") #Links a todos los episodios de cada serie
                       

                

            #Sacar el link a cada episodio desde el html y luego sacar la info en el json?
            
