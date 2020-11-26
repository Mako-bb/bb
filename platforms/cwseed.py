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
            
            prod_def = []

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
                prod_def = []
                lista1 = prod.split(')')
                for i in lista1:
                    if i != '':
                        x = i.split(' (')[0].strip(' ')
                        if '\r' in x:
                            y = x.split('\r')
                            for item in y:
                                prod_def.append(item)
                        else:
                            prod_def.append(x)
                
            else:
                prod_def = None
            print(prod_def)
            
            
            # Limpia el CAST
            cast_def = []
            cast_list = cast.split(')')
            for i in cast_list:
                if i != '':
                    x = i.split(' (')[0].strip(' ')
                    if '\r' in x:
                        y = x.split('\r')
                        for item in y:
                            cast_def.append(item)
                    else:
                        cast_def.append(x)    
            print(cast_def) 
                      # Cast definitivo
            
            

            # Quita el cast sucio
            """
            if titulo == "Being Reuben":
                cast2 = None
                prod_def = None
            elif titulo == "Dead Pixels":
                prod_def = None
            elif titulo == "Taskmaster":
                cast2 = None
            elif titulo == "The Secret Circle":
                prod_def = None
            elif titulo == "Whose Line Is It Anyway?":
                cast2 = None
            """
            
            
            

            #RATING
            rating = epi_soup.find("span",{"class":"rating"}).text
            if "L, V" in rating:
                rating = rating.replace(" L, V","")
            
            
            
            #DIFERENCIA MOVIE O SERIE según duración
            
            dif_dur = epi_soup.find("span",{"class":"dura"}).text
            dur = dif_dur.replace(" min", "")
            dur = int(dur)          # Duracion solo para comparar
            

            # Entramos a TODOS los episodios
            all_episodes = epi_soup.find("div",{"class": "secondary-videos video-thumbnail-list"})
            lista = len(all_episodes.findAll("li"))

            #DIFERENCIA MOVIE O SERIE segun cantidad de episodios & duración
            if lista == 1 and dur > 70:
                tipo = "movie"
                all_duration = dur              # Duracion definitiva
            else:
                tipo = "serie"
                all_duration = None             # Duracion definitiva


            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            str(id_link),
                'Title':         titulo,
                'OriginalTitle': None,
                'CleanTitle':    _replace(titulo),
                'Type':          tipo,
                'Year':          None,
                'Duration':      all_duration,
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
                'Cast':          cast_def,
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
            Datamanager._checkDBandAppend(self,payload,listDBMovie,listPayload)
            


#           ---------------------------------------------------------------------------
#                                       EPISODIOS
            if tipo == "serie":
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

                    #Consigue el rating del episodio
                    rating_epi = epi.find("span",{"class":"rating"}).text
                    if "L, V" in rating_epi:
                        rating_epi = rating_epi.replace(" L, V","")
                    
                                    
                    
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
                        'Rating'        : rating_epi,
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
                    Datamanager._checkDBandAppend(self,payloadEpi,listDBEpi,listPayloadEpi,isEpi=True)
                                        
                Datamanager._insertIntoDB(self,listPayload,self.titanScraping)
                Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios)

                # Upload
                Upload(self._platform_code, self._created_at, testing=True)   
            
            

                

            
            
