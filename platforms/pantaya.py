"""         30/11/20
La plataforma tiene series y peliculas, el paquete que ofrece es SUSCRIPCION pero tambien ofrece algunas
peliculas y episodios gratuitos, los cuales fueron "marcados" como "free-vod". El html de la página es
DINÁMICO (carga con js) por lo que se utilizaron VARIAS APIS que la plataforma tiene.
APIS: una api con todo el contenido (series y peliculas); otra api con el contenido de cada serie y sus
episodios; otra api que contiene los episodios que son FREE.
IMPORTANTE: Las series/peliculas "coming soon" se saltearon. línea 65
            Los actores y directores están en un diccionario en keys distintas, con la key "roles" dentro
            de las cuales tenemos (por ejemplo) 0: writer 1: actor 2: director; esto implica que la misma
            persona es writer, actor y director. Se tuvo en cuenta sólo hasta el rol 2. 
            VER SI ESTO CAMBIA
OBSERVACIONES: El scrapeo no presentó mayores dificultades gracias a toda la data facilitada por las APIS.
"""

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

class Pantaya():
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
         # Trae lista de Data Base & declara lista payload episodios
        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        listPayload = []
        listPayloadEpi = []

        
  
        # Trae el json
        URL = "https://playdata.starz.com/metadata-service/play/partner/Pantaya_Web/v5/blocks?playContents=map&pages=BROWSE,HOME,MOVIES,PLAYLIST,SEARCH,SEARCHRESULTS,SERIES&lang=en-US&includes=contentId,contentType,title,product,seriesName,seasonNumber,free,comingSoon,newContent,topContentId,properCaseTitle,categoryKeys,runtime,popularity,product,original,firstEpisodeRuntime,releaseYear,images,minReleaseYear,maxReleaseYear,episodeCount"
        json = Datamanager._getJSON(self,URL)
        
        # Todo el contenido
        all_content = json['blocks'][7]['playContentsById']
        
        for content in all_content:
            if "comingSoon" in all_content[content]:    #Saltea los Coming Soon
                continue
            else:       #Tanto para movies como para series: 
                title = all_content[content]["title"]       #Titulo
                get_tipo = all_content[content]["contentType"]  #Get tipo
                id_ = all_content[content]["contentId"]         #Id


                if get_tipo == "Movie":        #MOVIES
                    time_sec = all_content[content]["runtime"]      #En segundos
                    runtime = time_sec // 60                     #Duracion definitiva
                    year = all_content[content]['releaseYear']      #Year
                    tipo = "movie"                                  #Tipo
                    deeplink = "https://www.pantaya.com/en/movies/{}".format(id_) 
                    if "free" in all_content[content]:
                        packages = [
                            {
                                "Type": "free-vod"
                            }
                        ]
                    else:
                        packages = [
                            {
                                "Type": "suscription-vod"
                            }
                        ]
                    if "Celebremos Teaser" in title and runtime < 2:
                        continue

                    # Entra a cada movie y extrae info
                    url_movie = "https://playdata.starz.com/metadata-service/play/partner/Pantaya_Web/v5/content?lang=en-US&contentIds={}&includes=title,logLine,contentType,contentId,topContentId,releaseYear,runtime,credits".format(id_)
                    json_movie = Datamanager._getJSON(self,url_movie)
                    data = json_movie['playContentArray']['playContents']
                    sinopsis = data[0]['logLine']       #Sinopsis
                    roles = data[0]['credits']           
                    cast = []                           #Cast
                    directors = []                      #Directors
                    
                    for element in roles:           #Trae todo los cast y directos y enlistalos
                        role = element["roles"][0]
                        name = element["name"]                    
                        if role == 'Actor':
                            cast.append(name)
                        else:
                            directors.append(name)
                    
                
                else:           # SERIES
                    year = all_content[content]['minReleaseYear']   #Year
                    tipo = "serie"                                  #Tipo
                    deeplink = "https://www.pantaya.com/en/series/{}".format(id_)   #Deeplink
                    runtime = None

                
                    # Entra a cada serie y extrae info
                    url_serie = "https://playdata.starz.com/metadata-service/play/partner/Pantaya_Web/v5/content?lang=en-US&contentIds={}&includes=title,logLine,contentType,contentId,topContentId,releaseYear,runtime,credits,childContent,order".format(id_)
                    json_serie = Datamanager._getJSON(self,url_serie)
                    data = json_serie['playContentArray']['playContents']
                    sinopsis = data[0]['logLine']       #Sinopsis
                    roles = data[0]['credits']           
                    cast = []                           #Cast
                    directors = []                      #Directors
                    
                    for element in roles:           #Trae todo los cast y directos y enlistalos
                        role = element["roles"][0]
                        name = element["name"]
                                            
                        if role == 'Actor':
                            cast.append(name)
                        else:
                            directors.append(name)
                
                payload = {
                'PlatformCode':  self._platform_code,
                'Id':            str(id_),
                'Title':         title,
                'OriginalTitle': None,
                'CleanTitle':    _replace(title),
                'Type':          tipo,
                'Year':          year,
                'Duration':      runtime,
                'Deeplinks': {
                    'Web':       deeplink,
                    'Android':   None,
                    'iOS':       None,
                    },
                'Playback':      None,
                'Synopsis':      sinopsis,
                'Image':         None,
                'Rating':        None,
                'Provider':      None,
                'Genres':        None,
                'Cast':          cast,
                'Directors':     directors,
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
                
                if tipo == "serie":
                    # Entra a cada episodio        
                    data_epi = json_serie['playContentArray']['playContents'][0]['childContent']
                    for temporada in data_epi:
                        nroSeason = temporada['order']      # Numero de Season
                        content_epi = temporada['childContent']
                        
                        for episodio in content_epi:
                            #Titulo
                            title_epi = episodio['title']
                            title_epi = title_epi.split(":")
                            title_epi = title_epi[1]
                            #Numero del Episodio
                            nroEpi = episodio['order']
                            #Año del Episodio
                            year_epi = episodio['releaseYear']
                            #Sinopsis del Episodio
                            sinopsis_epi = episodio['logLine']
                            # ID del Episodio
                            id_epi = episodio['contentId']
                            # Duración del Episodio
                            runtime_epi = episodio['runtime']
                            duration_epi = runtime_epi // 60    #Duracion definitiva
                            # Cast y Directors del Episodio
                            roles_epi = episodio['credits']
                            cast_epi = []                           #Cast
                            directors_epi = []                      #Directors
                            for item in roles_epi:           #Trae todo los cast y directors y enlistalos
                                role = item["roles"][0]
                                name = item["name"]                    
                                if role == 'Actor' and name != 'Pantaya':
                                    cast_epi.append(name)
                                elif role == 'Director' and name != 'Pantaya':
                                    directors_epi.append(name)
                                try:
                                    role2 = item['roles'][1]
                                    if role2 == 'Actor' and name != 'Pantaya':
                                        cast_epi.append(name)
                                    elif role2 == 'Director' and name != 'Pantaya':
                                        directors_epi.append(name)
                                except:
                                    continue
                                try:
                                    role3 = item['roles'][2]
                                    if role3 == 'Actor' and name != 'Pantaya':
                                        cast_epi.append(name)
                                    elif role3 == 'Director' and name != 'Pantaya':
                                        directors_epi.append(name)
                                except:
                                    continue
                            


                            # Limpia cast y directors vacíos == None
                            if len(cast_epi) == 0:
                                    cast_epi = None
                            if len(directors_epi) == 0:
                                    directors_epi = None
                            
                            # Deeplink del Episodio
                            epi_deeplink = "https://www.pantaya.com/en/play/{}".format(id_epi)
                            # Consigue el package del Episodio
                            url_pack = "https://playdata.starz.com/metadata-service/play/partner/Pantaya_Web/v5/content?lang=es-419&contentIds={}&includes=title,contentId,contentType,runtime,creditTimeIn,nextContentId,seriesName,seasonNumber,order,topContentId,free,original".format(id_epi)
                            json_pack = Datamanager._getJSON(self,url_pack)
                            pack = json_pack['playContentArray']['playContents'][0]
                            if "free" in pack:
                                packages_epi = [
                                    {
                                        "Type": "free-vod"
                                    }
                                ]
                            else:
                                packages_epi = [
                                    {
                                        "Type": "suscription-vod"
                                    }
                                ]


                            payloadEpi = {
                                'PlatformCode'  : self._platform_code,
                                'ParentId'      : str(id_),
                                'ParentTitle'   : title,
                                'Id'            : str(id_epi),
                                'Title'         : title_epi,
                                'Episode'       : int(nroEpi),
                                'Season'        : int(nroSeason),
                                'Year'          : year_epi,
                                'Duration'      : int(duration_epi),
                                'Deeplinks'     : {
                                    'Web': epi_deeplink,
                                    'Android': None,
                                    'iOS': None
                                },
                                'Synopsis'      : sinopsis_epi,
                                'Rating'        : None,
                                'Provider'      : None,
                                'Genres'        : None,
                                'Cast'          : cast_epi,
                                'Directors'     : directors_epi,
                                'Availability'  : None,
                                'Download'      : None,
                                'IsOriginal'    : None,
                                'IsAdult'       : None,
                                'Country'       : None,
                                'Packages'      : packages_epi,
                                'Timestamp'     : datetime.now().isoformat(),
                                'CreatedAt'     : self._created_at
                            }
                            Datamanager._checkDBandAppend(self,payloadEpi,listDBEpi,listPayloadEpi,isEpi=True)
                                                
                Datamanager._insertIntoDB(self,listPayload,self.titanScraping)
                Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios)
                
                # Upload
                Upload(self._platform_code, self._created_at, testing=True)   
            
            
           
          
                
