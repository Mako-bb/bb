# -*- coding: utf-8 -*-
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

class TruTV():
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
            
    def _scraping(self):
        # https://api.trutv.com/v3/web/series  #SERIES
        # https://api.trutv.com/v3/web/series/landing/adam-ruins-everything #DETALLE SERIE
        # https://api.trutv.com/v3/web/series/episodes/adam-ruins-everything #EPISODIOS
        
        URL = "https://api.trutv.com/v2/web/series"
        dataSeries = Datamanager._getJSON(self,URL)

        listaSeriesDB = []
        listaSeries = []
        listaSeriesDB = Datamanager._getListDB(self,self.titanScraping)
        
        listaEpiDB = []
        listaEpi = []
        listaEpiDB = Datamanager._getListDB(self,self.titanScrapingEpisodios)
                
        for serie in dataSeries['shows']:
            URLDetallesSerie = "https://api.trutv.com/v2/web/series/landing/" + serie['slug']
            dataSerie = Datamanager._getJSON(self,URLDetallesSerie)
            print(dataSerie['hero']['title'])
            
            images = []
            images.append(dataSerie['hero']['images']['key'][0]['srcUrl'])
            
            packages = [
                {
                    'Type'  : 'tv-everywhere',
                }
            ]             
            
            try:
                castList = []
                for cast in dataSerie['cast']:
                    castList.append(cast['name'])
            except Exception as e:
                print(repr(e))
                castList = None
            
            payload = {
                    'PlatformCode':  self._platform_code,
                    'Id':            serie['slug'],
                    'Title':         dataSerie['hero']['title'],
                    'CleanTitle':    _replace(dataSerie['hero']['title']),
                    'OriginalTitle': None,
                    'Type':          'serie', # 'movie' o 'serie'
                    'Year':          None,
                    'Duration':      None, # duracion en minutos
                    'Deeplinks': {
                        'Web':       "https://www.trutv.com/shows/" + serie['slug'],
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      dataSerie['about']['description'],
                    'Image':         images, # [str, str, str...] # []
                    'Rating':        dataSerie['about']['rating'],
                    'Provider':      None,
                    'Genres':        None, # [str, str, str...]
                    'Cast':          castList, # [str, str, str...]
                    'Directors':     None, # [str, str, str...]
                    'Availability':  None,
                    'Download':      None,
                    'IsOriginal':    None,
                    'IsAdult':       None,
                    'Packages':      packages,
                    'Country':       None, # [str, str, str...]
                    'Timestamp':     datetime.now().isoformat(),
                    'CreatedAt':     self._created_at
            }
            Datamanager._checkDBandAppend(self,payload,listaSeriesDB,listaSeries)
            
            URLEpisodios = "https://api.trutv.com/v2/web/series/episodes/" + serie['slug']
            dataEpi = Datamanager._getJSON(self,URLEpisodios)
            
            for episodio in dataEpi['episodes']:
                
                year = str(episodio['originalPremiereDate'])[0:4]
                year = int(year)
                
                payloadEpi = {
                    'PlatformCode'  : self._platform_code,
                    'ParentId'      : serie['slug'],
                    'ParentTitle'   : dataSerie['hero']['title'],
                    'Id'            : str(episodio['titleId']),
                    'Title'         : episodio['title'],
                    'Episode'       : episodio['episodeNum'],
                    'Season'        : episodio['seasonNum'],
                    'Year'          : year,
                    'Duration'      : int(episodio['duration'])//60,
                    'Deeplinks'     : {
                        'Web': "https://www.trutv.com/full-episodes/{}/{}".format(serie['slug'],episodio['titleId']),
                        'Android': None,
                        'iOS': None
                    },
                    'Synopsis'      : episodio['description'],
                    'Rating'        : dataSerie['about']['rating'],
                    'Provider'      : None,
                    'Genres'        : None,
                    'Cast'          : castList,
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
                Datamanager._checkDBandAppend(self,payloadEpi,listaEpiDB,listaEpi,isEpi=True)
        
        Datamanager._insertIntoDB(self,listaSeries,self.titanScraping)
        Datamanager._insertIntoDB(self,listaEpi,self.titanScrapingEpisodios)
        '''
        Upload
        '''
        self.sesion.close()
        Upload(self._platform_code, self._created_at, testing=True)            
        