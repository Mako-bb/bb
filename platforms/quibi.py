# -*- coding: utf-8 -*-
import json
import time
import requests
import hashlib
import platform
import sys, os   
from common                 import config
from bs4                    import BeautifulSoup
from datetime               import datetime
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.datamanager     import Datamanager
from handle.replace         import _replace

class Quibi():
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
        listaSeries = []
        listaSeriesDB = Datamanager._getListDB(self,self.titanScraping)
        
        listaEpi = []
        listaEpiDB = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        packages = [
            {
                'Type' : 'subscription-vod'
            }
        ]

        ###GET KEY
        quibiKey = Datamanager._getSoup(self,"https://quibi.com/shows/")
        for g in quibiKey.findAll('link',{'rel':'preload'}):
            if 'shows.js' in g.get('href'):
                KEY = g.get('href').split('/')[3]
        
        URL = "https://quibi.com/_next/data/{}/shows.json".format(KEY)
        dataSeries = Datamanager._getJSON(self,URL)

        for title in dataSeries['initialProps']['appProps']['content']['allQuibiShows']:

            URLSerie = "https://quibi.com/_next/data/{}/shows/{}.json".format(KEY,title['deeplink_url'].split("/")[-1])
            dataSerie = Datamanager._getJSON(self,URLSerie)
            try:
                dataSerie = dataSerie['pageProps']['pageContent']['showData']
            except:
                dataSerie = None
            
            genres = title['tagline'].split(",")

            images = []
            images.append(title['showLogo'])
            images.append(title['showThumbnail'])

            cast = []
            if dataSerie != None:
                for castMember in dataSerie['credits']:
                    if Datamanager._checkIfKeyExists(castMember,'credit_type'):
                        if castMember['credit_type'] == "CREDIT_TYPE_CAST":
                            cast.append(castMember['artist']['name'])
            if cast == []:
                cast = None

            payload = {
                'PlatformCode':  self._platform_code, #str
                'Id':            str(title['id']), #str
                'Title':         title['title'], #str
                'CleanTitle':    _replace(title['title']), #str
                'OriginalTitle': None,#str
                'Type':          'serie', # 'movie' o 'serie'#str
                'Year':          None, #int
                'Duration':      None, # duracion en minutos #int
                'Deeplinks': {
                    'Web':       title['deeplink_url'], #str
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      dataSerie['description'] if dataSerie != None else None,
                'Image':         images, # [str, str, str...] # []
                'Rating':        None, #str
                'Provider':      None,
                'Genres':        genres, # [str, str, str...]
                'Cast':          cast, # [str, str, str...]
                'Directors':     None, # [str, str, str...]
                'Availability':  None,
                'Download':      None, #bool
                'IsOriginal':    None, #bool
                'IsAdult':       None, #bool
                'Packages':      packages,
                'Country':       None, # [str, str, str...]
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }
            if dataSerie['episodes'] != []:
                Datamanager._checkDBandAppend(self,payload,listaSeriesDB,listaSeries)

            if dataSerie != None:
                for epi in dataSerie['episodes']:
                    
                    payloadEpi = {
                        'PlatformCode'  : self._platform_code,
                        'ParentId'      : str(title['id']),
                        'ParentTitle'   : title['title'],
                        'Id'            : hashlib.md5(epi['title'].encode('utf-8')).hexdigest(),
                        'Title'         : epi['title'],
                        'Episode'       : int(epi['number']),
                        'Season'        : int(epi['season']),
                        'Year'          : None,
                        'Duration'      : None,
                        'Deeplinks'     : {
                            'Web': title['deeplink_url'],
                            'Android': None,
                            'iOS': None
                        },
                        'Synopsis'      : epi['description'],
                        'Rating'        : None,
                        'Provider'      : None,
                        'Genres'        : genres,
                        'Cast'          : cast,
                        'Directors'     : None,
                        'Availability'  : None,
                        'Download'      : None,
                        'IsOriginal'    : None,
                        'IsAdult'       : None,
                        'Country'       : None,
                        #'IsPremium'     : None,
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