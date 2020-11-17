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

class AdultSwim():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedTitles          = 0
        self.skippedEpis            = 0
        
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
        URL = "http://www.adultswim.com/videos"
        soup = Datamanager._getSoup(self,URL)
        
        items = soup.find('ul',{'class':'tK3iu'})
        
        listPayloadSerie = []
        listPayloadEpi = []
        listDBSerie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        
        for item in items.findAll('li'):
            link = item.find('a', {'class': '_39bM9'}).get('href')
            linkSerie = URL + link.replace("/videos","")
            contenidoId = hashlib.md5(linkSerie.encode('utf-8')).hexdigest()
            title = item.find('span',{'class':'_1WkqK'}).text  
            
            soupSerie = Datamanager._getSoup(self,linkSerie)
            
            # try:
            #     title = soupSerie.find('h1',{'class':'_17Jzh sT1fC'}).text
            # except:
            #     title = "Simulcast"

            packages = []
            firstEp = True
            for season in soupSerie.findAll('div',{'itemprop':'containsSeason'}):
                seasonNumber = season.find('meta',{'itemprop':'seasonNumber'}).get('content')
                for epi in season.findAll('div',{'class':'_29ThWwPi'}):
                    
                    try:
                        expiration = epi.find('meta',{'itemprop':'expires'}).get('content')
                    except:
                        expiration = None
                    
                    try:
                        epiNum = int(epi.find('meta',{'itemprop':'episodeNumber'}).get('content'))
                    except:
                        epiNum = None
                    
                    try:
                        epiTitle = epi.find('span',{'itemprop':'name'}).text
                    except:
                        epiTitle = "Simulcast"

                    try:
                        epiDescription = epi.find('meta',{'itemprop':'description'}).get('content')
                    except:
                        epiDescription = None

                    try:
                        epiRating = epi.find('meta',{'itemprop':'contentRating'}).get('content')
                    except:
                        epiRating = None

                    try:
                        epiYear = epi.find('meta',{'itemprop':'datePublished'}).get('content').split("-")[0]
                    except:
                        epiYear = None

                    try:
                        epiDuration = int(epi.find('meta',{'itemprop':'duration'}).get('content').split("M")[0].replace("T","").strip())
                    except:
                        epiDuration = None
                    
                    linkEpi = epi.find('a',{'itemprop':'url'}).get('href')
                    linkEpi = URL + linkEpi.replace("/videos","")

                    idEpi = hashlib.md5((linkEpi + title).encode('utf-8')).hexdigest()

                    if epi.find('svg',{'class':'_4ixT0cNJ GEFkIErH'}) == None:
                        packages = [
                            {
                                'Type'  : 'free-vod',
                            }
                        ]
                    else:
                        packages = [
                            {
                                'Type'  : 'tv-everywhere',
                            }
                        ]

                    if firstEp == True:
                        desc = epiDescription
                        rating = epiRating
                        year = epiYear
                        firstEp = False
                    
                    if linkSerie == 'http://www.adultswim.com/videos/specials':
                        seasonNumber = 0
                        epiNum = None

                    payloadEpi = {
                        'PlatformCode'  : self._platform_code,
                        'ParentId'      : contenidoId,
                        'ParentTitle'   : title,
                        'Id'            : idEpi,
                        'Title'         : epiTitle,
                        'Episode'       : epiNum,
                        'Season'        : int(seasonNumber),
                        'Year'          : epiYear,
                        'Duration'      : epiDuration,
                        'Deeplinks'     : {
                            'Web': linkEpi,
                            'Android': None,
                            'iOS': None
                        },
                        'Synopsis'      : epiDescription,
                        'Rating'        : epiRating,
                        'Provider'      : None,
                        'Genres'        : None,
                        'Cast'          : None,
                        'Directors'     : None,
                        'Availability'  : expiration,
                        'Download'      : None,
                        'IsOriginal'    : None,
                        'IsAdult'       : None,
                        'Country'       : None,
                        'Packages'      : packages,
                        'Timestamp'     : datetime.now().isoformat(),
                        'CreatedAt'     : self._created_at
                    }
                    Datamanager._checkDBandAppend(self,payloadEpi,listDBEpi,listPayloadEpi,isEpi=True)

            if packages == []:
                packages = [
                    {
                        'Type'  : 'free-vod',
                    }
                ]

            payload = {
                'PlatformCode'      : self._platform_code,
                'Id'                : contenidoId,
                'Type'              : 'serie',
                'Title'             : title,
                'CleanTitle'        : _replace(title),
                'OriginalTitle'     : None,
                'Year'              : year,
                'Duration'          : None,
                'Deeplinks'         : {
                    'Web': linkSerie,
                    'Android': None,
                    'iOS': None
                },
                'Synopsis'          : desc,
                'Rating'            : rating,
                'Provider'          : None,
                'Genres'            : None,
                'Cast'              : None,
                'Directors'         : None,
                'Availability'      : None,
                'Download'          : None,
                'IsOriginal'        : None,
                'IsAdult'           : None,
                'Packages'          : packages,
                'Country'           : None,
                'Timestamp'         : datetime.now().isoformat(),
                'CreatedAt'         : self._created_at
            }
            Datamanager._checkDBandAppend(self,payload,listDBSerie,listPayloadSerie)
                        
        Datamanager._insertIntoDB(self,listPayloadSerie,self.titanScraping)
        Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios)
        
        '''
        Upload
        '''
        self.sesion.close()
        Upload(self._platform_code, self._created_at, testing=True)                
                
        