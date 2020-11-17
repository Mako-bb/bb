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

class AcornTV():
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
        URL = "https://acorn.tv/browse/all/"
        soup = Datamanager._getSoup(self,URL)

        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        listPayload = []
        listPayloadEpi = []
        
        for rows in soup.findAll('div', {'class': 'row'}):
            
            for item in rows.findAll('div',{'class':'col-sm-6 col-md-6 col-lg-3'}):
                title = item.find('p', {'class':'franchise-title'}).text
                clean_title = _replace(title)
                link = str(item.find('a').get('href'))
                
                if 'Coming Soon' in title:
                    continue
                
                if link == 'https://acorn.tv/whatisacorntv/':
                    continue
                
                if 'Version' in title:
                    parentesis_i = title.find('(')
                    if parentesis_i != -1:
                        clean_title = title.replace(title[parentesis_i:], '')
                        clean_title = _replace(clean_title)         
                
                soupMovieSerie = Datamanager._getSoup(self,link)
                
                descripcion = soupMovieSerie.find('p',{'id':'franchise-description'}).text
                getTipoContenido = soupMovieSerie.find('div',{'class':'franchise-eps-bg'})
                
                if getTipoContenido:
                    getTipoContenido = str(getTipoContenido.find('h6').text).replace(" ","")
                else:
                    continue

                if getTipoContenido == "Feature" or getTipoContenido == "Movie":
                    contenidoTipo = "movie"
                else:
                    contenidoTipo = "serie"
                
                contenidoId = hashlib.md5(link.encode('utf-8')).hexdigest() 
                        
                packages = [
                    {
                        'Type'  : 'subscription-vod',
                    }
                ]
                
                payload = {
                    'PlatformCode'      : self._platform_code,
                    'Id'                : contenidoId,
                    'Type'              : contenidoTipo,
                    'Title'             : title,
                    'CleanTitle'        : clean_title,
                    'OriginalTitle'     : None,
                    'Year'              : None,
                    'Duration'          : None,
                    'Deeplinks'         : {
                                        'Web': link,
                                        'Android': None,
                                        'iOS': None
                    },
                    'Synopsis'          : descripcion,
                    'Rating'            : None,
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
                Datamanager._checkDBandAppend(self,payload,listDBMovie,listPayload)
                
                if contenidoTipo == "serie":
                    
                    for season in soupMovieSerie.findAll('span',{'itemprop':'containsSeason'}):
                        nroSeason = season.find('meta',{'itemprop':'seasonNumber'}).get('content')
                        seasonName = str(season.find('h4',{'class':'subnav2'}).text)
                        for epi in season.findAll('div', {'class':'col-sm-6 col-md-3'}):
                            linkEpi = epi.find('a',{'itemprop':'url'}).get('href')
                            if epi.find('span',{'itemprop':'episodeNumber'}) != None:
                                nroEpi = epi.find('span',{'itemprop':'episodeNumber'}).text
                            else:
                                nroEpi = 1
                            nombreEpi = str(epi.find('h5',{'itemprop':'name'}).text)
                            idEpi = hashlib.md5(linkEpi.encode('utf-8')).hexdigest()
                            
                            payloadEpi = {
                                'PlatformCode'  : self._platform_code,
                                'ParentId'      : contenidoId,
                                'ParentTitle'   : title,
                                'Id'            : idEpi,
                                'Title'         : nombreEpi,
                                #'SeasonName'    : seasonName,
                                'Episode'       : int(nroEpi),
                                'Season'        : int(nroSeason),
                                'Year'          : None,
                                'Duration'      : None,
                                'Deeplinks'     : {
                                    'Web': linkEpi,
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
                            Datamanager._checkDBandAppend(self,payloadEpi,listDBEpi,listPayloadEpi,isEpi=True)
                                
        Datamanager._insertIntoDB(self,listPayload,self.titanScraping)
        Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios)  
        '''
        Upload
        '''
        self.sesion.close()
        if not testing:
            Upload(self._platform_code, self._created_at, False)               
                
        