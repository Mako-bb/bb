#   @autor = tatsorr
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

class AcornTV_Test():
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
        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        listDBEpi = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        listPayload = []
        listPayloadEpi = []
        
        packages = [
            {
                'Type': 'subscription-vod'
            }
        ]

        payload = "action=browse_order_filter&active_section=all&order_by=a-z&filter_by=all&token=a539dc7762"
        headers = {
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }

        URL = 'https://acorn.tv/wp-admin/admin-ajax.php'
        json = Datamanager._getJSON(self,URL,usePOST=True,data=payload,headers=headers)

        soup = BeautifulSoup(json['data']['html'], features="html.parser")

        for item in soup.findAll('div',{'class':'col-sm-6 col-md-6 col-lg-3'}):
            
            title = item.find('p',{'class':'franchise-title'}).text.replace('(SCANDINAVIAN VERSION)','')

            deeplink = item.find('a',{'itemprop':'url'}).get('href')

            imagen = item.find('img',{'class':'wp-post-image'}).get('src')

            if title == 'What is Acorn TV?' or title == "MCLEOD'S DAUGHTERS SERIES 3 - US, CANADA, LATIN AMERICA AND UNITED KINGDOM - JULY 27TH":
                continue
            
            print(title)
            soup = Datamanager._getSoup(self,deeplink)
            
            subnav = soup.find('h4',{'class':'subnav2'}).text

            desc = soup.find('p',{'id':'franchise-description'}).text

            if subnav == 'Movie' or subnav == 'Feature':
                tipo = 'movie'
            else:
                tipo = 'serie'

            payload = {
                'PlatformCode'      : self._platform_code,
                'Id'                : hashlib.md5(deeplink.encode('utf-8')).hexdigest() ,
                'Type'              : tipo,
                'Title'             : title,
                'CleanTitle'        : _replace(title),
                'OriginalTitle'     : None,
                'Year'              : None,
                'Duration'          : None,
                'Deeplinks'         : {
                                    'Web': deeplink,
                                    'Android': None,
                                    'iOS': None
                },
                'Synopsis'          : desc,
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

        Datamanager._insertIntoDB(self,listPayload,self.titanScraping)

        if tipo == "serie":
            for seasons in soup.find_all("span",{"itemprop":"constainsSeason"}):
                season_num = seasons.find("meta",{"itemprop":"seasonNumber"}).get("content")
                
                for episode in seasons.find_all("span", {"itemprop":"episode"}):
                    link_epi = episode.find("a",{"itemprop":"url"}).get("href")
                    title_epi = episode.find("h5",{"itemprop":"name"}).text
                    epi_num = episode.find("span",{"itemprop":"episodeNumber"}).text
                


                    payloadEpi = {
                        'PlatformCode'  : self._platform_code,
                        'ParentId'      : hashlib.md5(deeplink.encode('utf-8')).hexdigest(),
                        'ParentTitle'   : title,
                        'Id'            : hashlib.md5(linkEpi.encode('utf-8')).hexdigest(),
                        'Title'         : title_epi,
                        #'SeasonName'    : seasonName,
                        'Episode'       : int(epi_num),
                        'Season'        : int(season_num),
                        'Year'          : None,
                        'Duration'      : None,
                        'Deeplinks'     : {
                            'Web': link_epi,
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
