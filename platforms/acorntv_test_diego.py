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
from handle.datamanager     import Datamanager
from handle.replace         import _replace

class AcornTV_Test_Diego():
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

        

        URL = 'https://acorn.tv/'
        soup1 = Datamanager._getSoup(self,URL)
        get_token = soup1.find('script',{'id':'rlje-carousel-pagination-js-js-extra'}).text
        token = get_token.split('token":"')[1].split('\"};')[0]

        payload = "action=browse_order_filter&active_section=all&order_by=a-z&filter_by=all&token="+token
        headers = {
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        URL2 = 'https://acorn.tv/wp-admin/admin-ajax.php'
        json_data = Datamanager._getJSON(self,URL2,usePOST=True,data=payload,headers=headers)

        soup = BeautifulSoup(json_data['data']['html'], features="html.parser")
        
        for item in soup.findAll('div',{'class':'col-sm-6 col-md-6 col-lg-3'}):
            
            title = item.find('p',{'class':'franchise-title'}).text.replace('(SCANDINAVIAN VERSION)','')

            deeplink = item.find('a',{'itemprop':'url'}).get('href')

            imagen = item.find('img',{'class':'wp-post-image'}).get('src')

            if title == 'What is Acorn TV?' or title == "MCLEOD'S DAUGHTERS SERIES 3 - US, CANADA, LATIN AMERICA AND UNITED KINGDOM - JULY 27TH":
                continue
            
            soup2 = Datamanager._getSoup(self,deeplink)
            context = soup2.find('script',{'type':'application/ld+json'}).text
            contextjson = json.loads(context)
            typeOf = contextjson['@type']
            actors = contextjson['actor']
            director = contextjson['director']
            desc = soup2.find('p',{'id':'franchise-description'}).text

            if typeOf == "Movie":
                tipo = 'movie'
            else:
                tipo = 'serie'

            if actors != []:
                actor_list = []
                for actor in actors:
                    actor_list.append(actor['name'])
                actors = actor_list

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
                'Cast'              : actors,
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