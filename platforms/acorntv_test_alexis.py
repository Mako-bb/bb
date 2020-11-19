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
import re

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

        payload = "action=browse_order_filter&active_section=all&order_by=a-z&filter_by=all&token=a23f19d2f0"
        headers = {
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        URL = 'https://acorn.tv/wp-admin/admin-ajax.php'
        json = Datamanager._getJSON(self, URL, usePOST=True, data=payload, headers=headers)
        soup = BeautifulSoup(json['data']['html'], features="html.parser")

        for item in soup.findAll('div',{'class':'col-sm-6 col-md-6 col-lg-3'}):
            title = item.find('p',{'class':'franchise-title'}).text.replace('(SCANDINAVIAN VERSION)','')
            print(title)
            deeplink = item.find('a',{'itemprop':'url'}).get('href')
            imagen = item.find('img',{'class':'wp-post-image'}).get('src')

            titulosOmitidos = ["mcleod's daughters series 3 - us, canada, latin america and united kingdom - july 27th",
                               "ms. fisher's modern muder mysteries - coming soon", "what is acorn tv",
                               "the good karma hospital series 1 and 2 - latin america - july 27"]

            soup = Datamanager._getSoup(self, deeplink)
            subnav = soup.find('h4', {'class': 'subnav2'}).text
            desc = soup.find('p', {'id': 'franchise-description'}).text

            if subnav == 'Movie' or subnav == 'Feature':
                tipo = 'movie'
            else:
                tipo = 'serie'

            if title.lower() not in titulosOmitidos:
                payload = {
                    'PlatformCode'      : self._platform_code,
                    'Id'                : hashlib.md5(deeplink.encode('utf-8')).hexdigest() ,
                    'Type'              : tipo,
                    'Title'             : title,
                    'CleanTitle'        : self.elimina_parentesis(_replace(title)),
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
            else:
                continue

            if tipo == "serie":
                for seasons in soup.findAll('span', {'itemprop': 'containsseason'}):
                    season_num = seasons.find("meta", {"itemprop": "seasonNumber"}).get("content")
                    for episode in seasons.find_all("span", {"itemprop": "episode"}):
                        url_episode = episode.find("a", {"itemprop": "url"}).get("href")
                        titulo_episodio = episode.find("h5", {"itemprop": "name"}).text
                        episode_num = episode.find("span", {"itemprop": "episodeNumber"}).text
                        id_episode = hashlib.md5(url_episode.encode('utf-8')).hexdigest()
                        payloadEpi = {
                            'PlatformCode': self._platform_code,
                            'ParentId': hashlib.md5(deeplink.encode('utf-8')).hexdigest(),
                            'ParentTitle': title,
                            'Id':id_episode,
                            'Title': titulo_episodio,
                            # 'SeasonName'    : seasonName,
                            'Episode': int(episode_num),
                            'Season': int(season_num),
                            'Year': None,
                            'Duration': None,
                            'Deeplinks': {
                                'Web': url_episode,
                                'Android': None,
                                'iOS': None
                            },
                            'Synopsis': None,
                            'Rating': None,
                            'Provider': None,
                            'Genres': None,
                            'Cast': None,
                            'Directors': None,
                            'Availability': None,
                            'Download': None,
                            'IsOriginal': None,
                            'IsAdult': None,
                            'Country': None,
                            'Packages': packages,
                            'Timestamp': datetime.now().isoformat(),
                            'CreatedAt': self._created_at
                        }
                        Datamanager._checkDBandAppend(self, payloadEpi, listDBEpi, listPayloadEpi, isEpi=True)

        Datamanager._insertIntoDB(self, listPayload, self.titanScraping)
        Datamanager._insertIntoDB(self, listPayloadEpi, self.titanScrapingEpisodios)

    @staticmethod
    def elimina_parentesis(self, titulo):
        if re.search(r"\(", titulo):
            return titulo.split('(')[0]
        else:
            return titulo
