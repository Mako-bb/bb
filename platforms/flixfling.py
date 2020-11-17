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

class FlixFling():
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
        listaCategorias = [
            "hotflix",
            "subscriber%20picks",
            "animated",
            "classic",
            "concerts",
            "drama",
            "comedy",
            "fitness",
            "action%20&%20adventure",
            "holiday",
            "horror",
            "lgbt",
            "music",
            "musical",
            "mystery",
            "romance",
            "scifi",
            "special%20interests",
            "sports",
            "thriller",
            "western"
        ]
        
        listPayload = []
        listDBMovie = Datamanager._getListDB(self,self.titanScraping)
        for x in range(0,len(listaCategorias)):
            soup = Datamanager._getSoup(self,"https://www.flixfling.com/browse/{}".format(listaCategorias[x]))
            
            for item in soup.findAll('div',{'class':'movie_item col-sm-2 col-xs-4'}):
                listDirector = []
                listCast = []
                listGenre = []
                
                link = item.find('a').get('href')
                contenidoId = str(link).split("/")[2]
                link = "https://www.flixfling.com{}".format(link)
                soupMovie = Datamanager._getSoup(self,link)


                if soupMovie.find('span',{'class':'btn_premium_button'}):
                    buyPrice = None
                    rentPrice = None
                    
                    rentButton = soupMovie.find('span',{'class':'rentBtnPrice'})
                    if rentButton:
                        rentPrice = float(rentButton.text.strip().replace('FROM','').replace(' ','').replace('$',''))

                    buyButton = soupMovie.find('span',{'class':'buyBtnPrice'})
                    if buyButton:
                        buyPrice = float(buyButton.text.strip().replace('FROM','').replace(' ','').replace('$',''))

                    packages = [
                        {
                            'Type' : 'transaction-vod',
                            'RentPrice': rentPrice,
                            'BuyPrice': buyPrice,
                            'Currency': 'USD'
                        }
                    ]
                    
                    if not buyPrice and not rentPrice:
                        continue 

                else:
                    packages = [
                        {
                            'Type' : 'subscription-vod'
                        }
                    ]
                
                print(packages)

                try:
                    title = soupMovie.find('a',{'itemprop':'url'}).find('span').text
                except:
                    continue
               
                
                
                try:
                    duracion = int(int(soupMovie.find('meta',{'property':'video:duration'}).get('content'))/60)
                except:
                    duracion = None
                
                try:
                    anio = int(str(soupMovie.find('span',{'itemprop':'releasedEvent'}).text)[0:4])
                except:
                    anio = None
                
                try:
                    int(duracion)
                except:
                    duracion = None
                               
                descripcion = str(soupMovie.find('meta',{'name':'twitter:description'}).get('content'))
                
                for x in soupMovie.findAll('a',{'itemprop':'director'}):
                    listDirector.append(x.text)
                
                for x in soupMovie.findAll('a',{'itemprop':'actor'}):
                    listCast.append(x.text)
                    
                for x in soupMovie.findAll('span',{'itemprop':'genre'}):
                    listGenre.append(x.text)
                    
                imageLink = soupMovie.find('img',{'itemprop':'image'}).get('src')
                images = []
                images.append(imageLink)
                
                payload = {
                'PlatformCode'      : self._platform_code,
                'Id'                : contenidoId,
                'Type'              : 'movie',
                'Title'             : title,
                'CleanTitle'        : _replace(title),
                'OriginalTitle'     : None,
                'Year'              : anio,
                'Duration'          : duracion,
                'Deeplinks'         :  {
                    'Web'     : link,
                    'Android' : None,
                    'iOS'     : None
                },
                'Synopsis'          : descripcion,
                'Rating'            : None,
                'Provider'          : None,
                'Genres'            : listGenre,
                'Cast'              : listCast,
                'Directors'         : listDirector,
                'Availability'      : None,
                'Download'          : None,
                'IsOriginal'        : None,
                'IsAdult'           : None,
                'Image'             : images,
                'Country'           : None,
                'Packages'          : packages,
                'Timestamp'         : datetime.now().isoformat(),
                'CreatedAt'         : self._created_at
                }
                
                Datamanager._checkDBandAppend(self,payload,listDBMovie,listPayload)
            
        Datamanager._insertIntoDB(self,listPayload, self.titanScraping)
        '''
        Upload
        '''
        self.sesion.close()
        Upload(self._platform_code, self._created_at, False, has_episodes=False)    