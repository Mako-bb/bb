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

'''

    La pagina usa una api, trae de a 20 contenidos por request
    Tiene en total alrededor de 1300 (fecha 2021-03-15)

'''

class OptimumTest():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)
            
        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''

            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
                    
            self._scraping(testing = True)

    def __query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}

        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at,
        }

        find_filter.update(extra_filter)

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )

        query = {item[field] for item in query}

        return query
                    
    def _scraping(self, testing = False):

        payloads = []

        scraped_ids = self.__query_field('titanScraping', 'Id')

        lista_ordenes = [
            '48265001',
            '48266001',
            '48270001',
            '48268001',
            '48269001',
            '48267001'

        ]

        for orden in lista_ordenes:

            offset = 0

            while True:

                req = self.sesion.get('https://www.optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination/{}/20/{}?sort=1&filter=0'.format(orden, offset))
                print(req.status_code, req.url)

                data = req.json()

                titulos = data['data']['result']['titles']

                for titulo in titulos:

                    title = titulo['title']        

                    year = titulo['release_year']
                    
                    if titulo.get('asset_id'):
                        id_ = titulo['asset_id']
                    else:
                        id_ = titulo['sd_asset']

                    tipo = 'movie' # movie o serie

                    price = titulo['price']

                    packages = [
                        {
                            'Type': 'transaction-vod',
                            'RentPrice': price,
                        }
                    ]

                    if titulo.get('actors'):
                        cast = titulo['actors']

                        actores = cast.split(', ')
                    else:
                        actores = None

                    deeplink = 'https://www.optimum.net/tv/asset/#/movie/{}'.format(id_)

                    payload = {
                        'PlatformCode':  self._platform_code, # Obligatorio
                        'Id':            str(id_), # Obligatorio
                        'Title':         title, # Obligatorio
                        'OriginalTitle': None,
                        'CleanTitle':    _replace(title), # Obligatorio
                        'Type':          tipo, # Obligatorio
                        'Year':          year, # Prioridad
                        # 'Duration':      runtime, # if type serie: duration None
                        'Deeplinks': {
                            'Web':       deeplink, # Obligatorio
                            'Android':   None,
                            'iOS':       None,
                            },
                        'Playback':      None,
                        # 'Synopsis':      sinopsis,
                        'Image':         None,
                        'Rating':        None,
                        'Provider':      None,
                        'Genres':        None,
                        'Cast':          actores,
                        # 'Directors':     directors, # Prioridad
                        'Availability':  None,
                        'Download':      None,
                        'IsOriginal':    None,
                        'IsAdult':       None,
                        'Packages':      packages,
                        'Country':       None,
                        'Timestamp':     datetime.now().isoformat(),
                        'CreatedAt':     self._created_at
                    }

                    if payload['Id'] not in scraped_ids:
                        payloads.append(payload)
                        scraped_ids.add(payload['Id'])
                        print("Insertado contenido {}".format(payload['Title']))
                    else:
                        print("Ya existe contenido {}".format(payload['Title']))


                    # print(payload)

                if data['data']['result']['next'] != '0':
                    offset += 20
                else:
                    break

        print("Terminado, insertados {} contenidos".format(len(payloads)))

        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
            print('Insertados {} en {}'.format(len(payloads), self.titanScraping))

        # if payloads_episodes:
        #     self.mongo.insertMany(self.titanScrapingEpisodes, payloads_episodes)
        #     print('Insertados {} en {}'.format(len(payloads_episodes), self.titanScrapingEpisodes))

        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing = testing)





        