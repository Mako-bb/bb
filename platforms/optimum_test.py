# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from slugify                import slugify
from handle.datamanager     import Datamanager
from updates.upload         import Upload

class OptimumTest():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']

        self.currentSession = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

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
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
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

        ids_guardados = self.__query_field('titanScraping', 'Id')

        categorias = [
            '48265008',
            '48266008',
            '48270008',
        ]

        for cat in categorias:

            offset = 0

            while True:

                request = self.currentSession.get('https://www.optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination/{}/20/{}?sort=1&filter=0'.format(cat, offset))
                print(request.status_code, request.url)

                data = request.json()

                titulos = data['data']['result']['titles']

                for titulo in titulos:

                    title = titulo['title']

                    year = 900

                    if titulo.get('actors'):
                        actors = titulo['actors'].split(', ')
                    else:
                        actors = None

                    packages = [
                        {
                            'Type': 'transaction-vod',
                            'RentPrice': titulo['price'],
                        }
                    ]

                    id_ = str(titulo['title_id'])

                    # id_ = hashlib.md5(title.encode('utf-8')).hexdigest()

                    payload = {
                        'PlatformCode':  self._platform_code,
                        'Id':            id_,
                        'Title':         title,
                        'OriginalTitle': None,
                        'CleanTitle':    _replace(title),
                        'Type':          'movie',
                        'Year':          year,
                        'Duration':      None,
                        'Deeplinks': {
                            'Web':       'https://www.optimum.net/tv/asset/#/movie/{}'.format(titulo['asset_id']),
                            'Android':   None,
                            'iOS':       None,
                        },
                        'Playback':      None,
                        'Synopsis':      None,
                        'Image':         None,
                        'Rating':        None,
                        'Provider':      None,
                        'Genres':        None,
                        'Cast':          actors,
                        'Directors':     None,
                        'Availability':  None,
                        'Download':      None,
                        'IsOriginal':    None,
                        'IsAdult':       None,
                        'Packages':      packages,
                        'Country':       None,
                        'Timestamp':     datetime.now().isoformat(),
                        'CreatedAt':     self._created_at
                    }

                    # print(payload)

                    if payload['Id'] not in ids_guardados:
                        payloads.append(payload)
                        ids_guardados.add(payload['Id'])
                        print('Insertado titulo {}'.format(payload['Title']))
                    else:
                        print('Id ya guardado {}'.format(payload['Id']))

                if data['data']['result']['next'] == '0':
                    break
                    
                offset += 20

        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
            print('Insertados {} en {}'.format(len(payloads), self.titanScraping))

        self.currentSession.close()

        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)