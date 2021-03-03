# -*- coding: utf-8 -*-
import time
import requests
import hashlib
import json
from common                  import config
from datetime                import datetime
from handle.mongo            import mongo
from updates.upload          import Upload
from slugify                 import slugify
from handle.replace         import _replace

class DasersteMediathek():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.test                   = False
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        if type == 'scraping':
            delete = self.mongo.delete(self.titanPreScraping, {'PlatformCode': self._platform_code})
            print('Eliminados {} items PreScraping'.format(delete))
            self._preScraping()

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

    def _preScraping(self):
        listPre = []

        num = 0
        while True:
            URLStart = 'https://api.ardmediathek.de/public-gateway?variables=%7B%22client%22%3A%22ard%22%2C%22compilationId%22%3A%227QcfHyBWGAomKAYcSEYCe%22%2C%22pageNumber%22%3A{num}%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%220aa6f77b1d2400b94b9f92e6dbd0fabf652903ecf7c9e74d1367458d079f0810%22%7D%7D'.format(num = num)
            r  = self.sesion.get(URLStart)
            d = r.json()
            if not d['data']['morePage']['widget']['teasers']:
                break
            num +=1
            for item in d['data']['morePage']['widget']['teasers']:
                _id             = item['links']['target']['id']
                _type           = 'movie'
                _duration       = item['duration'] // 60
                _images         = item['images']['aspect16x9']['src']
                _availability   = item['availableTo'].split('T')[0].strip()
                _deeplink       = 'https://www.ardmediathek.de/ard/player/{}'.format(_id)
                _apiDeeplink    = 'https://api.ardmediathek.de/public-gateway?variables=%7B%22client%22%3A%22ard%22%2C%22clipId%22%3A%22{id}%22%2C%22deviceType%22%3A%22pc%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22ddd2b90858eda4352ec87f12e91dd7b4297645ca288680679c32f7ebbd258610%22%7D%7D'.format(id = _id)
                pay = {
                    'PlatformCode' : self._platform_code,
                    'Id'            : _id,
                    'Type'          : _type,
                    'Duration'      : _duration,
                    'Availability'  : _availability,
                    'Images'        : _images,
                    'Deeplink'      : _deeplink,
                    'ApiDeeplink'   : _apiDeeplink,
                }
                listPre.append(pay)
                print(len(listPre))

        if listPre:
            self.mongo.insertMany(self.titanPreScraping, listPre)
            print("(!) {} Prescraped Titles Inserted ".format(len(listPre)))
        self._scraping()

    def _query_field(self, collection, field=None, extra_filter=None):
        find_filter = {'PlatformCode': self._platform_code, 'CreatedAt': self._created_at}

        if extra_filter:
            find_filter.update(extra_filter)

        find_projection = {'_id': 0, field: 1,} if field else None

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection=find_projection,
            no_cursor_timeout=False
        )

        if field:
            query = [item[field] for item in query]
        else:
            query = list(query)

        return query


    def _scraping(self):
        preScraping = self.mongo.search(self.titanPreScraping, {'PlatformCode': self._platform_code})
        listMovie = []

        listContent = self._query_field(self.titanScraping, field='Id')
        print('En titanScraping: {}'.format(len(listContent)))

        for item in preScraping:
            URLStart = item['ApiDeeplink']
            response = self.sesion.get(URLStart)
            data = response.json()
            movie = data['data']['playerPage']

            _id     = str(item['Id'])
            _title  = movie['title']

            if _id in listContent:
                print('Existe {}'.format(_id))
                continue
            else:
                listContent.append(_id)
                print('insertando', _title)

            _type           = item['Type']
            _duration       = item['Duration']
            _deeplink       = item['Deeplink']
            _synopsis       = movie['synopsis']
            _provider       = [movie['publicationService']['name']]
            _availability   = item['Availability']
            _images         = [item['Images']]
            _rating         = movie['maturityContentRating']
            if _rating == 'NONE':
                _rating = None

            packages = [
                {
                    'Type' : 'free-vod'
                }
            ]
            payload     = {
                'PlatformCode'      : self._platform_code,
                'Id'                : _id,
                'Type'              : _type,
                'Title'             : _title,
                'CleanTitle'        : _replace(movie['title']),
                'OriginalTitle'     : None,
                'Year'              : None,
                'Duration'          : _duration,
                'Deeplinks'         :  {
                    'Web'     : _deeplink,
                    'Android' : None,
                    'iOS'     : None
                },
                'Synopsis'          : _synopsis,
                'Rating'            : _rating,
                'Provider'          : _provider,
                'Genres'            : None,
                'Cast'              : None,
                'Directors'         : None,
                'Availability'      : _availability,
                'Download'          : None,
                'IsOriginal'        : None,
                'IsAdult'           : None,
                'Image'             : _images,
                'Country'           : None,
                'Packages'          : packages,
                'Timestamp'         : datetime.now().isoformat(),
                'CreatedAt'         : self._created_at
            }
            listMovie.append(payload)
        if listMovie:
            self.mongo.insertMany(self.titanScraping, listMovie)
            print('Insertados: {}'.format(len(listMovie)))   

        self.sesion.close()
        '''
        Upload
        '''
        if not self.test:
            Upload(self._platform_code, self._created_at, False, has_episodes = False)