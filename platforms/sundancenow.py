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
from handle.replace     import _replace

class SundanceNow():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self.test                   = False
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()     
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
        listMovie   = []
        listSeries  = []

        listCantidadDeEPI = []

        headers = {
            'Accept' : 'application/json'

        }

        listGeneros = [
        'films/suspense/Suspense',
        'films/comedy/Comedy',
        'films/foreign/Foreign',
        'films/drama/Drama',
        'films/documentaries/Documentaries',
        'films/history-politics/History%20&%20Politics',
        'films/action/Action',
        'films/classic/Classic',
        'films/romance/Romance',
        'films/thriller/Thriller',
        'films/biography/Biography',
        'films/all-movies/All%20Movies',
        ] 

        for l in listGeneros:
            URLStrart = 'https://www.sundancenow.com/{}'.format(l)
            print(URLStrart)
            response = self.sesion.get(URLStrart, headers = headers)
            data = response.json()

            if response.status_code == 404:
                continue
            
            _genre = data['activeGenre']['id']
            for item in data['movies']:
                _id         = item['id']
                _type       = item['videoType']
                _title      = item['title']
                _year       = item.get('year')
                _duration   = item.get('duration', {}).get('minutes')
                _legacyId   = item.get('legacyId')

                if _legacyId:
                    _nameDeeplink = _title.replace('&', '-')
                    _nameDeeplink = _nameDeeplink.replace(' ', '-')
                    _deeplink = 'https://www.sundancenow.com/films/watch/{name}/{legacy}'.format(name = _nameDeeplink, legacy = _legacyId)
                _synopsis   = item.get('description',{}).get('long')
                _rating     = item.get('rating')
                _genres     = _genre
                
                listCast    = []
                _cast       = item['castMembers']['display']
                _cast       = _cast.split(',')
                for c in _cast:
                    listCast.append(c.strip())
                
                listDirector = []
                _director    = item['director']
                _director    = _director.split(',')
                for d in _director:
                    listDirector.append(d.strip())
                
                listImage   = []
                _imageM     = item.get('images', {}).get('masthead')
                _imageT     = item.get('images', {}).get('thumbnail')
                _imageB     = item.get('images', {}).get('boxArt')
                
                listImage.append(_imageM)
                listImage.append(_imageT)
                listImage.append(_imageB)

                _country    = item.get('origin')

                packages    = [
                    {
                        'Type' : 'subscription-vod'
                    }
                ]

                payload     = {
                    'PlatformCode'      : self._platform_code,
                    'Id'                : _id,
                    'Type'              : _type,
                    'Title'             : _title,
                    'CleanTitle'        : _replace(_title),
                    'OriginalTitle'     : None,
                    'Year'              : _year,
                    'Duration'          : _duration,
                    'Deeplinks'         :  {
                        'Web'     : _deeplink,
                        'Android' : None,
                        'iOS'     : None
                    },
                    'Synopsis'          : _synopsis,
                    'Rating'            : _rating,
                    'Provider'          : None,
                    'Genres'            : [_genre],
                    'Cast'              : listCast,
                    'Directors'         : listDirector,
                    'Availability'      : None,
                    'Download'          : None,
                    'IsOriginal'        : None,
                    'IsAdult'           : None,
                    'Image'             : listImage,
                    'Country'           : [_country],
                    'Packages'          : packages,
                    'Timestamp'         : datetime.now().isoformat(),
                    'CreatedAt'         : self._created_at
                }
                if not any(d['Id'] == _id for d in listMovie):
                    print(payload['Id'], payload['Title'])
                    listMovie.append(payload)

        if listMovie:
            self.mongo.insertMany(self.titanScraping, listMovie)
        print('Insertados: {}'.format(len(listMovie)))
        
        #SERIES
        URLStrartSerie = 'https://www.sundancenow.com/series'
        print(URLStrartSerie)

        response = self.sesion.get(URLStrartSerie, headers = headers)
        data = response.json()
        
        for serie in data:
            listEpisode = []
            _id         = serie['id']
            _type       = 'serie'
            _title      = serie['title']
            _year       = serie.get('year')
            _duration   = serie.get('duration',{}).get('minutes')
            _legacy     = serie.get('legacyId')
            if _legacy:
                _nameDeeplink = _title.replace('&', '-')
                _nameDeeplink = _title.replace(' ','-')
                _deeplink = 'https://www.sundancenow.com/series/watch/{name}/{legacy}'.format(name = _nameDeeplink, legacy = _legacy)
            
            _synopsis   = serie.get('description',{}).get('long')
            _rating     = serie.get('rating')

            listDirector = []
            for d in serie['credits']:
                if d['role'] == 'Director':
                    _director = d['name']
                    listDirector.append(_director)


            listImage = []
            _imageM   = serie.get('images', {}).get('masthead')
            _imageT   = serie.get('images', {}).get('thumbnail')
            _imageB   = serie.get('images', {}).get('boxArt')      

            listImage.append(_imageM)
            listImage.append(_imageT)
            listImage.append(_imageB)   
            
            packages  = [
                {
                    'Type' : 'subscription-vod'
                }
            ]

            payload   = {
                'PlatformCode'      : self._platform_code,
                'Id'                : _id,
                'Type'              : _type,
                'Title'             : _title,
                'CleanTitle'        : _replace(_title),
                'OriginalTitle'     : None,
                'Year'              : _year,
                'Duration'          : _duration,
                'Deeplinks'         :  {
                    'Web'     : _deeplink,
                    'Android' : None,
                    'iOS'     : None
                },
                'Synopsis'          : _synopsis,
                'Rating'            : _rating,
                'Provider'          : None,
                'Genres'            : None,
                'Cast'              : None,
                'Directors'         : listDirector,
                'Availability'      : None,
                'Download'          : None,
                'IsOriginal'        : None,
                'IsAdult'           : None,
                'Image'             : listImage,
                'Country'           : None,
                'Packages'          : packages,
                'Timestamp'         : datetime.now().isoformat(),
                'CreatedAt'         : self._created_at
            }
            if not any(d['Id'] == _id for d in listSeries):
                print(payload['Id'], payload['Title'])
                listSeries.append(payload)



            for episode in serie['seasons']:
                for epi in episode['episodes']:
                    _episodeId  = epi['id']

                    _title      = epi['title']
                    if _title.find(' Ep. ') != -1:
                        _title      = _title
                        listCantidadDeEPI.append(_title)
                    elif _title.find('. ') != -1:
                        _title      = _title.split('. ')[-1].strip()
                        listCantidadDeEPI.append(_title)
                    else:
                        _title      = _title
                        listCantidadDeEPI.append(_title)

                    _episode    = epi.get('trackset', {}).get('Episode')
                    _season     = epi.get('trackset', {}).get('Season')
                    _year       = epi.get('year')
                    if _year >= 2029:
                        _year = None
                    else:
                        _year = _year
                    _duration   = epi.get('duration',{}).get('minutes')
                    _synopsis   = epi.get('description', {}).get('long')
                    _rating     = epi.get('rating')
                    _legacyId   = epi.get('legacyId')

                    if _legacyId:
                        _nameDeeplink = _title.replace('&', '-')
                        _nameDeeplink = _nameDeeplink.replace(' ', '-')
                        _deeplink = 'https://www.sundancenow.com/films/watch/{name}/{legacy}'.format(name = _nameDeeplink, legacy = _legacyId)

                    listCast    = []
                    _cast       = epi.get('castMembers', {}).get('display')
                    _cast       = _cast.split(',')
                    for c in _cast:
                        listCast.append(c.strip())

                    listDirector = []

                    _director = epi.get('director')
                    listDirector.append(_director)

                    # listDirector = []
                    # for d in epi['credits']:
                    #     if d['role'] == 'Director':
                    #         _director = d['name']
                    #         listDirector.append(_director)


                    listImage = []
                    _imageM   = epi.get('images', {}).get('masthead')
                    _imageT   = epi.get('images', {}).get('thumbnail')
                    _imageB   = epi.get('images', {}).get('boxArt')      

                    listImage.append(_imageM)
                    listImage.append(_imageT)
                    listImage.append(_imageB)   
                    
                    try:
                        _country  = [epi['origin']]
                    except:
                        _country = None
                    packages  = [
                        {
                            'Type' : 'subscription-vod'
                        }
                    ] 

                    _parentTitle = epi['trackset']['Series Title']

                    payload = {
                        "PlatformCode"  : self._platform_code,
                        "ParentId"      : _id,
                        "ParentTitle"   : _parentTitle,
                        "Id"            : _episodeId,
                        "Title"         : _title,
                        "Episode"       : _episode,
                        "Season"        : _season, 
                        'Year'          : _year,
                        'Duration'      : _duration,
                        'Deeplinks'     : {
                                'Web'    : _deeplink,
                                'Android': None,
                                'iOS'    : None
                        },
                        'Synopsis'      : _synopsis,
                        'Rating'        : _rating,
                        'Provider'      : None,
                        'Genres'        : None,
                        'Cast'          : listCast,
                        'Directors'     : listDirector,
                        'Availability'  : None,
                        'Download'      : None,
                        'IsOriginal'    : None,
                        'IsAdult'       : None,
                        'Image'         : listImage,
                        'Country'       : _country,
                        'Packages'      : packages,
                        "Timestamp"     : datetime.now().isoformat(),
                        'CreatedAt'     : self._created_at
                    }
                    listEpisode.append(payload)
                    print(payload['Id'], payload['Title'])

                #     if len(listEpisode) > 99:
                #         self.mongo.insertMany(self.titanScrapingEpisodes, listEpisode)
                #         print("!{} Titles Inserted ".format(len(listEpisode)))
                
                # if len(listSeries) > 99:
                #     self.mongo.insertMany(self.titanScraping, listSeries)
                #     print("!{} Titles Inserted ".format(len(listSeries)))



            if listEpisode:
                self.mongo.insertMany(self.titanScrapingEpisodes, listEpisode)
            print('Insertados: {} episodios'.format(len(listEpisode)))  

        if listSeries:
            self.mongo.insertMany(self.titanScraping, listSeries)
        print('Insertados: {}'.format(len(listSeries))) 


        self.sesion.close()
        '''
        Upload
        '''
        if not self.test:
            Upload(self._platform_code, self._created_at, False)