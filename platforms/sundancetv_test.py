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

class SundanceTvTest():
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

        ###############
        ## PELICULAS ##
        ###############
        request = self.currentSession.get('https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/sundance/url/movies?device=web')
        print(request.status_code, request.url)

        data = request.json()

        titulos = data['data']['children'][4]['children']

        for titulo in titulos:

            info = titulo['properties']['cardData']

            # Type y Genres tienen problemas de tipos, se cargan igual pero habria que revisarlo
            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            str(info['meta']['nid']),
                'Title':         info['text']['title'],
                'OriginalTitle': None,
                'CleanTitle':    _replace(info['text']['title']),
                'Type':          str(info['meta']['schemaType']),
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       'https://www.sundancetv.com{}'.format(info['meta']['permalink']),
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      info['text']['description'],
                'Image':         None,
                'Rating':        None,
                'Provider':      None,
                'Genres':        str(info['meta']['genre']),
                'Cast':          None,
                'Directors':     None,
                'Availability':  None,
                'Download':      None,
                'IsOriginal':    None,
                'IsAdult':       None,
                'Packages':      [{'Type': 'tv-everywhere'}],
                'Country':       None,
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }

            if payload['Id'] not in ids_guardados:
                payloads.append(payload)
                ids_guardados.add(payload['Id'])
                print('Insertado titulo {}'.format(payload['Title']))
            else:
                print('Id ya guardado {}'.format(payload['Id']))

        ###############
        ### SERIES ####
        ###############
        request = self.currentSession.get('https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/sundance/url/shows?device=web')
        print(request.status_code, request.url)

        data = request.json()

        titulos = data['data']['children'][5]['children']

        for titulo in titulos:

            info = titulo['properties']['cardData']

            # Type tiene problema de tipo, se cargan igual pero habria que revisarlo
            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            str(info['meta']['nid']),
                'Title':         info['text']['title'],
                'OriginalTitle': None,
                'CleanTitle':    _replace(info['text']['title']),
                'Type':          info['meta']['schemaType'],
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       'https://www.sundancetv.com{}'.format(info['meta']['permalink']),
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      info['text']['description'],
                'Image':         None,
                'Rating':        None,
                'Provider':      None,
                'Genres':        None,
                'Cast':          None,
                'Directors':     None,
                'Availability':  None,
                'Download':      None,
                'IsOriginal':    None,
                'IsAdult':       None,
                'Packages':      [{'Type': 'tv-everywhere'}],
                'Country':       None,
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }

            if payload['Id'] not in ids_guardados:
                payloads.append(payload)
                ids_guardados.add(payload['Id'])
                print('Insertado titulo {}'.format(payload['Title']))
            else:
                print('Id ya guardado {}'.format(payload['Id']))

        if payloads:
            self.mongo.insertMany(self.titanScraping, payloads)
            print('Insertados {} en {}'.format(len(payloads), self.titanScraping))

        self.currentSession.close()

        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)