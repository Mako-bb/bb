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
from bs4                    import BeautifulSoup
from selenium               import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from handle.datamanager     import Datamanager
from updates.upload         import Upload

class Optimum_test_diego():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']

        self.sesion = requests.session()
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

    def _scraping(self):
        offset = 0
        while True:
        
            URL = 'https://www.optimum.net/api/vod-webapp/services/v1/onyx/getTitlesForPagination/48265008/20/'+str(offset)+'?sort=1&filter=0'
            req = Datamanager._getJSON(self,URL)
            titles = req['data']['result']['titles']

            for each in titles:
                ### ID
                _id = each['title_id']
                ### TYPE
                type = 'movie'
                ### TITLE
                if each.get('tms_title'):
                    title = (each['title'] if len(each['title']) > len(each['tms_title']) else each['tms_title'])
                else:
                    title = each['title']
                ### YEAR
                year = each['release_year']
                ### DURATION
                duration = each['stream_runtime']//60
                ### DEEPLINK
                if each.get('asset_id'):
                    deeplink = 'https://www.optimum.net/tv/asset/#/movie/'+str(each['asset_id'])
                elif each.get('hd_asset'):
                    deeplink = 'https://www.optimum.net/tv/asset/#/movie/'+str(each['hd_asset'])
                else:
                    deeplink = 'https://www.optimum.net/tv/asset/#/movie/'+str(each['sd_asset'])
                ### SYNOPSIS
                description = each['long_desc']
                ### GENRES
                if each.get('genres'):
                    genres = each['genres'].split(', ')
                else:
                    genres = None
                ### CAST
                if each.get('actors'):
                    cast = each['actors'].split(', ')
                else:
                    cast = None
                ### DIRECTORS
                if each.get('directors'):
                    directors = each['directors'].split(', ')
                else:
                    directors = None
                ### AVAILABILITY
                availability = each['offer_end_date']

                payload = {
                        'PlatformCode'      : self._platform_code,
                        'Id'                : _id ,
                        'Type'              : type,
                        'Title'             : title,
                        'CleanTitle'        : _replace(title),
                        'OriginalTitle'     : None,
                        'Year'              : year,
                        'Duration'          : duration,
                        'Deeplinks'         : {
                                            'Web': deeplink,
                                            'Android': None,
                                            'iOS': None
                        },
                        'Synopsis'          : description,
                        'Rating'            : None,
                        'Provider'          : None,
                        'Genres'            : genres,
                        'Cast'              : cast,
                        'Directors'         : directors,
                        'Availability'      : None,
                        'Download'          : None,
                        'IsOriginal'        : None,
                        'IsAdult'           : None,
                        'Packages'          : None,
                        'Country'           : None,
                        'Timestamp'         : datetime.now().isoformat(),
                        'CreatedAt'         : self._created_at
                    }
                
            if req['data']['result']['next'] == '0':
                break
            else:
                offset += 20
        


