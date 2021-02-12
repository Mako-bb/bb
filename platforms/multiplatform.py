# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from handle.replace import _replace
from common import config
from datetime import datetime
from handle.mongo import mongo
from slugify import slugify
from handle.datamanager import Datamanager
from updates.upload import Upload

class MultiplatformScraping():
    """
        Scraping de las plataformas SundanceTv, Ifc, Amc, WeTV y BBC America. Las 5 comparten estructura tanto gráfica como interna en las APIS, por lo tanto se unifica el scraping
        para evitar repeticiones y redundancia de código. 

        METODOLOGIA API, HTML, SELENIUM --> API

        NECESITA VPN --> NO
    """
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        
        self.skippedTitles = 0
        self.skippedEpis = 0
        
        self.sesion = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        # TODO: Revisar ya que esta clase maneja mas de un platform_code.
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
            self._scraping(testing=True)

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

        # platforms_data = []

        # for code in self._platform_code:
        #     platform = { 
        #         'Code' : self._platform_code,
        #         'Name' : self._platform_code.replace("us.", "")
        #     }

        #     platforms_data.append(platform)

        # TEMPORAL
        platforms = [{
            'PlatformCode': 'us.wetv',
            'Name': 'wetv',
            'MovieIndex': None,
            'Link': 'www.wetv.com'
        },
            {
            'PlatformCode': 'us.sundancetv',
            'Name': 'sundance',
            'MovieIndex': 4,
            'Link': 'www.sundancetv.com'
        },
            {
            'PlatformCode': 'us.ifc',
            'Name': 'ifc',
            'MovieIndex': 4,
            'Link': 'www.ifc.com'
        },
            {
            'PlatformCode': 'us.amc',
            'Name': 'amc',
            'MovieIndex': 4,
            'Link': 'www.amc.com'
        },
            {
            'PlatformCode': 'us.bbca',
            'Name': 'bbca',
            'MovieIndex': 3,
            'Link': 'www.bbcamerica.com'
        }]

        for platform in platforms:

            payloads = []
            payloads_episodes = []

            scraped = Datamanager._getListDB(self, self.titanScraping)
            scraped_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)

            ###############
            ## PELICULAS ##
            ###############

            # Al momento 'WeTv' no tiene peliculas (11/2/2021), por eso hay que saltearse la plataforma en la iteración
            if platform['Name'] == 'wetv':
                continue
            else:
                data = Datamanager._getJSON(self, 'https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/{}/url/movies?device=web'.format(platform['Name']))

                titulos = data['data']['children'][platform['MovieIndex']]['children']

                for titulo in titulos:

                    info = titulo['properties']['cardData']

                    payload = {
                        'PlatformCode':  platform['PlatformCode'], # MODIFICAR POR LOS PLATFORM_CODE DEL CONFIG
                        'Id':            str(info['meta']['nid']),
                        'Title':         info['text']['title'],
                        'OriginalTitle': None,
                        'CleanTitle':    _replace(info['text']['title']),
                        'Type':          "movie",
                        'Year':          None,
                        'Duration':      None,
                        'Deeplinks': {
                            'Web':       platform['Link'] + info['meta']['permalink'],
                            'Android':   None,
                            'iOS':       None,
                        },
                        'Playback':      None,
                        'Synopsis':      info['text']['description'],
                        'Image':         [info['images']],
                        'Rating':        None,
                        'Provider':      None,
                        'Genres':        [info['meta']['genre']] if info['meta'].get('genre') else None,
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

                    Datamanager._checkDBandAppend(self, payload, scraped, payloads)

            Datamanager._insertIntoDB(self, payloads, self.titanScraping)  
                  

        Upload(self._platform_code, self._created_at, testing=testing)
