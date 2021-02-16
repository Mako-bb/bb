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
        self._start_url = self._config['start_url']
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
            'SerieIndex': 4,
            'Link': 'www.wetv.com'
        },
            {
            'PlatformCode': 'us.sundancetv',
            'Name': 'sundance',
            'MovieIndex': 4,
            'SerieIndex': 5,
            'Link': 'www.sundancetv.com'
        },
            {
            'PlatformCode': 'us.ifc',
            'Name': 'ifc',
            'MovieIndex': 4,
            'SerieIndex': 4,
            'Link': 'www.ifc.com'
        },
            {
            'PlatformCode': 'us.amc',
            'Name': 'amc',
            'MovieIndex': 4,
            'SerieIndex': 4,
            'Link': 'www.amc.com'
        },
            {
            'PlatformCode': 'us.bbca',
            'Name': 'bbca',
            'MovieIndex': 3,
            'SerieIndex': 4,
            'Link': 'www.bbcamerica.com'
        }]

        for platform in platforms:

            payloads = []
            payloads_episodes = []

            scraped = Datamanager._getListDB(self, self.titanScraping)
            scraped_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodios)

            ##############
            ### SERIES ###
            ##############

            data = Datamanager._getJSON(self, self._start_url.format(platform['Name'], '/shows'))

            # Cargo la lista de series del .json, teniendo en cuenta el indice que referencia el listado de titulos en la plataforma 
            series = data['data']['children'][platform['SerieIndex']]['children']

            for serie in series:

                info = serie['properties']['cardData']

                serie_link = info['meta']['permalink']
                serie_id = str(info['meta']['nid'])
                serie_title = info['text']['title']
                

                # Traigo el .json de la serie para obtener las temporadas y luego scrapear los episodios
                title_json = Datamanager._getJSON(self, self._start_url.format(platform['Name'], serie_link))

                # Segun figura en la página, si una serie no dispone de ninguna temporada entonces solo contiene un poco de informacion de la misma.
                # Por lo que se podria validar esto, si no cuenta con un dropdown que contenga un listado con al menos una temporada, se saltea a la 
                # proxima serie
                if title_json['data']['children'][3]['type'] != "dropdown":
                    continue
                else:
                    seasons_data = title_json['data']['children'][3]['properties']['dropdownItems']

                # Esta lista va a almacenar los links de cada temporada para luego acceder al .json de cada una
                # y obtener los episodios de la misma
                seasons_links = []

                for season in seasons_data:
                    season_properties = season['properties']

                    # Como el id para los extras y episodios de una temporada es el mismo, reemplazo el "video-extra" del link de
                    # ser necesario (a veces trae directo los epis de la temporada y a veces los extra, por eso es necesario filtrarlo)
                    season_link = season_properties['permalink'].replace("video-extras", "seasons")

                    season_payload = {
                            'Id':        season_properties['nid'],
                            'Synopsis':  None,
                            'Title':     season_properties['title'],
                            'Deeplink':  platform['Link'] + season_link,
                            'Number':    int(season_properties['title'].replace("Season ", "")),
                            'Year':      None,
                            'Image':     None,
                            'Directors': None,
                            'Cast':      None,
                            }

                    seasons_links.append(season_link)

                #################
                ### EPISODIOS ###
                #################

                for season in seasons_links:

                    season_json = Datamanager._getJSON(self, self._start_url.format(platform['Name'], season))

                    episodes_data = season_json['data']['children'][4]

                    # Valido que el elemento en el que me ubico en el .json es de tipo lista, si no lo es significa que la temporada 
                    # puede no tener episodios disponibles por lo que avanza con la ejecucion sin hacer nada. Caso contrario completa el 
                    # payload de los episodios
                    if episodes_data['type'] != "list":
                        continue
                    else:
                        episodes = episodes_data['children']

                    for episode in episodes:

                        info = episode['properties']['cardData']

                        payload_episodes = {
                            'PlatformCode':  platform['PlatformCode'],
                            'Id':            str(info['meta']['nid']), 
                            'ParentId':      serie_id,
                            'ParentTitle':   serie_title,
                            'Episode':       int(info['text']['seasonEpisodeNumber'].split(',')[1].replace('E', '')) if info['text'].get('seasonEpisodeNumber') else None, 
                            'Season':        int(info['text']['seasonEpisodeNumber'].split(',')[0].replace('S', '')) if info['text'].get('seasonEpisodeNumber') else None, 
                            'Title':         info['text']['title'],
                            'OriginalTitle':  None, 
                            'Year':          None, 
                            'Duration':      None,
                            'ExternalIds':   None,
                            'Deeplinks': {
                                'Web':       platform['Link'] + info['meta']['permalink'],
                                'Android':   None,
                                'iOS':       None,
                                },
                            'Synopsis':      info['text']['description'] if info['text'].get('description') else None,
                            'Image':         [info['images']] if info.get('images') else None,
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

                        Datamanager._checkDBandAppend(self, payload_episodes, scraped_episodes, payloads_episodes, isEpi=True)

                # Luego de obtener los datos de las temporadas y con estos también los episodios, finalmente
                # se completa el payload de la serie actual
                payload = {
                    'PlatformCode':  platform['PlatformCode'], # MODIFICAR POR LOS PLATFORM_CODE DEL CONFIG
                    'Id':            serie_id,
                    'Seasons':       season_payload,
                    'Title':         serie_title,
                    'OriginalTitle': None,
                    'CleanTitle':    _replace(serie_title),
                    'Type':          "serie",
                    'Year':          None,
                    'Duration':      None,
                    'Deeplinks': {
                        'Web':       platform['Link'] + serie_link,
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      info['text']['description'] if info['text'].get('description') else None,
                    'Image':         [info['images']] if info.get('images') else None,
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

                Datamanager._checkDBandAppend(self, payload, scraped, payloads)

            Datamanager._insertIntoDB(self, payloads_episodes, self.titanScrapingEpisodios)
            Datamanager._insertIntoDB(self, payloads, self.titanScraping) 

            ###############
            ## PELICULAS ##
            ###############

            # Al momento 'WeTv' no tiene peliculas (11/2/2021), por eso hay que saltearse la plataforma en la iteración
            if platform['Name'] == 'wetv':
                continue
            else:
                data = Datamanager._getJSON(self, self._start_url.format(platform['Name'], '/movies'))

                # Cargo la lista de peliculas del .json, teniendo en cuenta el indice que referencia el listado de titulos en la plataforma 
                movies = data['data']['children'][platform['MovieIndex']]['children']

                for movie in movies:

                    info = movie['properties']['cardData']

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
