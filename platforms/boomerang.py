# -*- coding: utf-8 -*-
import time
import requests  
import pymongo 
import json
import platform
from handle.replace         import _replace
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from handle.datamanager     import Datamanager
from updates.upload         import Upload
'''
// CONEXION
- El sitio requiere conexion a VPN de EEUU
// CONTENIDO
- Se obtiene por API
- El sitio tiene 3 grandes categorías: Shows, Movies y Playlists.
    - Movies: contiene las peliculas de la plataforma.
    - Playlists: recopila espisodios especiales de series y los presenta como una playlist, ejemplo: Especial Navidad Looney-Toons y
    contiene todos los episodios de cada temporada donde fue navidad. 
    - Shows: se presentan las franquicias de la plataforma, dentro de las cuales se encuentran sub-categorias:
        - Movies: las peliculas de ese franquicia que puede o no estar repetidas con la categoria Movies principal del sitio.
        - Shows: donde se presentan las series, dividas en volumenes y sus correspondientes capitulos. 
        - Playlists: armadas exclusivamente con material de dicha franquicia.
// DISPONIBILIDAD MUNDIAL
- La plataforma sólo está disponible para EEUU
// RECORRIDO DEL SCRAPING
    1 - Obtiene Movies de la categoria principal Movies
    2 - Obtiene las franquicias de la categoria principal Shows
    3 - Obtiene  las URLs de Movies/Series de la franquicias y descarta las playlists
    4 - De las URLs obtenidas, crea los payloads de las Movies y las Series, guarda las URLs de las series
    5 - De las URLs de las series, solicita los episodios de cada una y genera los payloads de los episodios
// OBSERVACIONES
- Los episodios viene por API con numeros correlativos respecto de la serie entera, por lo que se optó por colocar ese valor como None
'''
class Boomerang():
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

        headers = {
        'X-Consumer-Key': 'DA59dtVXYLxajktV'
        }

        #### GET MOVIES OF MOVIES CATEGORY ####
        print("------- GET MOVIES OF MOVIES CATEGORY -------")
        page = 1
        total_page = 1
        while not page > total_page:
            URL = 'https://watch.boomerang.com/api/5/1/collection-items/c196?page='+str(page)+'&trans=en'
            response = Datamanager._getJSON(self,URL,headers=headers)
            total_page = response['num_pages']
            for item in response['values']:
                id_movie = item['item']['uuid']
                typeOf = 'movie' if item['item']['is_film'] else 'serie'
                title_movie = item['item']['title']
                duration_movie = item['item']['runtime']
                year_movie = item['item']['year']
                synopsis_movie = item['item']['description']
                provider_movie = item['item']['network'].split(',')
                originalTitle_movie = item['item']['native_lang_title'] if item['item'].get('native_lang_title') else None
                root_weblink = 'https://watch.boomerang.com'
                deeplink_movie = root_weblink + item['item']['url']
                image_movie = (item['item']['base_asset_url']+'/poster.jpg').split(',')
                rating_movie = item['item']['tv_rating']
                package_movie = [{
                'Type': 'subscription-vod'
                }]
                country_movie = item['item']['country_name'].split(',')
                payload = {
                'PlatformCode'      :self._platform_code,
                'Id'                :id_movie,
                'Title'             :title_movie,
                'OriginalTitle'     :originalTitle_movie,
                'CleanTitle'        :_replace(title_movie),
                'Type'              :typeOf,
                'Year'              :year_movie if year_movie >= 1870 and year_movie <= datetime.now().year else None,
                'Duration'          :duration_movie,
                'Deeplinks': {
                    'Web'       :deeplink_movie,
                    'Android'   :None,
                    'iOS'       :None,
                },
                'Playback'          :None,
                'Synopsis'          :synopsis_movie,
                'Image'             :image_movie,
                'Rating'            :rating_movie,
                'Provider'          :provider_movie,
                'Genres'            :None,
                'Cast'              :None,
                'Directors'         :None,
                'Availability'      :None,
                'Download'          :None, ### !!!!AGREGAR
                'IsOriginal'        :None,
                'IsAdult'           :None,
                'Packages'          :package_movie,
                'Country'           :country_movie,
                'Timestamp'         :datetime.now().isoformat(),
                'CreatedAt'         :self._created_at
                }
                Datamanager._checkDBandAppend(self,payload,listDBMovie,listPayload)
            page += 1
        
        #### GET FRANCHISE OF SHOWS CATEGORY ####
        print("------- GET FRANCHISE OF SHOWS CATEGORY -------")
        franchise_list = []
        page = 1
        num_pages = 1
        while not page > num_pages:
            URL = 'https://watch.boomerang.com/api/5/1/collection-items/c195?page='+str(page)+'&trans=en'
            response = Datamanager._getJSON(self,URL,headers=headers)
            num_pages = response['num_pages']
            for franchise in response['values']:
                franchise_list.append(franchise['item']['slug'])
            page += 1

        #### GET CONTENT(Serie,Movie,Playlist) OF FRANCHISE ####
        print("------- GET CONTENT(Serie,Movie,Playlist) OF FRANCHISE -------")
        content_list = []
        for elem in franchise_list:
            URL = 'https://watch.boomerang.com/api/5/1/collection-items/slug/'+elem
            response = Datamanager._getJSON(self,URL,headers=headers)
            for item in response['values']:
                content_list.append(item['item']['url'].replace('/browse/genre/',''))
                
        #### GET URL OF CONTENT ####
        print("------- GET URL OF CONTENT -------")
        urls_item_series = {}
        for item in content_list:
            page = 1
            total_page = 1
            if 'movie' in item:
                while not page > total_page:
                    URL = 'https://watch.boomerang.com/api/5/1/collection-items/slug/'+item+'/?page='+str(page)+'&trans=en'
                    response = Datamanager._getJSON(self,URL,headers=headers)
                    total_page = response['num_pages']
                    for elem in response['values']:
                        id_movie = elem['item']['uuid']
                        typeOf = 'movie' if elem['item']['is_film'] else 'serie'
                        title_movie = elem['item']['title']
                        duration_movie = elem['item']['runtime']
                        year_movie = elem['item']['year']
                        synopsis_movie = elem['item']['description']
                        provider_movie = elem['item']['network'].split(',')
                        originalTitle_movie = elem['item']['native_lang_title'] if elem['item'].get('native_lang_title') else None
                        root_weblink = 'https://watch.boomerang.com'
                        deeplink_movie = root_weblink + elem['item']['url']
                        image_movie = (elem['item']['base_asset_url']+'/poster.jpg').split(',')
                        rating_movie = elem['item']['tv_rating']
                        check_isfree = elem['item']['first_episode_program_schedule'][0]['name']
                        package_movie = [{
                        'Type': 'subscription-vod' if check_isfree != 'Free Movie' else 'free-vod'
                        }]
                        country_movie = elem['item']['country_name'].split(',')
                        payload = {
                        'PlatformCode'      :self._platform_code,
                        'Id'                :id_movie,
                        'Title'             :title_movie,
                        'OriginalTitle'     :originalTitle_movie,
                        'CleanTitle'        :_replace(title_movie),
                        'Type'              :typeOf,
                        'Year'              :year_movie if year_movie >= 1870 and year_movie <= datetime.now().year else None,
                        'Duration'          :duration_movie,
                        'Deeplinks': {
                            'Web'       :deeplink_movie,
                            'Android'   :None,
                            'iOS'       :None,
                        },
                        'Playback'          :None,
                        'Synopsis'          :synopsis_movie,
                        'Image'             :image_movie,
                        'Rating'            :rating_movie,
                        'Provider'          :provider_movie,
                        'Genres'            :None,
                        'Cast'              :None,
                        'Directors'         :None,
                        'Availability'      :None,
                        'Download'          :None,
                        'IsOriginal'        :None,
                        'IsAdult'           :None,
                        'Packages'          :package_movie,
                        'Country'           :country_movie,
                        'Timestamp'         :datetime.now().isoformat(),
                        'CreatedAt'         :self._created_at
                        }
                        Datamanager._checkDBandAppend(self,payload,listDBMovie,listPayload)
                    page += 1
            elif 'serie' in item:
                while not page > total_page:
                    URL = 'https://watch.boomerang.com/api/5/1/collection-items/slug/'+item+'/?page='+str(page)+'&trans=en'
                    response = Datamanager._getJSON(self,URL,headers=headers)
                    total_page = response['num_pages']
                    for elem in response['values']:
                        urls_item_series[elem['item']['title']] = {'url':elem['item']['url'],'num_seasons':elem['item']['num_seasons']}
                        id_serie = elem['item']['uuid']
                        typeOf = 'movie' if elem['item']['is_film'] else 'serie'
                        title_serie = elem['item']['title']
                        duration_serie = elem['item']['runtime']
                        year_serie = elem['item']['year']
                        synopsis_serie = elem['item']['description']
                        provider_serie = elem['item']['network'].split(',')
                        originalTitle_serie = elem['item']['native_lang_title'] if elem['item'].get('native_lang_title') else None
                        root_weblink = 'https://watch.boomerang.com'
                        deeplink_serie = root_weblink + elem['item']['url']
                        image_serie = (elem['item']['base_asset_url']+'/poster.jpg').split(',')
                        rating_serie = elem['item']['tv_rating']
                        package_serie = [{
                        'Type': 'subscription-vod'
                        }]
                        country_serie = elem['item']['country_name'].split(',')
                        payload = {
                        'PlatformCode'      :self._platform_code,
                        'Id'                :id_serie,
                        'Title'             :title_serie,
                        'OriginalTitle'     :originalTitle_serie,
                        'CleanTitle'        :_replace(title_serie),
                        'Type'              :typeOf,
                        'Year'              :year_serie if year_serie >= 1870 and year_serie <= datetime.now().year else None,
                        'Duration'          :duration_serie,
                        'Deeplinks': {
                            'Web'       :deeplink_serie,
                            'Android'   :None,
                            'iOS'       :None,
                        },
                        'Playback'          :None,
                        'Synopsis'          :synopsis_serie,
                        'Image'             :image_serie,
                        'Rating'            :rating_serie,
                        'Provider'          :provider_serie,
                        'Genres'            :None,
                        'Cast'              :None,
                        'Directors'         :None,
                        'Availability'      :None,
                        'Download'          :None, ### !!!!AGREGAR
                        'IsOriginal'        :None,
                        'IsAdult'           :None,
                        'Packages'          :package_serie,
                        'Country'           :country_serie,
                        'Timestamp'         :datetime.now().isoformat(),
                        'CreatedAt'         :self._created_at
                        }
                        Datamanager._checkDBandAppend(self,payload,listDBMovie,listPayload)
                    page += 1  
                    
        #### GET EPISODES OF SERIES ####
        print("------- GET EPISODES -------")
        root_weblink = 'https://watch.boomerang.com'
        for h in urls_item_series:
            url = urls_item_series[h]['url'].replace('/watch/','').strip('/')
            seasons = urls_item_series[h]['num_seasons']
            typeOf = urls_item_series[h]['type'] if urls_item_series.get('type') else None
            page = 1
            while not seasons <= page:
                URL = 'https://watch.boomerang.com/api/5/series/'+url+'/seasons/'+str(page)+'?trans=en'
                response = Datamanager._getJSON(self,URL,headers=headers)
                for elem in response['values']:
                    check_isfree = elem['program_schedule'][0]['name']
                    id_episode = elem['video_uuid']
                    parent_id = elem['series_uuid']
                    parent_title = elem['series_title']
                    title_episode = elem['title']
                    number_season = elem['season']
                    number_episode = None
                    year_episode = int(elem['metadata']['ReleaseYear']) if elem['metadata'].get('ReleaseYear') != None else int(0)
                    duration_episode = int(elem['duration'].split(':')[1])
                    image_episode = (elem['base_asset_url']+'/thumb.jpg').split(',')
                    synopsis_episode = elem['description']
                    rating_episode = elem['tv_rating']
                    genres_episode = elem['metadata']['search-genres'].split(',') if elem['metadata'].get('search-genres') else None
                    deeplink_episode = root_weblink + elem['url']
                    packages = [{
                                'Type': 'subscription-vod' if check_isfree != 'First Episode Free Wall' else 'free-vod'
                                }]
                    download_episode = elem['has_download_files']
                    payloadEpi = {
                                'PlatformCode'  : self._platform_code,
                                'Id'            : id_episode,
                                'Title'         : title_episode,
                                'OriginalTitle' : None,
                                'ParentId'      : parent_id,
                                'ParentTitle'   : parent_title,
                                'Season'        : number_season,
                                'Episode'       : number_episode,
                                'Year'          : year_episode if year_episode >= 1870 and year_episode <= datetime.now().year else None,
                                'Duration'      : duration_episode,
                                'Deeplinks'     : {
                                    'Web': deeplink_episode,
                                    'Android': None,
                                    'iOS': None
                                },
                                'Playback'      : None,
                                'Synopsis'      : synopsis_episode,
                                'Image'         : image_episode,
                                'Rating'        : rating_episode,
                                'Provider'      : None,
                                'Genres'        : genres_episode,
                                'Cast'          : None,
                                'Directors'     : None,
                                'Availability'  : None,
                                'Download'      : download_episode,
                                'IsOriginal'    : None,
                                'IsAdult'       : None,
                                'Country'       : None,
                                'Packages'      : packages,
                                'Timestamp'     : datetime.now().isoformat(),
                                'CreatedAt'     : self._created_at
                                }
                    Datamanager._checkDBandAppend(self,payloadEpi,listDBEpi,listPayloadEpi,isEpi=True)
                page += 1
                URL = 'https://watch.boomerang.com/api/5/series/'+url+'/seasons/'+str(page)+'?trans=en'
                response = Datamanager._getJSON(self,URL,headers=headers)
        
        Datamanager._insertIntoDB(self,listPayload, self.titanScraping)
        Datamanager._insertIntoDB(self,listPayloadEpi,self.titanScrapingEpisodios)

        self.sesion.close()
        Upload(self._platform_code, self._created_at, testing=True)