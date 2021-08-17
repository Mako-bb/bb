import re

import json
import time
import requests
import hashlib
import platform   
from common                 import config
from datetime               import datetime
from handle.mongo           import mongo
from updates.upload         import Upload
from handle.datamanager     import Datamanager
from handle.replace         import _replace
from handle.payload         import Payload

class DSmartGo():
    """
    DSmartGo es una ott Turquía.

    DATOS IMPORTANTES:
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si.
    - ¿Usa BS4?: No.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? 5 min (2021-04-16)
    - ¿Cuanto contenidos trajo la ultima vez? 1303 titanScraping, 2220 titanScrapingEpisodes (2021-04-16)

    OTROS COMENTARIOS:
    Setee la duracion de los episodios a None porque en varios casos era incorrecta
    """
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
        self.country                = ott_site_country
        
        # urls
        self.api_categorias         = self._config['urls']['api_categorias']
        self.api_post               = self._config['urls']['api_post']
        self.api_static             = self._config['urls']['api_static']
        self.api_seasons            = self._config['urls']['api_seasons']
        self.base_url               = self._config['urls']['base_url']

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
        
        if type == 'testing':
            self._scraping(testing = True)
    
    def _scraping(self, testing = False):

        self.listaSeriesyPelis = []
        self.listaSeriesyPelisDB = Datamanager._getListDB(self,self.titanScraping)
            
        self.listaEpi = []
        self.listaEpiDB = Datamanager._getListDB(self,self.titanScrapingEpisodios)

        self.packages = [{'Type':'subscription-vod'}]

        categorias = Datamanager._getJSON(self, self.api_categorias)
        tipo_cont = []
        for categoria in categorias:
            sub_categorias = categoria['ListInfo']
            print('/' * 5,categoria['PageTitle'], len(sub_categorias), '/' * 5)

            for sub_cat in sub_categorias:
                post_request = sub_cat['Request']
                id_sub_cat = sub_cat['Id']

                if post_request != '':
                    # hacer request por post
                    contenidos = []
                    page_index = 0
                    while True:
                        data = self._post_request(body = post_request, page_index=page_index)
                        
                        if data['Items'] == []:
                            break
                        
                        contenidos += data['Items']
                        page_index += 1

                else:
                    # hacer request por get
                    contenidos = Datamanager._getJSON(self, self.api_static.format(id_sub_cat))
                    contenidos = [contenido['Item'] for contenido in contenidos]

                for contenido in contenidos:
                    
                    payload = self._obtener_data_contenido(contenido)

                    if not payload:
                        continue

                    Datamanager._checkDBandAppend(self, payload, self.listaSeriesyPelisDB, self.listaSeriesyPelis)
                    
                    if len(self.listaSeriesyPelis) == 20:
                        Datamanager._insertIntoDB(self, self.listaSeriesyPelis, self.titanScraping)
                        Datamanager._insertIntoDB(self, self.listaEpi, self.titanScrapingEpisodios)
                
            if self.listaSeriesyPelis:
                Datamanager._insertIntoDB(self, self.listaSeriesyPelis, self.titanScraping)
                Datamanager._insertIntoDB(self, self.listaEpi, self.titanScrapingEpisodios)

        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)
        
    def _post_request(self, body, page_index):
        
        regex_page_index = re.compile(r'"PageIndex":\d+')
        
        headers = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json;charset=utf-8'
        }

        body = regex_page_index.sub('"PageIndex":{}'.format(page_index), body)
        data = Datamanager._getJSON(self, self.api_post, headers=headers, data=body, usePOST=True)

        return data
        
    def _obtener_data_contenido(self, contenido):
        
        content_type = 'movie' if contenido['ContentType'][0]['Name'] == 'MovieContainer' else 'serie'

        id_ = str(contenido['Id'])
        title = contenido['Name']
        slug = contenido['EncodedURL']
        slug_content_type = 'film' if content_type == 'movie' else 'dizi'
        deeplink = self.base_url + slug_content_type + '/detay/' + slug + '/' + id_
        images = contenido['Images']
        images = ['https://img.dsmartgo.com.tr' + image['ImageUrl'] for image in images] if images else None
        synopsis = contenido['Description']
        duration = int(contenido['Duration'] // 60) if contenido['Duration'] else None
        
        availability_start = contenido['DisplayStart']
        availability_end = contenido['DisplayEnd']
        
        if availability_start and datetime.strptime(availability_start, '%Y-%m-%dT%H:%M:%S') > datetime.now():
            return # titulo no disponible 
        if availability_end and datetime.strptime(availability_end, '%Y-%m-%dT%H:%M:%S') < datetime.now():
            return # titulo no disponible 
            
        metadata = contenido['Metadata']
        original_title = None
        year = None
        downloadable = None
        genres = []
        directors = []
        cast = []
        country = []
        provider = []
        episode_format = None
        
        for meta in metadata:
            if meta['NameSpace'] == 'original_name':
                original_title = meta['Value'].strip()
            if meta['NameSpace'] == 'genres':
                genres.append(meta['Value'].strip())
            if meta['NameSpace'] == 'directors':
                directors.append(meta['Value'].strip())
            if meta['NameSpace'] == 'cast':
                cast.append(meta['Value'].strip())
            if meta['NameSpace'] == 'made_year':
                year = meta['Value']
            if meta['NameSpace'] == 'downloadable':
                downloadable = meta['Value'] == 'True'
            if meta['NameSpace'] == 'origin':
                country.append(meta['Value'].strip())
            if meta['NameSpace'] == 'channel':
                provider.append(meta['Value'].strip())
            if content_type == 'serie' and meta['NameSpace'] == 'etf':
                episode_format = meta['Value'] 
        
        year = int(year.split('-')[0]) if year else None
        genres = genres if genres != [] else None
        directors = directors if directors != [] else None
        cast = cast if cast != [] else None
        country = country if country != [] else None
        provider = provider if provider != [] else None

        if content_type == 'serie':
            duration = None
            categorias = contenido['Category']
            api_id = None
            for categoria in categorias:
                if categoria['Name'] == title:
                    api_id = str(categoria['Id'])
                    break
            if api_id:
                seasons = Datamanager._getJSON(self, self.api_seasons.format(api_id))
                seasons = [season['ShortName'] for season in seasons] if seasons else []

                epi_count = 0

                for season in seasons:
                    season_num = int(season.split('-')[-1])
                    body = '{"Categories":[' + api_id + '],"ContentTypes":[1],"CustomFilters":[{"Name":"season","Value":"' + season + '"}],"SortDirection":0,"SortType":4,"PageIndex":0,"PageSize":20,"CustomSortField":"' + episode_format + '"}'
                    
                    page_index = 0
                    while True:

                        data_season = self._post_request(body, page_index)
                        if data_season['Items'] == []:
                            break
                        
                        epi_count += self._obtener_data_epis(data_season, season_num, title, id_)
                        page_index += 1

                if epi_count == 0:
                    print('Serie sin episodios!! SKIP', id_)
                    return

        payload = Payload(platform_code=self._platform_code, id_=id_, title=title, clean_title=_replace(title), year=year,
                        duration = duration, deeplink_web=deeplink, synopsis=synopsis, image=images, provider=provider, genres=genres,
                        availability=availability_end, download=downloadable, packages = self.packages, country = country, 
                        directors=directors, cast=cast, original_title=original_title, createdAt=self._created_at)
        
        if content_type == 'movie':
            return payload.payload_movie()
        else:
            return payload.payload_serie()

    def _obtener_data_epis(self, data_season, season_num, parent_title, parent_id):

        epi_count = 0
        
        data_season = data_season['Items']
        for epi in data_season:
            id_ = str(epi['Id'])
            title = epi['Name']
            slug = epi['EncodedURL']
            deeplink = self.base_url + 'dizi-izle/' + slug + '/' + id_
            synopsis = epi['Description']
            images = epi['Images']
            images = ['https://img.dsmartgo.com.tr' + image['ImageUrl'] for image in images if image['ImageType'] == 'Thumbnail'] if images else None
            #duration = int(epi['Duration'] // 60) if epi['Duration'] else None
            duration = None # varios episodios venian con duraciones erroneas
            availability_start = epi['DisplayStart']
            availability_end = epi['DisplayEnd']
            
            if availability_start and datetime.strptime(availability_start, '%Y-%m-%dT%H:%M:%S') > datetime.now():
                continue # titulo no disponible 
            if availability_end and datetime.strptime(availability_end, '%Y-%m-%dT%H:%M:%S') < datetime.now():
                continue # titulo no disponible 
            
            metadata = epi['Metadata']

            epi_num = None
            year = None
            downloadable = None
            genres = []
            directors = []
            cast = []
            country = []
            provider = []
            
            for meta in metadata:
                if meta['NameSpace'] == 'episode_number':
                    epi_num = meta['Value']
                if meta['NameSpace'] == 'genres':
                    genres.append(meta['Value'].strip())
                if meta['NameSpace'] == 'directors':
                    directors.append(meta['Value'].strip())
                if meta['NameSpace'] == 'cast':
                    cast.append(meta['Value'].strip())
                if meta['NameSpace'] == 'made_year':
                    year = meta['Value']
                if meta['NameSpace'] == 'downloadable':
                    downloadable = meta['Value'] == 'True'
                if meta['NameSpace'] == 'origin':
                    country.append(meta['Value'].strip())
                if meta['NameSpace'] == 'channel':
                    provider.append(meta['Value'].strip())
            
            year = int(year.split('-')[0]) if year else None
            epi_num = int(epi_num) if epi_num else None
            genres = genres if genres != [] else None
            directors = directors if directors != [] else None
            cast = cast if cast != [] else None
            country = country if country != [] else None
            provider = provider if provider != [] else None

            payload_epi = Payload(platform_code=self._platform_code, id_=id_, title=title, year=year,duration = duration, 
                        deeplink_web=deeplink, synopsis=synopsis, image=images, provider=provider, genres=genres,
                        availability=availability_end, download=downloadable, packages = self.packages, country = country, 
                        directors=directors, cast=cast, parent_id=parent_id, parent_title=parent_title, episode=epi_num, 
                        season=season_num, createdAt=self._created_at).payload_episode()

            Datamanager._checkDBandAppend(self, payload_epi, self.listaEpiDB, self.listaEpi, isEpi=True)
            epi_count += 1
        
        return epi_count

