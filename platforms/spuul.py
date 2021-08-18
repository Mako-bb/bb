# -*- coding: utf-8 -*-
import time
from types import CoroutineType
import requests
import json
import sys
import pprint
import re
from handle.datamanager import RequestsUtils
from common                  import config
from datetime                import datetime
from handle.mongo            import mongo
from updates.upload          import Upload
from handle.replace          import _replace
from seleniumwire            import webdriver
from handle.datamanager      import Datamanager
from bs4                     import BeautifulSoup

class Spuul():
    """
        Spuul es una ott de la India
        DATOS IMPORTANTES:
        - VPN: No.
        - ¿Usa Selenium?: No
        - ¿Tiene API?: Si.
        - ¿Usa BS4?: No.
        - ¿Se relaciona con scripts TP? No
        - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
        - ¿Cuanto demoró la ultima vez? 1:57 minutes
        - ¿Cuanto contenidos trajo la ultima vez? 86 contenidos y 458 episodios.

        OTROS COMENTARIOS:
        - La plataforma se maneja por APIS.

        - Por esta url = https://spuul.com/browse conseguimos la api en donde se encuentran todas las categorias, por el momento no tiene rental pero
        parece que en algún momento aparecerá, la plataforma saltará con error de precio erroneo. 

        - La plataforma cuenta con CREW.

    """

    def __init__(self, ott_site_uid, ott_site_country, operation):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        self._created_at            = time.strftime('%Y-%m-%d')
        self.mongo                  = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.content_type = self._config['url']['content_type']
        self.subcategories = self._config['url']['subcategories']
        self.api_contents = self._config['url']['api_contents']
        self.api_item = self._config['url']['api_item']
        self.epis_item = self._config['url']['epis_item']
        self.sesion = requests.session()
        self.requests_utils = RequestsUtils()
        self.testing = False
        self.skippedTitles = 0
        self.skippedEpis = 0

        if operation == 'scraping':
            self.scraping()
        elif operation == 'return':
            self.testing = True
            params = {'PlatformCode' : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']

            self.scraping()

        elif operation == 'testing':
            self.testing = True
            self.scraping()

    def scraping(self):

        list_url_movies = list()
        list_url_series = list()
        listMovies = list()
        listSeries = list()
        
        list_categories = self.get_categories()
        list_payloads = []

        for categories in list_categories:
                content_type = categories['Type']
                data = Datamanager._getJSON(self, self.api_contents.format(slug=categories['Slug']))
                for block in data['data']['screen']['blocks']:
                    subcategories = Datamanager._getJSON(self, self.subcategories.format(slug=block['widgets'][0]['slug']))
                    for item in subcategories['data']['blocks'][0]['widgets'][0]['playlist']['contents']:
                        if content_type == 'movie':
                            list_url_movies.append(self.api_item.format(id = item['id']))
                        else:
                            list_url_series.append(self.api_item.format(id = item['id']))
        
        #hacemos threads y conseguimos por separado toda la info de las series y peliculas.
        for url in list_url_movies:
            resp = Datamanager._getJSON(self,url)
            listMovies.append(resp['data'])
        for url in list_url_series:
            resp = Datamanager._getJSON(self,url)
            try:
                listSeries.append(resp['data'])
            except:
                if resp['code'] == 500:
                    continue
        self.scraping_movies(listMovies)
        self.scraping_series(listSeries)

        Upload(self._platform_code, self._created_at, testing = self.testing)
    
    @staticmethod
    def get_id(content):  return str(content['id'])

    @staticmethod
    def get_title(content):  return content['name']

    @staticmethod
    def get_original_title(content): return content['original_name']

    @staticmethod
    def get_rating(content):
        try:  
            return content['age_rating'][0]['name']
        except:
            return None
        
    @staticmethod
    def get_synopsis(content):  return content['long_description']

    @staticmethod
    def get_time(content):  return int(content['length'])

    @staticmethod
    def get_year(content):  
        year = content['production_year']
        if year < 1900 or year > datetime.now().year:
            year = None
        else: return year

    @staticmethod
    def get_cast(content): 
        cast = []
        try:
            actors = content['cast']['actors']
        except:
            return None
        for c in actors:
            cast.append(c['name'])
        if cast == []:
            return None
        else:
            return cast
    
    @staticmethod
    def get_director(content):  
        director = []
        try:
            directors = content['cast']['directors']
        except:
            return None
        for d in directors:
            director.append(d['name'])
        if director == []:
            return None
        else: return director
    
    @staticmethod
    def get_crew(content):  
        crew = list()
        list_crew = list()
        content_crew = list()
        try: 
            box_crew = content['cast']
        except:
            return None
        for cast in box_crew:
            if cast == 'actors' or cast == 'directors':
                continue
            list_crew.append(cast)
        for p in list_crew:
            crew = content['cast'][p]
            if crew == []:
                crew = None
            else:
                for n in crew:
                    content_crew.append({'Role': p.replace('_',' '), 'Name': n['name'] })
        if content_crew == []:
            return None
        return content_crew
    @staticmethod
    def get_language(content):
        content_language = list()
        try:
            language = content['short_description']
            language = re.sub('Language -','',language)
            if re.search(',',language):
                box = language.split(',')
                for lang in box:
                    content_language.append(lang)
            else:
                content_language.append(language)
            if content_language == []:
                return None
        except:
            return None
    @staticmethod
    def get_provider(content): return [content['video_provider']['name']]

    @staticmethod
    def get_country(content):
        try:
            country = [content['production_country']]
            if country == ['']:
                return None
        except:
            country = None
        return country

    @classmethod
    def get_deeplink(cls, content): return 'https://www.spuul.com/details/' + cls.get_id(content)

    @staticmethod
    def get_genre(content):
        genres = []
        try:
            genre = content['genres']
        except:
            return None
        for g in genre:
            genres.append(g['name'])
        if genres == []:
            return None
        else: return genres
            

    @classmethod
    def get_clean_title(cls,content):
        title = cls.get_title(content)
        title = re.sub("\(.{0,}\)", "" ,title ,flags=re.IGNORECASE)
        return _replace(title)

    @classmethod
    def get_package(cls,content):
        svod= False
        prices = content['store_product_ids'] 
        for items in prices:
            if items['product_type'] == 'FREE':
                return[{'Type': 'free-vod'}]
            elif items['product_type'] == 'SVOD':
                svod = True
            else:
                print(items['product_type'])
        if svod:
            return[{'Type':'subscription-vod'}]

    @staticmethod
    def get_image(content): 
        image = []
        image.append(content['images']['banner']['url'])
        image.append(content['images']['wide_banner']['url'])
        image.append(content['images']['backdrop']['url'])
        image.append(content['images']['thumbnail']['url'])
        image.append(content['images']['poster']['url'])
        if image == []:
            return None
        else: return image

    def scraping_movies(self, listMovies):
        list_payloads = list()
        list_db = Datamanager._getListDB(self, self.titanScraping)
        for item in listMovies: 
            item = item['asset']                                 
            payload = {
            'PlatformCode'  :self._platform_code,
            'Id': self.get_id(item),
            'Type': 'movie',
            'Title': self.get_title(item) ,
            'OriginalTitle' : self.get_original_title(item),
            'CleanTitle': self.get_clean_title(item),
            'Year': self.get_year(item),
            'Duration': self.get_time(item),
            # 'Language': self.get_language(item),
            'Crew': self.get_crew(item),    
            'Deeplinks': {
                'Web': self.get_deeplink(item),
                'Android': None,
                'iOS': None
            },
            'Playback': None,
            'ExternalIds': None,
            'Synopsis': self.get_synopsis(item),
            'Image': self.get_image(item),
            'Rating': self.get_rating(item),
            'Provider': self.get_provider(item),
            'Genres': self.get_genre(item),
            'Cast': self.get_cast(item),
            'Directors': self.get_director(item), 
            'Availability': None,
            'Download': None,
            'IsOriginal': None,
            'IsAdult': None,
            'Packages': self.get_package(item),
            'Country': self.get_country(item),
            'Timestamp': datetime.now().isoformat(),
            'CreatedAt': self._created_at
            }
            Datamanager._checkDBandAppend(self, payload, list_db, list_payloads)
        Datamanager._insertIntoDB(self, list_payloads, self.titanScraping)           

    def scraping_series(self, listSeries):
        list_payloads = []
        list_episodes = []
        listEpiDB = Datamanager._getListDB(self, self.titanScrapingEpisodios)
        listSeriesDB = Datamanager._getListDB(self, self.titanScraping)
        for serie in listSeries:
            item = serie['asset']
            content = self.find_repeat(item,list_payloads)
            if content == 'no existe':
                serie = Datamanager._getJSON(self,self.api_item.format(id = item['series_id']))['data']
                item = serie['asset']            
            payload = {
                'PlatformCode': self._platform_code,
                'Id': self.get_id(item),
                'Title': self.get_title(item),
                'OriginalTitle': self.get_original_title(item),
                'CleanTitle': self.get_clean_title(item),
                'Type': 'serie',
                'Year': self.get_year(item),
                'Duration': None,
                'Seasons': None,
                'Crew'   : self.get_crew(item),    
                'Deeplinks': {
                    'Web': self.get_deeplink(item),
                    'Android': None,
                    'iOS': None,
                },
                'Playback': None,
                'Synopsis': self.get_synopsis(item),
                'Image': self.get_image(item),
                'Rating': self.get_rating(item),
                'Provider':  self.get_provider(item),
                'Genres': self.get_genre(item),
                'Cast': self.get_cast(item),
                'Directors': self.get_director(item),
                'Availability':None,
                'Download': None,
                'IsOriginal': None,
                'IsBranded': None,
                'IsAdult': None,
                'Packages': self.get_package(item),
                'Country':  self.get_country(item),
                'Timestamp': datetime.now().isoformat(),
                'CreatedAt': self._created_at
            }
            self.get_chapters(item, serie, payload, listEpiDB, list_payloads, listSeriesDB, list_episodes)
        Datamanager._insertIntoDB(self, list_payloads, self.titanScraping)
        Datamanager._insertIntoDB(self, list_episodes, self.titanScrapingEpisodios)

    def find_repeat(self, item, list_payloads):
        """
            Algunas series son traídas como capitulos, le encontramos el id de la serie y
            buscamos la info general, si se encuentra repetido lo salteamos.

            Devuelve:
                existe :  si la serie se encuentra repetida
                no existe : si no se encuentra repetida
                no cumple : si el tipo es series y no necesita pasar por ese proceso
        """
        if item['type'] != 'series':
            for pload in list_payloads:
                if item['series_id'] == pload['Id']:
                    return 'existe'
            return 'no existe'
        else: return 'no cumple'


    def get_chapters(self,item,serie, payload, listEpiDB, list_payloads, listSeriesDB, list_episodes):
        """
            Encuentra los capitulos de cada serie divididos por temporadas y utiliza Datamanager para 
            ver que no se repitan.

            Args:
                serie(dict): La información de la serie
                payload(dict): payload principal de la serie
                listEpiDB(list): recopila en db Titan Scraping Episodes
                list_payloads(list): acumula los payloads de la serie si es que tienen episodios
                listSeriesDB(list): recopila en db Titan Scraping
                list_episodes(list): lista en donde se acumulan los episodios.
                list_id(list): Corrobora que no se repitan epis
        """
        list_epis = list()
        seasons =  self.get_season(serie)
        listEpis = self.get_data_chapters(serie)
        for item in listEpis:
            item = item['asset']
            payloadEpi = {
                'PlatformCode': self._platform_code,
                'ParentId': payload['Id'],
                'ParentTitle': payload['Title'],
                'Id': self.get_id(item),
                'Title': self.get_title(item),
                'Episode': self.get_episode_number(item),
                'Season': self.get_season_number(item),
                'Year': self.get_year(item),
                'Duration': self.get_time(item),
                'Deeplinks': {
                    'Web':  self.get_deeplink(item),
                    'Android': None,
                    'iOS': None
                },
                'Synopsis': self.get_synopsis(item),
                'Image':self.get_image(item),
                'Rating': self.get_rating(item),
                'Provider':  self.get_provider(item),
                'Genres': self.get_genre(item),
                'Cast': self.get_cast(item),
                'Directors': self.get_director(item),
                'Availability': None,
                'Download': None,
                'IsOriginal': None,
                'IsBranded': None,
                'IsAdult': None,
                'Country': None,
                'Packages':self.get_package(item),
                'Timestamp': datetime.now().isoformat(),
                'CreatedAt': self._created_at
            }   
            list_epis.append(payloadEpi)
            Datamanager._checkDBandAppend(self, payloadEpi, listEpiDB, list_episodes, isEpi=True)
        payload['Seasons'] = seasons
        if list_epis:
            Datamanager._checkDBandAppend(self, payload, listSeriesDB, list_payloads)

    @staticmethod
    def get_episode_number(content): return content['episode_number']

    @staticmethod
    def get_season_number(content): return content['season_number']

    def get_season(self,item):
        data_season = []
        for season in item['asset']['seasons']:
            response= Datamanager._getJSON(self, self.api_item.format(id=season['id']))
            info = response['data']['asset']
            data = {
                'Id': self.get_id(info),
                'Sinopsis': self.get_synopsis(info),
                'Deeplink': self.get_deeplink(info),
                'Title': "{}: Season {}".format(self.get_title(info),self.get_season_number(info)),
                'Number': self.get_season_number(info),
                'Episodes': self.get_number_epis(season,item),
                'Year': self.get_year(info),
                'Image': self.get_image(info),
                'Directors':self.get_director(info),
                'Cast': self.get_cast(info),
                'IsOriginal': None
            }
            if data['Episodes'] == 0:
                continue
            data_season.append(data)
        return data_season

    def get_number_epis(self,season,item):
        """
            Devuelve la cantidad de episodios que contiene la temporada
            Recibe:
                -

        """

        if season['seasons_number'] == 1:
            box_contents = item['screen']['blocks']
            for content in box_contents:
                episodes = content['widgets'][0]['playlist']['name']
                if episodes == 'Episodes':
                    return len(content['widgets'][0]['playlist']['contents'])
        else:
            slug_content = 'tvshow/{}/season/{}'.format(self.get_id(item['asset']), str(season['id'])+'-'+ (season['name']).lower())
            response = Datamanager._getJSON(self,self.epis_item.format(slug = slug_content))
            try:
                if response['description'] == 'Season not found':
                    return 0
            except:
                return len(response['data'])

    def get_data_chapters(self, item):
        """
            Se encontrará la información de todos los episodios de todas
            las temporadas y series
            Recibe(dict): informacion de la serie
            Devuelve(list): una lista con todos los diccionarios con toda la info
            de los epis
        """
        list_url = list()
        listEpis = list()
        for season in item['asset']['seasons']:
            if season['seasons_number'] == 1:
                box_contents = item['screen']['blocks']
                for content in box_contents:
                    episodes = content['widgets'][0]['playlist']['name']
                    if episodes == 'Episodes':
                        for content in content['widgets'][0]['playlist']['contents']:
                            list_url.append(self.api_item.format(id = content['id']))
            else:
                slug_content = 'tvshow/{}/season/{}'.format(self.get_id(item['asset']), str(season['id'])+'-'+ (season['name']).lower())
                response = Datamanager._getJSON(self,self.epis_item.format(slug = slug_content))
                try:
                    if response['description'] == 'Season not found':
                        continue
                except:
                    for content in response['data']:
                        list_url.append(self.api_item.format(id = content['id']))
        resp_episodes = self.requests_utils.async_requests(list_url)
        for response in resp_episodes:
            listEpis.append(response.json()['data'])
        return listEpis

    def get_categories(self):
        """
        Se obtienen los codigos de las categorías.
        Retorna: lista con las categorías
        """
        list_content_type = []
        data_category = Datamanager._getJSON(self,self.content_type)
        data_category = data_category['data']['options']
        for cat in data_category:
            if cat['name'] == 'Movies':
                list_content_type.append({'Type': 'movie', 'Slug': cat['slug']})
            elif cat['name'] == 'Series':
                list_content_type.append({'Type': 'serie', 'Slug': cat['slug']})
        return list_content_type

