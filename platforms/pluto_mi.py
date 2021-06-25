import time
import requests
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from handle.payload import Payload
from handle.datamanager import Datamanager
import datetime
# from time import sleep
# import re


class PlutoMI():
    """
    """

    def __init__(self, ott_site_uid, ott_site_country, type):
        self.ott_site_uid = ott_site_uid
        self.ott_site_country = ott_site_country
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config(
        )['mongo']['collections']['episode']

        # self.api_url = self._config['api_url']

        self.session = requests.session()

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
            params = {"PlatformCode": self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']

            self._scraping()

        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing=True)

    def query_field(self, collection, field=None):
        """Método que devuelve una lista de una columna específica
        de la bbdd.

        Args:
            collection (str): Indica la colección de la bbdd.
            field (str, optional): Indica la columna, por ejemplo puede ser
            'Id' o 'CleanTitle. Defaults to None.

        Returns:
            list: Lista de los field encontrados.
        """
        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at
        }

        find_projection = {'_id': 0, field: 1, } if field else None

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection=find_projection,
            no_cursor_timeout=False
        )

        if field:
            query = [item[field] for item in query if item.get(field)]
        else:
            query = list(query)

        return query

    def _scraping(self, testing=False):
        url = 'https://service-vod.clusters.pluto.tv/v3/vod/categories?includeItems=true&includeCategoryFields=imageFeatured%2CiconPng&itemOffset=10000&advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=5ba90432-9a1d-46d1-8f93-b54afe54cd1e&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=5ba90432-9a1d-46d1-8f93-b54afe54cd1e&deviceLat=-34.5106&deviceLon=-58.7536&deviceMake=Microsoft%2BEdge&deviceModel=web&deviceType=web&deviceVersion=91.0.864.54&marketingRegion=VE&serverSideAds=true'
        response = self.session.get(url)
        json_data_pluto = response.json()

        movies_list = []
        series_list = []

        for content in json_data_pluto["categories"]:
            for item in content["items"]:
                self.movie_payload(item, movies_list) if item["type"] == 'movie' else self.series_payload(
                    item, series_list)
        
        #log para debug si saca los datos ok
        '''
        for movie in self.movies_list:
            for title,value in movie.items():
                print(title,value)
        '''

    def movie_payload(self, content, contents_list):
        movie = {
            "PlatformCode": "ar.pultotv",  # No se de donde sacarlo lo hardcode
            "Id": content['_id'],
            "Title": content['name'],
            "CleanTitle": _replace(content['name']),
            "OriginalTitle": content['slug'],
            "Type": content['type'],
            "Year": None,
            "Duration": datetime.timedelta(milliseconds=content['duration']),
            "ExternalIds": None,
            "Deeplinks": {
                # No estoy seguro si es este dato
                "Web": content['stitched']['urls'][0]['url'],
                "Android": None,  # No se de donde sacarlo
                "iOS": None,  # No se de donde sacarlo
            },
            "Synopsis": content['summary'],
            # No estoy seguro si es este dato
            "Image": content['featuredImage']['path'],
            "Rating": content['rating'],
            "Provider": None,
            "Genres": [content['genre']],
            "Cast": None,
            "Directors": None,
            "Availability": None,
            "Download": None,
            "IsOriginal": None,
            "IsAdult": None,
            "IsBranded": None,
            # No estoy seguro si es este dato
            "Packages": [{'Type': 'free-vod'}],
            "Country": None,
            "Timestamp":datetime.datetime.now().isoformat(),
            "CreatedAt": self._created_at,
        }
        contents_list.append(movie)

    def series_payload(self, content, contents_list):
        series = {
            'PlatformCode': "ar.pultotv",  # No se de donde sacarlo lo hardcode
            'Id': content['_id'],
            'Title': content['name'],
            'OriginalTitle': content['slug'],
            'CleanTitle': _replace(content['name']),
            'Type': content['type'],
            'Year': None,
            'Duration': None,
            'Deeplinks': {
                # No estoy seguro si es este dato
                "Web": content['stitched']['urls'][0]['url'],
                'Android': None,  # No se de donde sacarlo
                'iOS': None,  # No se de donde sacarlo
            },
            'Seasons': len(content['seasonsNumbers']),
            'Playback': None,  # No se que es
            'Synopsis': content['summary'],
            # No estoy seguro si es este dato
            'Image': content['featuredImage']['path'],
            'Rating': content['rating'],
            'Provider': None,
            'ExternalIds': None,
            'Genres': content['genre'],
            'Cast': None,
            'Directors': None,
            'Availability': None,
            'Download': None,
            'IsOriginal': None,
            'IsBranded': None,
            'IsAdult': None,
            # No estoy seguro si es este dato
            "Packages": [{'Type': 'free-vod'}],
            'Country': None,
            'Timestamp': datetime.datetime.now().isoformat(),
            'CreatedAt': self._created_at,
        }
        contents_list.append(series)
      
