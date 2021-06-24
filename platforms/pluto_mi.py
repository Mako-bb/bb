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

        self.api_url = self._config['api_url']
        self.url=self._config['url']

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
        url_api = self.api_url
        response = self.session.get(url_api)
        json_data_pluto = response.json()

        for content in json_data_pluto["categories"]:
            for item in content["items"]:
                self.movie_payload(item) if item["type"] == 'movie' else self.series_payload(item)

    def movie_payload(self, content):
        seconds=content['duration']
        minutes=seconds/60
        duration= int(minutes)
        movie = {
            "PlatformCode": self._platform_code,
            "Id": content['_id'],
            "Title": content['name'],
            "CleanTitle": _replace(content['name']),
            "OriginalTitle": content['slug'],
            "Type": content['type'],
            "Year": None,
            "Duration": duration,
            "ExternalIds": None,
            "Deeplinks": {
                "Web": self.url,
                "Android": None,
                "iOS": None,
            },
            "Synopsis": content['summary'],
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
            "Packages": [{'Type': 'free-vod'}],
            "Country": self.ott_site_country,
            "Timestamp":datetime.datetime.now().isoformat(),
            "CreatedAt": self._created_at,
        }
        if self.mongo.search("titanScraping",movie)==False:
            self.mongo.insert("titanScraping",movie)
        else:
            pass
            

    def series_payload(self, content):
        series = {
            'PlatformCode': self._platform_code,
            'Id': content['_id'],
            'Title': content['name'],
            'OriginalTitle': content['slug'],
            'CleanTitle': _replace(content['name']),
            'Type': content['type'],
            'Year': None,
            'Duration': None,
            'Deeplinks': {
                "Web": self.url,
                'Android': None,
                'iOS': None,
            },
            'Seasons': len(content['seasonsNumbers']),
            'Playback': None,  # No se que es
            'Synopsis': content['summary'],
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
            "Packages": [{'Type': 'free-vod'}],
            'Country': self.ott_site_country,
            'Timestamp': datetime.datetime.now().isoformat(),
            'CreatedAt': self._created_at,
        }
        if self.mongo.search("titanScraping",series)==False:
            self.mongo.insert("titanScraping",series)
            parent_id=content['_id']
            parent_title=_replace(content['name'])
            series_api_url='https://service-vod.clusters.pluto.tv/v3/vod/series/{}/seasons?advertisingId=&appName=web&appVersion=5.17.1-be7b5e79fc7cad022e22627cbb64a390ca9429c7&app_name=web&clientDeviceType=0&clientID=820bd17e-1326-4985-afbf-2a75398c0e4e&clientModelNumber=na&country=AR&deviceDNT=false&deviceId=820bd17e-1326-4985-afbf-2a75398c0e4e&deviceLat=-39.0576&deviceLon=-67.5301&deviceMake=Firefox&deviceModel=web&deviceType=web&deviceVersion=89.0&marketingRegion=VE&serverSideAds=true&sessionID=1ddd9448-d514-11eb-b85e-0242ac110002&sid=1ddd9448-d514-11eb-b85e-0242ac110002&userId=&attributeV4=foo'.format(parent_id)
            self.episodes_payload(series_api_url,parent_id,parent_title)
        else:
            pass

    def episodes_payload(self, metaData, parentId, parentTitle):
        response_episodes = self.session.get(metaData)
        content=response_episodes.json()
        for seasonValue in content['seasons']:
            for epValue in seasonValue['episodes']:
                seconds=epValue['duration']
                minutes=seconds/60
                duration= int(minutes)
                episode = {
                    'PlatformCode':self._platform_code,
                    'ParentId': parentId,
                    'ParentTitle': parentTitle,
                    'Id': epValue['_id'] ,
                    'Title':epValue['name'] ,
                    'Episode':epValue['number'],
                    'Season': epValue['season'],
                    'Year': None,
                    'Image':None ,
                    'Duration': duration,
                    'Deeplinks':{
                        'Web':self.url ,
                        'Android': None,
                        'iOS':None ,
                    },
                    'Synopsis':epValue['description'],
                    'Rating':epValue['rating'] ,
                    'Provider':None ,
                    'ExternalIds': None,
                    'Genres': epValue['genre'],
                    'Cast':None ,
                    'Directors':None ,
                    'Availability':None ,
                    'Download':None ,
                    'IsOriginal': None,
                    'IsAdult': None,
                    'Country': self.ott_site_country,
                    'Packages': [{'Type': 'free-vod'}],
                    'Timestamp': datetime.datetime.now().isoformat(),
                    'CreatedAt': self._created_at,
                }
                if self.mongo.search("titanScrapingEpisodes",episode):
                    pass
                else:
                    self.mongo.insert("titanScrapingEpisodes",episode)
