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


class StarzMI():
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

    def _scraping(self, testing = False):
        api_url=self.api_url
        content_response = requests.get(api_url)
        content_json = content_response.json()
        content_data = content_json['playContentArray']['playContents']

        for content in content_data:
            self.series_payload(content) if content["contentType"] == 'Series with Season' else self.movie_payload(content)

    def movie_payload(self, content):
        seconds=content['runtime']
        minutes=seconds/60
        duration= int(minutes)
        ratingCode=content['ratingCode']
        ratingSys=content['ratingSystem']
        rating=ratingSys.join(ratingCode)
        genres = []
        directors = []
        cast=[]
        for genre in content['genres']:
            genres.append(genre['description'])
        for credit in content['credits']:
            for rols in credit['keyedRoles']:
                if rols['key'] == 'D':
                    directors.append(credit['name']) 
                elif rols['key'] == 'C':
                        cast.append(credit['name']) 
        movie = {
            "PlatformCode": self._platform_code,
            "Id": content['contentId'],
            "Title": content['title'],
            "CleanTitle": _replace(content['title']),
            "OriginalTitle": content['properCaseTitle'],
            "Type": content['contentType'],
            "Year": content['releaseYear'],
            "Duration": duration,
            "ExternalIds": None,
            "Deeplinks": {
                "Web": self.url,
                "Android": None,
                "iOS": None,
            },
            "Synopsis": content['logLine'],
            "Image": None,
            "Rating": rating,
            "Provider": content['studio'],
            "Genres": genres,
            "Cast": cast,
            "Directors": directors,
            "Availability": None,
            "Download": content['downloadable'],
            "IsOriginal":content['original'],
            "IsAdult": None,
            "IsBranded": None,
            "Packages": [{'Type':'subscription-vod'}],
            "Country": ["AR"],
            "Timestamp":datetime.datetime.now().isoformat(),
            "CreatedAt": self._created_at,
        }
        if self.mongo.search("titanScraping",movie)==False:
            self.mongo.insert("titanScraping",movie)
        else:
            pass

    def series_payload(self, content):
        ratingCode=content['ratingCode']
        ratingSys=content['ratingSystem']
        rating=ratingSys.join(ratingCode)
        genres = []
        directors = []
        cast=[]
        parent_title=content['title']
        parent_id=content['contentId']
        for genre in content['genres']:
            genres.append(genre['description'])
        for credit in content['credits']:
            for rols in credit['keyedRoles']:
                if rols['key'] == 'D':
                    directors.append(credit['name']) 
                elif rols['key'] == 'C':
                        cast.append(credit['name']) 
        series = {
            'PlatformCode': self._platform_code,
            'Id': content['contentId'],
            'Title': content['title'],
            'OriginalTitle': content['properCaseTitle'],
            'CleanTitle': _replace(content['title']),
            'Type': content['contentType'],
            'Year': content['minReleaseYear'],
            'Duration': None,
            'Deeplinks': {
                "Web": self.url,
                'Android': None,
                'iOS': None,
            },
            'Seasons': len(content['childContent']),
            'Playback': None,
            'Synopsis': content['logLine'],
            'Image': None,
            'Rating': rating,
            'Provider': content['studio'],
            'ExternalIds': None,
            'Genres': genres,
            'Cast': cast,
            'Directors': directors,
            'Availability': None,
            'Download': None,
            'IsOriginal': content['original'],
            'IsBranded': None,
            'IsAdult': None,
            "Packages": [{'Type':'subscription-vod'}],
            'Country': ["AR"],
            'Timestamp': datetime.datetime.now().isoformat(),
            'CreatedAt': self._created_at,
        }
        if self.mongo.search("titanScraping",series)==False:
            self.mongo.insert("titanScraping",series)
            self.episodes_payload(content,parent_id,parent_title)
        else:
            pass
    
    def episodes_payload(self, content, parentId, parentTitle):
        counter=0
        for seasonValue in content['childContent']:
            for epValue in seasonValue['childContent']:
                nonTrailer_num=epValue['order']
                if nonTrailer_num >0:
                    counter+=1
                    seconds=epValue['runtime']
                    minutes=seconds/60
                    duration= int(minutes)
                    ratingCode=content['ratingCode']
                    ratingSys=content['ratingSystem']
                    rating=ratingSys.join(ratingCode)
                    genres=[]
                    for genre in epValue['genres']:
                        genres.append(genre['description'])
                    episode = {
                        'PlatformCode':self._platform_code,
                        'ParentId': parentId,
                        'ParentTitle': parentTitle,
                        'Id': epValue['contentId'] ,
                        'Title':epValue['title'] ,
                        'Episode':counter,
                        'Season': seasonValue['order'],
                        'Year': epValue['releaseYear'],
                        'Image':None ,
                        'Duration': duration,
                        'Deeplinks':{
                            'Web':self.url ,
                            'Android': None,
                            'iOS':None ,
                        },
                        'Synopsis':epValue['logLine'],
                        'Rating':rating ,
                        'Provider':epValue['studio'],
                        'ExternalIds': None,
                        'Genres': genres,
                        'Cast':None,
                        'Directors':None,
                        'Availability':None,
                        'Download': None,
                        'IsOriginal': epValue['original'],
                        'IsAdult': None,
                        'Country': self.ott_site_country,
                        'Packages': [{'Type':'subscription-vod'}],
                        'Timestamp': datetime.datetime.now().isoformat(),
                        'CreatedAt': self._created_at,
                    }
                    if self.mongo.search("titanScrapingEpisodes",episode)==False:
                        self.mongo.insert("titanScrapingEpisodes",episode)
                else:
                    pass