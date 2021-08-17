import time
import requests
import json
from handle.replace import _replace
from common import config
from handle.mongo import mongo
from pprint import pprint
from bs4 import BeautifulSoup
from updates.upload import Upload
from handle.datamanager import Datamanager
from handle.payload import Payload
from pymongo.message import insert
import ast
from datetime import datetime


class DarkMattertv():

    def __init__(self, ott_site_uid, ott_site_country, type):

        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        self._start_url = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config(
        )['mongo']['collections']['episode']
        self.api_url = self._config['api_url']
        self.session = requests.session()
        self.headers = {
            "authority": "api-ott.darkmattertv.com",
            "sec-ch-ua": "^\^"
        }
        if type == 'return':
            '''            Retorna a la Ultima Fecha            '''
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
        " Metodo de scrap que inserta datos validados a la BBDD "
        self.payloads = []
        contents_list = []

        # valores para formatear en la url
        parent_ids = [41, 77, 71, 40, 1027, 529, 148,
                      23, 149, 607, 668, 1274, 1273, 998, 69, 8]
        for id in parent_ids:
            uri = self.api_url.format(id)
            contents = self._get_contents(uri)
            list_content = contents['objects']
            contents_list += list_content

        self.scraped = self.query_field(self.titanScraping, field='Id')
        print(f"{self.titanScraping} {len(self.scraped)}")

        for n, item in enumerate(contents_list):
            print(f"\n----- Progreso ({n}/{len(contents_list)}) -----\n")

            if item['id'] in self.scraped:
                print(item['name'] + ' ya esta scrapeado!')
                continue
            else:
                self.scraped.append(item['id'])
                if (item['type']) == 'video':
                    self._movies_payload(item)

        if self.payloads:
            self.mongo.insertMany(self.titanScraping, self.payloads)
        else:
            print(
                f'\n---- Ninguna serie o pelicula para insertar a la base de datos ----\n')

        Upload(self._platform_code, self._created_at, testing=True)

        print("Scraping finalizado")
        self.session.close()

    def _get_contents(self, uri_formated):
        """ Metodo que realiza una request a la uri (API) y retorna la response parseada (JSON).
        Returns:
            list: Lista de diccionarios
        """
        response = self.request(uri_formated, headers=self.headers)
        dict_contents = response.json()

        return dict_contents

    def request(self, url, headers=None):
        """Método para hacer y validar una petición a un servidor.

        Args:
            url (str): Url a la cual realizaremos la petición.

        Returns:
            obj: Retorna un objeto tipo requests.
        """
        request_timeout = 5
        while True:
            try:
                # Request con header:
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=request_timeout
                )
                if response.status_code == 200:
                    return response
                else:
                    raise Exception(f"ERROR: {response.status_code}")
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(request_timeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...')
                time.sleep(request_timeout)
                continue

    def _movies_payload(self, item):
        """Metodo que ordena el payload de las movies"""

        # duration = self._get_duration(item)
        # image = self._get_image('movie', item)
        # print('Movie: ' + item['name'])
        #deeplink = self._get_deep_link(items)

        genres = self._get_genres(item['meta'])
        directors = self._get_directors(item['meta'])
        cast = self._get_cast(item['meta'])

        #for item in items:

        payload = {

                "PlatformCode": self._platform_code,
                "Id": item['id'],
                "Title": item['name'],
                "CleanTitle": _replace(item['name']),
                "OriginalTitle": item['name'],
                "Type": 'movie',  # plataforma solo de movies
                "Year": item.get('year'),
                "Duration": int(item['duration'])/60,
                "ExternalIds": None,
                "Deeplinks": {
                    "Web": item['progressive_url'],
                    "Android": None,
                    "iOS": None,
                },
                "Synopsis": item['long_description'],
                "Image": [item['thumbnail_url']],
                "Rating": item['mpaa_rating'],
                "Provider": None,
                "Genres": [genres],
                "Cast": cast,
                "Directors": directors,
                "Availability": None,
                "Download": None,
                "IsOriginal": None,
                "IsAdult": None,
                "IsBranded": None,
                "Packages": [{'Type': 'free-vod'}],
                "Country": None,
                "Timestamp": datetime.now().isoformat(),
                "CreatedAt": self._created_at,
            }
        self.payloads.append(payload)

    # def _get_deep_link(self, items):

        # for item in items:
        #     deep_links = item['progressive_url']
        # return deep_links

    def _get_genres(self, items):
        items = items['categories']
        name = [{}]
        for item in items:
            name = {
                'genres': item['name']
            }
        return name

    def _get_cast(self, items):
        items = items['categories']
        cast_ = [{}]
        for item in items:
            cast_ = {
                'actors': item.get('actors'),
            }
        return cast_

    def _get_directors(self, items):
        items = items['categories']
        cast_ = [{}]
        for item in items:
            cast_ = {
                'directors': item.get('directors'),
            }
        return cast_
