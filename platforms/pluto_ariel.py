import time
import requests
from handle.replace import _replace
from common import config
from handle.mongo import mongo
import json
from handle.payload import Payload
from pymongo import MongoClient


class Pluto():
    """
    Pluto es una ott onDemand

    DATOS IMPORTANTES:
    - VPN: No.
    - ¿Usa Selenium?: No.
    - ¿Tiene API?: Si.
    - ¿Usa BS4?: No.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instancia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? Todavía no se realizó.
    - ¿Cuanto contenidos trajo la ultima vez? Todavía no se realizó.
    """

    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.api = self._config['api_url']
        self.movie_url = self._config['movie_url']
        self.episodes_url = self._config['episodes_url']
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

    def _scraping(self, testing=False):
        api = self.get_api()
        payload = self.get_payload(api)
        # payloadEpisodes = self.get_payload_episodes(api)

    def get_api(self):
        """
        Obtenemos y verificamos el estado de la api.

        Retornamos la api en caso de encontrarla.
        """
        api = requests.get(self.api)
        if api.status_code == 200:
            return api
        else:
            print('No se encuentra la api.')

    def get_payload(self, api):
        """
        Obtenemos los payloads de las películas y series con sus capítulos.

        Retornamos un diccionario de los payloads.
        """
        api = json.loads(api.text)
        categories = api['categories']
        self.get_movies(categories)

        print('Acá terminamos')

    def get_movies(self, dict_categories):
        payload_movies = []
        for category in dict_categories:
            category_name = category['name']
            for item in category['items']:
                title = item['name']
                if item['type'] == 'movie':
                    clean_title = _replace(title)
                    payload_movies.append(Payload(platform_code=self._platform_code,
                                                  id_=item['_id'],
                                                  title=item['name'],
                                                  clean_title=clean_title,
                                                  duration=item['duration'],
                                                  deeplink_web=self.get_deeplink(type_content=item['type'],
                                                                                 slug=item['slug']),
                                                  synopsis=item['summary'],
                                                  image=item['covers'][0],
                                                  rating=item['rating'],
                                                  genres=item['genre']
                                                  ).__dict__)
        from pprint import pprint
        pprint(payload_movies)

    def get_deeplink(self, type_content, slug, season=None, episode_name=None, emission_year=None, ):
        """
        El deeplink no está en la API.
        Para las películas lo generamos a través de un slug tag que nos brinda la API.
        https: // pluto.tv / on - demand / movies / {slug_name}
        Para cada episodio de la serie
        https://pluto.tv/on-demand/series/{slug_name}/season/{int: season}/episode/{episode_name}-{emission_year}-
        {int: season}-{int:episode}
        """
        deeplink = None
        if type_content == 'movie':
            deeplink = self.movie_url.format(slug)
        else:
            pass
        return deeplink
