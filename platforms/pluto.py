import time
import requests
from handle.replace import _replace
from common import config
from handle.mongo import mongo
# from time import sleep
# import re

class Pluto():
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
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']

        self.api_url = self._config['api_url']

        self.sesion = requests.session()

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
        # 1) Encontrar la API o APIS.
        # 2) Bs4
        # 3) Selenium

        uri = self.api_url

        response = self.sesion.get(uri)

        if response.status_code == 200:
            from pprint import pprint

            dict_contents = response.json()
            list_categories = dict_contents['categories']

            for categories in list_categories:
                print("")
                contents = categories['items']

                for content in contents:
                    pprint(content['name'])

                    title = ''
                    cast = ['Sbaraglia','Francella']
                    year = 1900
                    # Imprimir todos los datos que se puedan