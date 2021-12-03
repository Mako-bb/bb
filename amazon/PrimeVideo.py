from amazon.Pelicula import Pelicula
from amazon.Serie import Serie
from amazon.Data import Data
from amazon.Platform_dispatcher import PlatformDispatcher
from amazon.Content import Content
from handle.datamanager import Datamanager
from handle.mongo import mongo
from common import config
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from amazon.globals import domain, currency, country, set_globals
from amazon.Token import Token
import time
import requests
import itertools
import os
from amazon.Login import Login
from updates.upload import Upload
from amazon.AmazonCleaner import AmazonCleaner


class PrimeVideo:
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self._platform_code = self._config['countries'][ott_site_country]['platformCode']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes = config()['mongo']['collections']['episode']
        self.sesion = requests.session()
        self.skippedEpis = 0
        self.skippedTitles = 0
        self.dispatcher = PlatformDispatcher()
        set_globals(ott_site_country, ott_site_uid)
        self.links = config()['ott_sites'][ott_site_uid]['countries'][ott_site_country]['links']
        self.headers = {
            'x-requested-with': 'XMLHttpRequest',
            'x-amzn-requestid': 'Y841GHQVM3SST2JD8JKQ',
            'X-Amzn-Client-TTL-Seconds': '15',
            'Cookie':'session-id=355-5684253-4209825; session-id-time=2082787201l; i18n-prefs=JPY; csm-hit=tb:s-EWASN93MK6G5GXAH4YX8|1617918931152&t:1617918931760&adb:adblk_no; ubid-acbjp=356-5647833-3531455; session-token=gnt6x4IBJP1Kpe1Hhn8+ADjuk5QLJaLnleXaZzBQexf7/2jTrzOOS4CvVWC9Fs7URKUu3e4FV27c/ObwOEhwm78cK0BB8rnCcECdHwwyFrZ7qPcGSmFMBLz/KaPmohmgFATBjH+x/7x/6lCptvaz0buHUPTmkHEgkycv4xg9N8bt2n4fo674PjJilbg9yoAW',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0',
            'Referer': f'{domain()}/storefront/ref=topnav_storetab_atv?node=2858778011',
            'Connection': 'keep-alive',
            'Accept': '*/*'
        }
        self.driver = self.get_driver()
        self.lista_contenidos = []
        self.lista_episodios = []
        self.email = config()['ott_sites'][ott_site_uid]['countries'][ott_site_country]['email']
        self.password = config()['ott_sites'][ott_site_uid]['countries'][ott_site_country]['password']
        self.post_scraping = AmazonCleaner(ott_site_uid, ott_site_country)

        if type == 'return':
            # Retorna a la Ultima Fecha
            params = {"PlatformCode": self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
            self.scraping()

        if type == 'scraping':
            self.scraping()
        if type == 'testing':
            self.scraping(testing=True)

    def _mongo(self, db, platform_code):
        return self.mongo.search(db, {
            "PlatformCode": platform_code,
            "CreatedAt": self._created_at
        }) or list()

    def __query_field(self, collection):
        return self._mongo(collection, self._platform_code)

    def scraping(self, testing=False):
        Login.login(self.driver, self.email, self.password)
        contenidos = Content.get_all_content_pv(self)
        total = len(contenidos)
        iteraciones = 0
        for contenido in contenidos:
            iteraciones += 1
            print(f"{iteraciones} / {total}")
            self.scrapear_contenido(contenido, iteraciones)
        self.post_scraping.eliminar_repetidos()
        self.driver.close()
        Upload(self._platform_code, self._created_at, testing=testing)

    @staticmethod
    def get_driver():
        option = Options()
        option.add_argument('--headless')
        return webdriver.Firefox(options=option)

    def scrapear_contenido(self, contenido, iteraciones):
        soup = Datamanager._getSoupSelenium(self, contenido)
        data = Data.get_data(soup)
        try:
            if self.is_movie(data):
                Pelicula.insertar_peliculas(self, soup, contenido)
            else:
                Serie.insertar_series(self, data, contenido)
        except Exception as e:
            pass
        if iteraciones % 500 == 0:
            self.insert_conect_into_db()
            self.insert_episodes_into_db()
            self.reiniciar_mongo()

    def insert_conect_into_db(self):
        lista_contenidos = list(itertools.chain.from_iterable(self.lista_contenidos))
        print(f'Insertando {len(lista_contenidos)} series/peliculas')
        self.mongo.insertMany(self.titanScraping, lista_contenidos)
        self.lista_contenidos.clear()

    def insert_episodes_into_db(self):
        lista_episodios = list(itertools.chain.from_iterable(self.lista_episodios))
        print(f'Insertando {len(lista_episodios)} episodios')
        self.mongo.insertMany(self.titanScrapingEpisodes, lista_episodios)
        self.lista_episodios.clear()

    @staticmethod
    def generar_espera(tiempo):
        n = tiempo
        while n >= 0:
            print('\rTiempo para volver a scrapear: {} segundos'.format(n), end=' ')
            n -= 1
            time.sleep(1)
        print(' ')

    def reiniciar_mongo(self):
        with open('/tmp/reset-mongo-db.lock', 'w') as f:
            pass
        self.generar_espera(70)
        if os.path.exists('/tmp/reset-mongo-db.lock'):
            os.path.remove('/tmp/reset-mongo-db.lock')
        self.mongo = mongo()

    @staticmethod
    def is_movie(dictionary):
        return not dictionary['detail']['detail']

    def eliminar_repetidos(self):
        contenidos_repetidos = list(self.mongo.db[self.titanScraping].aggregate(
            [
                {'$match': {'CreatedAt': self._created_at, 'PlatformCode': self._platform_code}},
                {'$group': {
                    '_id': {'Title': '$Title', 'Year': '$Year', 'Type': '$Type', 'Duration': '$Duration'},
                    'dups': {'$push': "$Id"},
                    'sum': {'$sum': 1}
                }},
                {'$match': {'sum': {'$gt': 1}}}
            ]
        ))
        for contenido in contenidos_repetidos:
            contenido['dups'].pop()
            self.mongo.db[self.titanScraping].remove({'Id': {'$in': contenido['dups']}})

        capitulos_repetidos = list(self.mongo.db[self.titanScrapingEpisodes].aggregate(
            [
                {'$match': {'CreatedAt': self._created_at, 'PlatformCode': self._platform_code}},
                {'$group': {
                    '_id': {'Title': '$Title', 'Year': '$Year', 'Duration': '$Duration',
                            'ParentId': '$ParentId', 'ParentTitle': '$ParentTitle'},
                    'dups': {'$push': "$Id"},
                    'sum': {'$sum': 1}
                }},
                {'$match': {'sum': {'$gt': 1}}}
            ]
        ))
        for capitulo in capitulos_repetidos:
            capitulo['dups'].pop()
            self.mongo.db[self.titanScrapingEpisodes].remove({'Id': {'$in': capitulo['dups']}})
