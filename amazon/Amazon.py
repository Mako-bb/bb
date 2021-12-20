import re

from amazon.Data import Data
from amazon.Platform_dispatcher import PlatformDispatcher
from amazon.Content import Content
from amazon.Scraping import Scraping
from handle.mongo import mongo
from common import config
from amazon.globals import domain, currency, country, set_globals
from datetime import datetime
import time
import requests


class Amazon:
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
        self.driver = Scraping.get_driver()
        self.lista_contenidos = []
        self.lista_episodios = []

        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''
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
        lista_contenidos = Content.get_all_content(self)
        self.get_from_jw(lista_contenidos)
        iteraciones = 0
        cant_scrapeados = 1
        lista_ids_scrapeados = []
        for contenido in lista_contenidos:
            print(f"{cant_scrapeados} / {len(lista_contenidos)}")
            cant_scrapeados += 1
            iteraciones += 1
            try:
                if not self.fue_scrapeado(contenido, self.lista_contenidos, lista_ids_scrapeados):
                    Scraping.scrapear_contenido(self, contenido, iteraciones)
            except Exception as e:
                Scraping.guardar_log(contenido, e)
                Scraping.generar_espera(120)
                pass
        Scraping.end_scraping(self)

    @staticmethod
    def fue_scrapeado(contenido, lista_contenidos,lista_ids_scrapeados):
        id_contenido = Data.get_id(contenido)
        if id_contenido in lista_contenidos:
            return True
        else:
            lista_ids_scrapeados.append(id_contenido)
            return False

    def get_from_jw(self, lista_contenidos):
        year = datetime.now().year
        for anio in range(1900, year+1):
            print(f'Obteniendo peliculas del año {anio}')
            try:
                page = 1
                while True:
                    api = self.api_jw(anio, page)
                    items = self.sesion.get(api).json()['items']
                    if not items:
                        break
                    for item in items:
                        self.append_id(item, lista_contenidos)
                    page += 1
            except (KeyError, ValueError):
                pass
        return list(set(lista_contenidos))

    @staticmethod
    def api_jw(anio, page):
        return f'https://apis.justwatch.com/content/titles/en_US/popular?body=%7B%22fields%22:[%22offers%22],' \
               f'%22providers%22:[%22amz%22,%22amp%22],%22release_year_from%22:{anio},%22release_year_until' \
               f'%22:{anio+1},%22sort_asc%22:true,%22enable_provider_filter%22:false,%22monetization_types%22:' \
               f'[],%22page%22:{page},%22page_size%22:100,%22matching_offers_only%22:true%7D&language=en'

    @staticmethod
    def append_id(item, ids_unicos):
        try:
            url = item['offers'][0]['urls']['standard_web']
            id_item = re.search(r'[A-Z0-9]{10}', url).group(0)
            ids_unicos.append(f'{domain()}gp/video/detail/{id_item}/')
        except AttributeError:
            pass
