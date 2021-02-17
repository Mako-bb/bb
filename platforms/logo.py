from os import replace
import time
import requests
import hashlib
import pymongo
import re
import json
import platform
from handle.replace import _replace
from common import config
from datetime import datetime
from handle.mongo import mongo
from slugify import slugify
from handle.datamanager import Datamanager
from updates.upload import Upload
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from time import sleep
import re
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


class Logo():
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._platform_code = self._config['countries'][ott_site_country]
        # self._start_url             = self._config['start_url']
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self.titanPreScraping = config()['mongo']['collections']['prescraping']
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config(
        )['mongo']['collections']['episode']
        self.skippedEpis = 0
        self.skippedTitles = 0

        # Urls .YAML
        self._movies_url = self._config['movie_url']
        self._show_url = self._config['show_url']
        self._format_url = self._config['format_url']
        self._episode_url = self._config['episode_url']
        self._full_episode_url = self._config['full_episodes_url']
        self._cast_url = self._config['cast_url']

        # Queries .YAML
        self._movies_a = self._config['queries']['movies_a']
        self._movie_title = self._config['queries']['movie_title']
        self._movie_desc = self._config['queries']['movie_desc']
        self._show_div = self._config['queries']['show_div']
        self._episode_div = self._config['queries']['episode_div']
        self._content_div = self._config['queries']['content_div']
        self._full_episode_checker_div = self._config['queries']['full_episode_check_div']
        self._episode_grid_div = self._config['queries']['episode_grid_div']
        self._cast_div = self._config['queries']['cast_div']
        self._title_div = self._config['queries']['title_div']


        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}

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

    def __query_field(self, collection, field, extra_filter=None):
        if not extra_filter:
            extra_filter = {}

        find_filter = {
            'PlatformCode': self._platform_code,
            'CreatedAt': self._created_at,
        }

        find_filter.update(extra_filter)

        query = self.mongo.db[collection].find(
            filter=find_filter,
            projection={
                '_id': 0,
                field: 1,
            },
            no_cursor_timeout=False
        )

        query = {item[field] for item in query}

        return query

    def scroll(self, driver, timeout):
        scroll_pause_time = timeout

        # Toma la altura del Scroll
        last_height = driver.execute_script("return document.body.scrollHeight")

        while True:
            # Scrollea hasta el final
            driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);")

            # Espera a que la pagina cargue
            sleep(scroll_pause_time)

            # Calcula la nueva altura del scroll y la compara con la ultima altura de scroll
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                # si las alturas son iguales, la función se quiebra
                break
            last_height = new_height

    def _scraping(self, testing=False):
        ids_guardados = Datamanager._getListDB(self, self.titanScraping)
        ids_guardados_series = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)
        payloads = []
        payloads_series = []
        soup_a = Datamanager._getSoup(self, self._movies_url)
        for link in soup_a.find_all('a',self._movies_a):
            soup_s = Datamanager._getSoup(self,link.get('href'))
            href = link.get('href')
            title = soup_s.find('div', self._movie_title).text
            desc = soup_s.find('div', self._movie_desc).text
            payload = {
                "PlatformCode":  self._platform_code,
                "Id":            hashlib.md5(title.encode('utf-8')).hexdigest(),
                'Title':         title,
                "Type":          "movie",
                'OriginalTitle': None,
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       href,
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                "CleanTitle":    _replace(title),
                'Synopsis':      desc,
                'Image':         None,
                'Rating':        None,
                'Provider':      None,
                'Genres':        None,
                'Cast':          None,
                'Directors':     None,
                'Availability':  None,
                'Download':      None,
                'IsOriginal':    None,
                'IsAdult':       None,
                'Packages':
                    [{'Type': 'tv-everywhere'}],
                'Country':       None,
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }
            Datamanager._checkDBandAppend(
                self, payload, ids_guardados, payloads
            )
        