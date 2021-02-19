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


class Oxygen():
    """ Plataforma: Oxygen
        País: Estados Unidos (US)
        Tiempo de Ejecución: 40-50min.
        Requiere VPN: No
        BS4/API/Selenium: BS4
        Cantidad de contenidos (última revisión):
            TitanScraping: 166
            TitanScrapingEpisodes: 1522
    """
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
        self.sesion = requests.session()
        self.headers = {"Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"}

        # Urls .YAML
        self._movies_url = self._config['movie_url']
        self._show_url = self._config['show_url']
        self._format_url = self._config['format_url']
        self._episode_url = self._config['episode_url']
        self._full_episode_url = self._config['full_episodes_url']
        self._cast_url = self._config['cast_url']

        # Queries .YAML
        self._movie_div = self._config['queries']['movies_div']
        self._show_div = self._config['queries']['show_div']
        self._episode_div = self._config['queries']['episode_div']
        self._content_div = self._config['queries']['content_div']
        self._full_episode_checker_div = self._config['queries']['full_episode_check_div']
        self._episode_grid_div = self._config['queries']['episode_grid_div']
        self._cast_div = self._config['queries']['cast_div']
        self._title_div = self._config['queries']['title_div']
        self._form_select = self._config['queries']['form_div']
        self._article_div = self._config['queries']['article_div']
        self._episode_label = self._config['queries']['episode_label']
        self._sinopsis_div = self._config['queries']['sinopsis_div']
        self._parent_div = self._config['queries']['parent_div']

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

    def _scraping(self, testing=False):
        ids_guardados = Datamanager._getListDB(self, self.titanScraping)
        ids_guardados_series = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)
        payloads = []
        options = Options()
        options.preferences.update({"javascript.enabled": False})
        soup_a = Datamanager._getSoup(self, self._show_url)
        hrefs = []
        payloads_series = []
        # Buscamos los links que contienen las referencias de las series para ponerlas en una lista
        for show in soup_a.find_all('a', self._show_div):
            hrefs.append(show.get('href'))
        # Buscamos los titulos y recorremos la lista anterior para colocar los deeplinks de forma correcta.
        for titulos, referencia in zip(soup_a.find_all('a', "teaser__wrapper-link"), hrefs):
            titulo = (titulos.find('h2').text).strip()
            image = titulos.find('img').get('src')
            # Definimos el Payload de las series
            payload_series = {
                "PlatformCode":  self._platform_code,
                "Id":            hashlib.md5(titulo.encode('utf-8')).hexdigest(),
                'Title':         titulo,
                "Type":          "serie",
                'OriginalTitle': None,
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       self._format_url + referencia.replace('videos', 'episode-guide'),
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                "CleanTitle":    _replace(titulo),
                'Synopsis':      None,
                'Image':         [image],
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
                self, payload_series, ids_guardados, payloads_series
            )
        # Definimos una lista de los links de cada episodio.
        episode_hrefs = []

        # Recorremos la lista de referencias de la pagina, reemplazando "videos" por "episode-guide" para llegar a la lista de los capitulos
        for href in hrefs:
            soup_b = Datamanager._getSoup(self, self._format_url
                                            + href.replace('videos', 'episode-guide'))
            # Condicional si la serie tiene 2 o más temporadas
            if soup_b.find('select'):
                # Buscamos las opciones dentro del selector
                opciones = soup_b.find(
                    'select', self._form_select).find_all('option')
                # por cada valor de las opciones, scrappeamos todos los valores de la pagina.
                for valor in opciones:
                    print(valor.get('value'))
                    soup_c = Datamanager._getSoup(self, self._format_url
                                                + href.replace('videos', 'episode-guide')
                                                + self._episode_url + valor.get('value'))
                    # Recorremos una lista de los links de cada uno de los episodios de la temporada.
                    for linkepisodio in soup_c.find_all(
                            'article', self._article_div):
                        episode_hrefs.append(
                            linkepisodio.find('a').get('href'))
                        print('added ' + linkepisodio.find('a').get('href'))
            else:
                # Recorremos una lista de los links de cada uno de los episodios.
                for linkepisodios in soup_b.find_all(
                        'article', self._article_div):
                    episode_hrefs.append(linkepisodios.find('a').get('href'))
                    print('added' + linkepisodios.find('a').get('href'))
        # Recorremos todos los links de las referencias de los episodios de cada temporada
        for link in episode_hrefs:
            soup_e = Datamanager._getSoup(
                self, self._format_url + link)
            epi = soup_e.find(
                'div', self._episode_label).text.rstrip().split('-')
            temporada = re.sub('\D', '', str(epi[0]))
            episodio = re.sub('\D', '', str(epi[1]))
            try:
                temporada = int(temporada)
                episodio = int(episodio)
            except:
                temporada = None
                episodio = None
            sinopsis = (soup_e.find('div', self._sinopsis_div).text).strip() if soup_e.find(
                'div', self._sinopsis_div) is not None else None
            titulo = re.sub('\n', '', soup_e.find('h1', self._title_div).text).strip() if soup_e.find(
                'h1', self._title_div) is not None else None
            parentTitle = (soup_e.find('div',self._parent_div).text).strip()
            try:
                image = soup_e.find('div','tv-episode__image-container').find('img').get('src')
            except:
                try:
                    image = soup_e.find('div','video__mpx-player').find('img').get('src')
                except:
                    image = None
            rating = soup_e.find('span','video__rating').text
            # Definimos el payload de los capitulos de cada serie
            payload = {
                "PlatformCode":  self._platform_code,
                "Id": hashlib.md5(titulo.encode('utf-8')
                    + parentTitle.encode('utf-8')
                    + str(link).encode('utf-8')).hexdigest(),
                "ParentId":      hashlib.md5(parentTitle.encode('utf-8')).hexdigest(),
                "ParentTitle":   parentTitle,
                "Episode":     episodio,
                "Season":       temporada,
                'Title':         titulo,
                'OriginalTitle': None,
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       self._format_url + link,
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      sinopsis,
                'Image':         [image],
                'Rating':        rating,
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
                self, payload, ids_guardados_series, payloads, isEpi=True
            )
        Datamanager._insertIntoDB(
            self, payloads, self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)
        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)
