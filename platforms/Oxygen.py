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
        url = "https://www.oxygen.com/full-episodes"
        soup_a = Datamanager._getSoup(self, url)
        hrefs = []
        payloads_series = []
        # Buscamos los links que contienen las referencias de las series para ponerlas en una lista
        for show in soup_a.find_all('a', class_='teaser__wrapper-link'):
            hrefs.append(show.get('href'))
        # Buscamos los titulos y recorremos la lista anterior para colocar los deeplinks de forma correcta.
        for titulos, referencia in zip(soup_a.find_all('h2', class_='headline'), hrefs):
            titulo = (titulos.text).strip()
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
                    'Web':       'https://www.oxygen.com' + referencia.replace('videos', 'episode-guide'),
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                "CleanTitle":    _replace(titulo),
                'Synopsis':      None,
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
                self, payload_series, ids_guardados, payloads_series
            )
        # Definimos una lista de los links de cada episodio.
        episode_hrefs = []

        # Recorremos la lista de referencias de la pagina, reemplazando "videos" por "episode-guide" para llegar a la lista de los capitulos
        for href in hrefs:
            soup_b = Datamanager._getSoup(self, 'https://www.oxygen.com' +
                                          href.replace('videos', 'episode-guide'))
            # Condicional si la serie tiene 2 o más temporadas
            if soup_b.find('select'):
                # Buscamos las opciones dentro del selector
                opciones = soup_b.find(
                    'select', class_='form-select').find_all('option')
                # por cada valor de las opciones, scrappeamos todos los valores de la pagina.
                for valor in opciones:
                    print(valor.get('value'))
                    soup_c = Datamanager._getSoup(self, 'https://www.oxygen.com' + href.replace('videos', 'episode-guide') +
                                                  '?field_tv_shows_season=' + valor.get('value'))
                    # Recorremos una lista de los links de cada uno de los episodios de la temporada.
                    for linkepisodio in soup_c.find_all(
                            'article', class_='teaser teaser--episode-guide-teaser'):
                        episode_hrefs.append(
                            linkepisodio.find('a').get('href'))
                        print('added ' + linkepisodio.find('a').get('href'))
            else:
                # Recorremos una lista de los links de cada uno de los episodios.
                for linkepisodios in soup_b.find_all(
                        'article', class_='teaser teaser--episode-guide-teaser'):
                    episode_hrefs.append(linkepisodios.find('a').get('href'))
                    print('added' + linkepisodios.find('a').get('href'))
        # Definimos variables de soporte para episodios que no están catalogados en orden.
        sneak = 0
        specialcount = 0
        # Recorremos todos los links de las referencias de los episodios de cada temporada
        for link in episode_hrefs:
            soup_e = Datamanager._getSoup(
                self, 'https://www.oxygen.com' + link)
            epi = soup_e.find(
                'div', class_='video__label').text.rstrip().split('-')
            # Filtro de información para el payload
            if epi[1] == " Sneak Peek":
                epi[1] = 50 + sneak

            if epi[1] == " Special":
                epi[1] = 60 + specialcount
                specialcount += 1
            temporada = re.sub('\D', '', str(epi[0]))
            episodio = re.sub('\D', '', str(epi[1]))

            sinopsis = (soup_e.find('div', class_='video__description').text).strip() if soup_e.find(
                'div', class_='video__description') is not None else None
            titulo = re.sub('\n', '', soup_e.find('h1', class_='headline').text).strip() if soup_e.find(
                'h1', class_='headline') is not None else None
            parentTitle = (soup_e.find('div', 'nav__title').text).strip()
            # Definimos el payload de los capitulos de cada serie
            payload = {
                "PlatformCode":  self._platform_code,
                "Id": hashlib.md5(titulo.encode('utf-8')+str(episodio).encode('utf-8')+str(temporada).encode('utf-8')).hexdigest(),
                "ParentId":      hashlib.md5(parentTitle.encode('utf-8')).hexdigest(),
                "ParentTitle":   parentTitle,
                "Episode":     int(episodio),
                "Season":        int(temporada),
                'Title':         titulo,
                'OriginalTitle': None,
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       'https://www.oxygen.com' + link,
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      sinopsis,
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
                self, payload, ids_guardados_series, payloads, isEpi=True
            )
        Datamanager._insertIntoDB(
            self, payloads, self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self, payloads_series, self.titanScraping)
        if not testing:
            Upload(self._platform_code, self._created_at, testing=True)
