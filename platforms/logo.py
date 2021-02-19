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
        self._content_filter = self._config['queries']['content_filter']
        self._content_result = self._config['queries']['content_result']
        self._title_div = self._config['queries']['title_div']
        self._desc_div = self._config['queries']['desc_div']
        self._season_selector = self._config['queries']['season_selector_css']
        self._all_season_selector = self._config['queries']['all_season_selector_xpath']
        self._available_checkbox = self._config['queries']['available_checkbox']
        self._available_id = self._config['queries']['available_id']
        self._load_more_xpath = self._config['queries']['load_more_xpath']
        self._image_div = self._config['queries']['image_div']
        self._episode_div = self._config['queries']['episode_div']
        self._show_title_h1 = self._config['queries']['show_title_h1']
        self._duration_div = self._config['queries']['duration_div']



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
        """ Plataforma: http://www.logotv.com
            el Metodo _scrapping se encarga de conseguir todos los contenidos de la plataforma.

        """
        ids_guardados = Datamanager._getListDB(self, self.titanScraping)
        ids_guardados_series = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)
        payloads = []
        payloads_series = []
        # Vamos a la URl de las peliculas y obtenemos el soup con la función _getSoup
        soup_a = Datamanager._getSoup(self, self._movies_url)
        # buscamos todos los links de la pagina y los recorremos con un bucle
        for link in soup_a.find_all('a',self._movies_a):
            # Obtenemos el Soup de cada uno de los links de la pagina para poder obtener la descripción
            soup_s = Datamanager._getSoup(self,link.get('href'))
            href = link.get('href')
            title = soup_s.find('div', self._movie_title).text
            desc = soup_s.find('div', self._movie_desc).text
            image = soup_s.find('div',self._image_div).get('data-info').split('"')
            image = image[9]
            # Definimos los valores del payload con las variables
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
        # Obtenemos el Soup de los shows y definimos una lista para luego recorrer con bucles
        lista = []
        soup_b = Datamanager._getSoup(self, self._show_url)
        # Bucle que obtiene cada uno de los links y los muestra por consola
        for link in soup_b.find_all('li',self._show_div):
            lista.append(link.find('a').get('href'))
            print(link.find('a').get('href'))
            # rorremos la lista anterior y obtenemos el soup de cada elemento para así poder obtener los valores del payload
        for ref in lista:
            soup_show = Datamanager._getSoup(self, ref)
            title = soup_show.find('h2',self._title_div).text if soup_show.find('h2',self._title_div) else None
            desc = soup_show.find('div',self._desc_div).text if soup_show.find('div',self._desc_div) else None
            data_info = soup_show.find('div',self._image_div).get('data-info').split('"')
            image_src = data_info[9]
            payload_show = {
                "PlatformCode":  self._platform_code,
                "Id":            hashlib.md5(title.encode('utf-8')).hexdigest(),
                'Title':         title,
                "Type":          "serie",
                'OriginalTitle': None,
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       ref,
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                "CleanTitle":    _replace(title),
                'Synopsis':      desc,
                'Image':         [image_src],
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
                self, payload_show, ids_guardados, payloads
            )
        #Definimos el Driver de Selenium para recorrer los episodios
        driver = webdriver.Firefox()
        for link in lista:
            # Filtro para evitar url mal guardada en la pagina
            if link == self._content_filter:
                link = self._content_result
            # Vamos con el driver a cada una de las series de la lista
            driver.get(link+ self._episode_url)
            time.sleep(3)
            # Buscamos el selector de temporadas y lo clickeamos
            season = driver.find_element_by_css_selector(self._season_selector)
            season.click()
            # Esperamos a que los componentes de la pagina carguen
            waiter = WebDriverWait(driver, 7)
            waiter.until(EC.presence_of_element_located((By.XPATH,
            self._all_season_selector)))
            # Buscamos el boton de "Todas las temporadas" y lo clickeamos
            all_seasons = driver.find_element_by_xpath(self._all_season_selector)
            all_seasons.click()
            time.sleep(3)
            # Esperamos a que los componentes carguen
            w = WebDriverWait(driver, 7)
            w.until(EC.presence_of_element_located((By.ID,
            self._available_checkbox)))
            # buscamos el boton que muestra los episodios no disponibles en la plataforma pero con información
            Available = driver.find_element_by_id(self._available_id).find_element_by_tag_name('label')
            Available.click()
            time.sleep(1)
            # Ejecutamos un script que hace que la pagina vaya al punto mas bajo
            driver.execute_script("return document.body.scrollHeight")
            driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")
            # Creamos un bucle que busca el boton "Load More" Mientras exista y sea visible
            while True:
                try:
                    load_more = driver.find_element_by_xpath(self._load_more_xpath)
                    time.sleep(1)
                    if load_more.is_displayed():
                        load_more.click()
                    else:
                        break
                except:
                    break
            time.sleep(2)
            # tomamos un soup de la pagina del driver
            episode_soup_ = BeautifulSoup(driver.page_source, 'lxml')
            # Recorremos con un bucle todos los 
            for episode in episode_soup_.find_all('a',self._episode_div):
                # Definimos las variables del Payload
                parent_title = episode_soup_.find('h1',self._show_title_h1).text
                title = episode.find('h3').text if episode.find('h3') else "None"
                episode_season = episode.find('h4').text if episode.find('h4') else "None"
                # Filtro para episodios que no tienen ningun valor numerico(Especiales, final de temporada, etc...)
                if episode_season != "None":
                    try:
                        episode_season = episode_season.split(',')
                        season= int(episode_season[0].replace('Season ', ''))
                        episode_number  = int(episode_season[1].replace(' Ep ', ''))
                    except:
                        episode_number = None
                        season = None
                else:
                    episode_number = None
                    season = None
                description = episode.find('p').text if episode.find('p') else None
                duration = int(episode.find('div',self._duration_div).text.replace(':','')) if episode.find('div',self._duration_div) else None
                data_info = episode.find('div',self._image_div).get('data-info').split('"')
                image_src = data_info[9]
                deep_link = episode.get('href')
                payload_episodes = {
                "PlatformCode":  self._platform_code,
                "Id": hashlib.md5(title.encode('utf-8')+str(parent_title).encode('utf-8')+str(deep_link).encode('utf-8')).hexdigest(),
                "ParentId":      hashlib.md5(parent_title.encode('utf-8')).hexdigest(),
                "ParentTitle":   parent_title,
                "Episode":       episode_number,
                "Season":        season,
                'Title':         title,
                'OriginalTitle': None,
                'Year':          None,
                'Duration':      duration,
                'Deeplinks': {
                    'Web':       deep_link,
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      description,
                'Image':         [image_src],
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
                    self, payload_episodes, ids_guardados_series, payloads_series, isEpi=True
                )
        Datamanager._insertIntoDB(
            self,payloads_series , self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self, payloads, self.titanScraping)
        Upload(self._platform_code, self._created_at, testing=testing)