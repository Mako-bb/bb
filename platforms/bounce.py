# -*- coding: utf-8 -*-
import json
import time
import requests
import hashlib
from common import config
from bs4 import BeautifulSoup
from datetime import datetime
from handle.mongo import mongo
from updates.upload import Upload
from handle.datamanager import Datamanager
from handle.replace import _replace
#import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from string import digits


class BounceTV():
    '''Bounce TV es una platafora de EEUU 
        Requiere VPN ---> NO.
        API/SELENIUM/BS4 ---> SELENIUM/BS4, es imposible hacer un request simple a la pag. principal
        (no hay permisos).
        Estructura---> Show all movies y show all shows de las cuales podemos
                        extraer todos los href y visitar cada contenido
                        extrayendo data de etiquetas comunes
        Tiempo aproximado---> Al usar Selenium 15 minutos.
        Que falta: datos de las series, agregar datamanager y payloads'''

    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config = config()['ott_sites'][ott_site_uid]
        self._created_at = time.strftime("%Y-%m-%d")
        self.mongo = mongo()
        self._platform_code = self._config['countries'][ott_site_country]
        self.titanScraping = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config(
        )['mongo']['collections']['episode']
        self.sesion = requests.session()
        self.skippedEpis = 0
        self.skippedTitles = 0

        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing=True)

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

    def _scraping(self, testing=False):

        link_movies = 'https://www.bouncetv.com/movies/'

        link_series = 'https://www.bouncetv.com/shows/?show-type=streaming'

        self.get_links(link_movies, 'movies')
        self.get_links(link_series, 'series')

        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)

    def selenium_options(self):
        '''Aca se setea nuestro selenium'''

        webdriver.ChromeOptions()

        chrome_options = Options()
        # chrome_options.add_argument("--headless") #Primera vez debe estar disabled para ver velocidad scroll.
        chrome_options.add_argument("--incognito")
        driver = webdriver.Chrome(r"C:\Users\tadeo\OneDrive\Escritorio\Curso Data Analyst\Python 0\chromedriver.exe",
                                  options=chrome_options)  # PATH de chromedriver de C/U
        return driver

    def get_links(self, url, _type=None):
        '''Esta función sirve para hacer la conexión a las paginas principales mediante el driver, 
        generar un scroll hasta el final (asi se cargan todas)
        y extraer los links de cada pelicula unica para luego loopear y scrapear'''

        driver = self.selenium_options()

        driver.get(url)
        time.sleep(1)

        if _type == 'movies':

            driver.find_element_by_xpath(
                '//a[contains(@href,"#stream")]').click()

        elif _type == 'series':
            driver.find_element_by_xpath(
                '//a[contains(@href,"/shows/?show-type=streaming")]').click()

        '''Aca necesitamos generar el scroll para que aparezca todo el contenido
        creo que en un futuro quizas agreguen mas contenido por eso lo dejo'''
        iter = 1
        while True:
            scroll_height = driver.execute_script(
                "return document.documentElement.scrollHeight")  # Obtenemos el size del documento
            height = 250*iter
            # Scroll hasta el final
            driver.execute_script("window.scrollTo(0, " + str(height) + ");")
            if height > scroll_height:
                print('End of page')
                break
            time.sleep(1)
            iter += 1

        if _type == 'movies':
            list_url_movies = []

            streaming_content = driver.find_element_by_id(
                'stream').find_elements_by_tag_name('a')
            for a_tag in streaming_content:
                href = a_tag.get_attribute('href')
                img = a_tag.get_attribute('innerHTML')
                # Hay unos divs vacios que tienen de href esto, y algunas img que retornan vacio
                if href != 'https://www.bouncetv.com/movie//' and 'img' in img:
                    list_url_movies.append((href, img.strip()))

            list_final = set(list_url_movies)
            list_final_movies = [list(serie) for serie in list_final]

            self.get_info(list_final_movies, 'movies')

        elif _type == 'series':
            list_url_series = []

            streaming_content = driver.find_element_by_css_selector(
                'div.container.posterContainer').find_elements_by_tag_name('a')

            for a_tag in streaming_content:
                if a_tag.get_attribute('text'):
                    list_url_series.append((
                        a_tag.get_attribute('href'),
                        a_tag.get_attribute('text')
                    ))

            list_final = set(list_url_series)
            list_final_series = [list(serie) for serie in list_final]

            self.get_info(list_final_series, 'series')

        else:
            print('No se ha especificado el argumento movies o series a get_links()')

    def get_info(self, list_urls, _type=None):
        '''Dependiendo del argumento _type que tome, esta función, lopea 
            visitando las distintas URL's del contenido, extrayendo datos de etiquetas
            distintas para movies y series'''

        driver = self.selenium_options()

        if _type == 'movies':
            info_movies = []

            for url in list_urls:
                try:
                    driver.get(url[0])
                    time.sleep(2)

                    body = driver.execute_script("return document.body")
                    html = body.get_attribute('innerHTML')

                    soup = BeautifulSoup(html, "html.parser")

                    try:
                        # Tomamos numerosId de url
                        id_ = [int(s)
                               for s in url[0].split('/') if s.isdigit()]
                        # Cast y genres vienen juntos
                        cast_genres = soup.find_all(
                            'div', class_='singleMovieText')
                        div_of_image = soup.find(
                            'div', class_='smMoviePoster col-lg-6 col-md-6 col-sm-12')
                        data = soup.find('div', class_='singleMovieInfo').get_text().replace(
                            '\xa0', '').split('|')

                        info_movies.append([str(id_[0]),
                                            url[0],
                                            url[1].replace('<img src="', '').replace(
                                                '">', '').replace('" <="" a="', ''),
                                            soup.find(
                                                'h4', class_='singleMovieTitle').get_text(),
                                            float(data[1].replace('hr ', '.').replace('m', '')) if any(
                                                string.isdigit() for string in data[1]) else None,
                                            data[2].strip(),
                                            soup.find(
                                                'div', class_='singleMovieDescription').get_text(),
                                            cast_genres[0].get_text().replace(
                                                '\xa0', '').replace('Starring: ', ''),
                                            cast_genres[1].get_text().replace(
                                                '\xa0', '').replace('/', '')
                                            ])
                    except KeyError as e:
                        print(f'Error no se encuentra campo {e} de {url}')
                        pass
                except Exception as e:
                    print(f'Error {e}')
                    pass
            
            #self.payloads(info_movies, 'movie')

        elif _type == 'series':
            info_series = []
            info_episodes = []

            for url in list_urls:
                try:
                    driver.get(url[0])
                    time.sleep(2)

                    body = driver.execute_script("return document.body")
                    html = body.get_attribute('innerHTML')
                    soup = BeautifulSoup(html, "html.parser")

                    id_ = [int(s)
                           for s in url[0].split('/') if s.isdigit()]

                    title = url[1].replace(' EPISODES', '')
                    try:
                        info_series.append([str(id_[0]),
                                            title,
                                            url[0],
                                            soup.find(
                                                'div', id='aboutContainer').get_text().replace('\nX\n', ''),
                                            [x.get_text()
                                             for x in soup.find_all('h3')],
                                            soup.find_all(
                                                'div', class_='seasonMenu'),
                                            self.get_seasons(
                                                soup, id_[0], title),
                                            soup.find('div', class_='ssFeatureImage col-12').find('img')['src']])

                        # info_episodes.append(
                        # [x.get_text() for x in soup.find_all('div', class_='showCaptions')])

                    except KeyError as e:
                        print(
                            f'Error no se encuentra campo {e} al extraer datos de {url}')

                except Exception as e:
                    print(f'error {e}')
                    pass
            
            print(info_series)
            #self.payloads(info_series, 'serie')

        else:
            print('No se ha especificado el argumento movies o series a get_info()')

    def get_seasons(self, soup, id_, title):
        '''Obtiene conteo de seasons y capitulos
        de cada serie, recibiendo el soap como argumento.'''
        list_seasons = []
        try:
            # Si la serie tiene una temporada entonces el html no tiene un seasonMenu entonces se reemplaza con 1.
            seasons = [x.get_text().replace('Season ', '') for x in soup.find_all(
                'span', class_='seasonMenu')] if soup.find_all(
                'span', class_='seasonMenu') else [1]
            for season in seasons:
                list_seasons.append(
                    {  # Tomamos parte del payload
                        "Id": id_,
                        "Synopsis": None,
                        "Title": title + ' Season ' + str(season),
                        "Deeplink": None,
                        "Number": season,
                        "Year": None,
                        "Image": None,
                        "Directors": None,
                        "Cast": None,
                        "Episodes": len([x.get_text() for x in soup.find(
                            'div', id=f'seasonContainer{season}').find_all(
                            'div', class_='showCaptions')]),
                        "IsOriginal": None
                    })

        except Exception as e:
            print(f'Error {e}')
            pass

        return list_seasons

    def payloads(self, data, type_):
        '''Funcion payloads'''

        payloads = []

        packages = [
            {
                "Type": "subscription-vod"
            }
        ]

        if type_ == 'movie':
            list_db_movies = Datamanager._getListDB(self, self.titanScraping)
            for content in data:

                payload = {
                    'PlatformCode':  self._platform_code,
                    'Id':            content[0],
                    'Title':         content[3],
                    'OriginalTitle': None,
                    'CleanTitle':    _replace(content[3]),
                    'Type':          'movie',
                    'Year':          content[5],
                    'Duration':      int(content[4] * 60) if content[4] else None,
                    'Deeplinks': {
                        'Web':       content[1],
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      content[6],
                    'Image':         [content[2]] if content[2] else None,
                    'Rating':        None,
                    'Provider':      None,
                    'Genres':        [content[8]] if content[8] else None,
                    'Cast':          [content[7]]
                                        if content[7] else None,
                    'Directors':     None,
                    'Availability':  None,
                    'Download':      None,
                    'IsOriginal':    None,
                    'IsAdult':       None,
                    'Packages':      packages,
                    'Country':       None,
                    'Timestamp':     datetime.now().isoformat(),
                    'CreatedAt':     self._created_at
                }

                Datamanager._checkDBandAppend(self, payload, list_db_movies, payloads)
            Datamanager._insertIntoDB(self, payloads, self.titanScraping)

        elif type_ == 'serie':
            list_db_series = Datamanager._getListDB(self, self.titanScraping)
            for content in data:
                payload = {
                    'PlatformCode':  self._platform_code,
                    'Id':            content[0],
                    'Title':         content[1],
                    'OriginalTitle': None,
                    'CleanTitle':    _replace(content[1]),
                    'Type':          'serie',
                    'Year':          None,
                    'Duration':      None,
                    "Seasons":       content[6],
                    'Deeplinks': {
                        'Web':       content[2],
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      content[3],
                    'Image':         [content[7]] if content[7] else None,
                    'Rating':        None,
                    'Provider':      None,
                    'Genres':        None,
                    'Cast':          [str(content[4])]
                                        if content[4] else None,
                    'Directors':     None,
                    'Availability':  None,
                    'Download':      None,
                    'IsOriginal':    None,
                    'IsAdult':       None,
                    'Packages':      packages,
                    'Country':       None,
                    'Timestamp':     datetime.now().isoformat(),
                    'CreatedAt':     self._created_at
                }
                Datamanager._checkDBandAppend(self, payload, list_db_series, payloads)
            Datamanager._insertIntoDB(self, payloads, self.titanScraping)
        else:
            pass
