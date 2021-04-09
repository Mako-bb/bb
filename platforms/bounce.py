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

        Upload(self._platform_code, self._created_at, testing = testing)

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

            print(list_final_movies)
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
                        _id = [int(s)
                               for s in url[0].split('/') if s.isdigit()]
                        # Cast y genres vienen juntos
                        cast_genres = soup.find_all(
                            'div', class_='singleMovieText')
                        div_of_image = soup.find(
                            'div', class_='smMoviePoster col-lg-6 col-md-6 col-sm-12')
                        data = soup.find('div', class_='singleMovieInfo').get_text().replace(
                            '\xa0', '').split('|')

                        info_movies.append([int(_id[0]),
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
                                            cast_genres[1].get_text().replace('\xa0', '').replace('/', '')
                                            ])
                    except KeyError as e:
                        print(f'Error no se encuentra campo {e} de {url}')
                        pass
                except Exception as e:
                    print(f'Error {e}')
                    pass
            
            self.payloads(info_movies, 'movies')

        elif _type == 'series':
            # soup.find('div', id=lambda x: x and x.startswith('seasonContainer')
            info_series = []
            info_episodes = []

            for url in list_urls:
                try:
                    driver.get(url[0])
                    time.sleep(2)

                    body = driver.execute_script("return document.body")
                    html = body.get_attribute('innerHTML')

                    soup = BeautifulSoup(html, "html.parser")

                    try:
                        info_series.append([url[1],
                                            url[0],
                                            soup.find(
                                                'div', id='aboutContainer').get_text(),
                                            [x.get_text()
                                             for x in soup.find_all('h3')],
                                            soup.find_all(
                                                'div', class_='seasonMenu'),
                                            [x.get_text() for x in soup.find_all(
                                                'span', class_='seasonMenu')],
                                            [x.get_text() for x in soup.find('div', id='seasonContainer1').find_all(
                                                'div', class_='showCaptions')],
                                            ])

                        info_episodes.append(
                            [x.get_text() for x in soup.find_all('div', class_='showCaptions')])

                    except KeyError as e:
                        print(
                            f'Error no se encuentra campo {e} al extraer datos de {url}')

                except Exception as e:
                    print(f'error {e}')
                    pass

            print(info_episodes)

        else:
            print('No se ha especificado el argumento movies o series a get_info()')

    def payloads(self, data, type_=None):
        '''Funcion payloads'''

        payloads = []

        packages = [
            {
                "Type": "subscription-vod"
            }
        ]

        if type_ == 'movies':

            list_db_movies = Datamanager._getListDB(self, self.titanScraping)

            for movie in data:

                payload = {
                    'PlatformCode':  self._platform_code,
                    'Id':            str(movie[0]),
                    'Title':         movie[3],
                    'OriginalTitle': None,
                    'CleanTitle':    _replace(movie[3]),
                    'Type':          'movie',
                    'Year':          movie[5],
                    'Duration':      movie[4] * 60 if movie[4] else None,
                    'Deeplinks': {
                        'Web':       movie[1],
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      movie[6],
                    'Image':         [movie[2]],
                    'Rating':        None,
                    'Provider':      None,
                    'Genres':        [movie[8]] if movie [8] else None,
                    'Cast':          [movie[7]]
                                    if movie[7] else None,
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
        
        else:
            pass

    def get_response(self, url):  # Sin uso todavia
        '''Esta función intenta hacer la conexión mediante request simple. 
        Si el request es imposible, arrojara error e intentara por el driver.'''

        r = self.sesion.get(url)

        if r.status_code == 200:
            self.get_links(r)

        elif r.status_code == 301:
            print(
                f'Error {r.status_code}, el servidor esta tratando de redirigirte hacia otra URL. Quizas haya cambiado de dominio.')

        elif r.status_code == 400:
            print(
                f'Error {r.status_code}, request erroneo, por favor verificar sintaxis y url escrita.')

        elif r.status_code == 401:
            print(
                f'Error {r.status_code}, autenticación incorrecta, verifica credenciales o token.')

        elif r.status_code == 403:
            print(
                f'Error {r.status_code}, no tienes permiso para ver el/los recurso/s.')

        elif r.status_code == 404:
            print(f'Error {r.status_code}, recurso no encontrado.')

        elif r.status_code == 503:
            print(
                f'Error {r.status_code}, el servidor no pudo procesar la petición.')

        else:
            print(f'Error {r.status_code}.')
