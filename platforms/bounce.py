# -*- coding: utf-8 -*-
import json
import time
import requests
import hashlib
from common import config
from bs4 import BeautifulSoup
#from HTMLParser import HTMLParseError
from datetime import datetime
from handle.mongo import mongo
from updates.upload import Upload
from handle.datamanager import Datamanager
from handle.replace import _replace
#import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
import re


class BounceTV():
    '''Bounce TV es una platafora de EEUU 
        Requiere VPN ---> NO.
        API/SELENIUM/BS4 ---> SELENIUM/BS4, es imposible hacer un request simple a la pag. principal
        (no hay permisos). Luego en episodes encontre una mini API por id de cada epi.
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
        self.movies_url = self._config['url_movies']
        self.series_url = self._config['url_series']
        self.api = self._config['api']

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

        self.get_links(self.movies_url, 'movies')
        self.get_links(self.series_url, 'series')

        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)

    def selenium_options(self):
        '''Aca se setea nuestro selenium'''

        webdriver.ChromeOptions()

        chrome_options = Options()
        # chrome_options.add_argument("--headless") #Primera vez debe estar disabled para ver velocidad scroll.
        chrome_options.add_argument("--incognito")
        driver = webdriver.Chrome(r".\drivers\chromedriver.exe",
                                  options=chrome_options)  # PATH de chromedriver de C/U
        return driver
    
    def selenium_html(self, url):
        '''Aca se prueba la conexion y se devuelve el html'''

        with self.selenium_options() as driver:
            try:
                driver.get(url)
                time.sleep(2)
                body = driver.execute_script("return document.body")
                html = body.get_attribute('innerHTML')
                soup = BeautifulSoup(html, "html.parser")
                
                return soup

            except TimeoutException as e:
                print(f'Error {e}, la pagina {driver.current_url} no responde.')
            
            except StaleElementReferenceException as e:
                print(f'Error {e}, no se cargaron completamente los elementos.')
        

    def get_links(self, url, _type=None):
        '''Esta función sirve para hacer la conexión a las paginas principales mediante el driver, 
        generar un scroll hasta el final (asi se cargan todas)
        y extraer los links de cada pelicula unica para luego loopear y scrapear'''

        driver = self.selenium_options()

        try:
            driver.get(url)
            time.sleep(1)

            if _type == 'movies':

                driver.find_element_by_xpath(
                    '//a[contains(@href,"#stream")]').click()

            elif _type == 'series':
                driver.find_element_by_xpath(
                    '//a[contains(@href,"/shows/?show-type=streaming")]').click()
        
        except TimeoutException as e:
            print(f'Error {e}, la pagina {driver.current_url} no responde.')
        
        except NoSuchElementException as e:
            print('Error {e}, no se encuentran los componentes principales'
                    'en el menu superior.')

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


    def get_info(self, list_urls, _type=None):
        '''Dependiendo del argumento _type que tome, esta función, lopea 
            visitando las distintas URL's del contenido, extrayendo datos de etiquetas
            distintas para movies y series'''

        if _type == 'movies':
            info_movies = []

            for url in list_urls:
                try:
                    soup = self.selenium_html(url[0])

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

                        info_movies.append([str(id_[0]), #url[0] es la url, url[1] la imagen.
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

            self.payloads(info_movies, 'movie')

        elif _type == 'series':
            info_series = []
            soups = []

            for url in list_urls:
                try:
                    soup = self.selenium_html(url[0])

                    id_ = [int(s)
                           for s in url[0].split('/') if s.isdigit()]
                    
                    #Los titles vienen con otra info, la cual sacamos con el modulo re y replace.
                    title = re.sub(r'\d+', '', url[1].replace(' EPISODES', '')) 
                    synopsis = soup.find('div', id='aboutContainer')
                    #Algunos nombres viene formato Nombre Actor - Personaje
                    cast = [x.get_text().split(' -')[0] for x in soup.find_all('h3')] if soup.find_all(
                        'h3') else None
                    try:
                        info_series.append([str(id_[0]),
                                            title,
                                            url[0],
                                            synopsis.get_text().replace(
                                                '\nX\n        ', '').replace('    ', '').replace(
                                                '\n', '') if synopsis else None,
                                            ', '.join([str(name) for name in cast]) if cast else None,
                                            self.get_seasons(soup, id_[0], title), #Funcion aparte de seasons
                                            soup.find('div', class_='ssFeatureImage col-12').find('img')['src']])

                        soups.append([soup, str(id_[0]), title, url[0]])

                    except KeyError as e:
                        print(
                            f'Error no se encuentra campo {e} al extraer datos de {url}')

                except Exception as e:
                    print(f'error {e}')
                    pass
            
            self.payloads(info_series, 'serie')
            self.get_episodes(soups)


    def get_seasons(self, soup, id_, title):
        '''Obtiene conteo de seasons y capitulos
        de cada serie, recibiendo el soap como argumento.'''

        list_seasons = []
        try:
            '''Si la serie tiene una temporada entonces el html no tiene un seasonMenu 
            entonces se reemplaza con 1.'''
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

        if not list_seasons:
            return None
        else:
            return list_seasons
    
    def get_episodes(self, soups):
        '''Esta funcion recibe como parametros los soup
        y recolecta los id de los episodios
        para luego hacer request a la API'''

        episodes_final = []
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                'AppleWebKit/537.36 (KHTML, like Gecko)'
                'Chrome/89.0.4389.114 Safari/537.36'} #Permiso

        for soup in soups:
            #Modulo re nos permite encontrar todos los div que muestrene episodes.
            episodes = soup[0].find('div', id='episodeContainer').find_all(
                'div', class_=re.compile('^showEpisodeCol*'))
            for epi in episodes:
                div = epi.find('div')
                if div != None:
                    parameter = div['id']
                    r = self.sesion.get(self.api+parameter, 
                            headers=headers)
                    if r.status_code == 200:
                        data = r.json()
                        try:
                            for json in data:
                                image = json['images']['thumb']
                                episodes_final.append([
                                    soup[1], #parent id
                                    soup[2], #parent title
                                    json.get('id', None),
                                    json.get('title', None),
                                    json.get('cast', None),
                                    json.get('yearReleased', None),
                                    json.get('description', None),
                                    json.get('rating', None),
                                    json.get('seasonNumber', None),
                                    json.get('episodeNumber', None),
                                    json.get('duration', None),
                                    image.get('url', None),
                                    soup[3] #url
                                ])
                        except KeyError as e:
                            print(f'Error en campo{e}.')
                    else:
                        print(f'Error al conectarse a la url, codigo {r.status_code}')
                        pass
        
        
        self.payloads(episodes_final, 'episode')
                    

    def payloads(self, data, type_):
        '''Funcion payloads'''

        payloads = []

        packages = [
            {
                "Type": "tv-everywhere"
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
                    "Seasons":       None,
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

                Datamanager._checkDBandAppend(
                    self, payload, list_db_movies, payloads)
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
                    "Seasons":       content[5],
                    'Deeplinks': {
                        'Web':       content[2],
                        'Android':   None,
                        'iOS':       None,
                    },
                    'Playback':      None,
                    'Synopsis':      content[3],
                    'Image':         [content[6]] if content[6] else None,
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
                Datamanager._checkDBandAppend(
                    self, payload, list_db_series, payloads)
            Datamanager._insertIntoDB(self, payloads, self.titanScraping)

        else:
            list_db_episodes = Datamanager._getListDB(
                self, self.titanScrapingEpisodios)

            for epi in data:

                payload_epi = {
                    "PlatformCode":  self._platform_code,
                    "Id":            str(epi[2]) if epi[2] else None,
                    "ParentId":      epi[0],
                    "ParentTitle":   epi[1],
                    "Episode":       int(epi[9]) if epi[9] else None,
                    "Season":        int(epi[8]) if epi[8] else None,
                    "Title":         epi[3].replace("â\x80\x99", "'") if epi[3] else None,
                    "CleanTitle":    _replace(epi[3]) if epi[3] else None,
                    "OriginalTitle": epi[3],
                    "Type":          'episode',
                    "Year":          int(epi[5]) if epi[5] >= 1700 and epi[5] <= 2021 else None,
                    "Duration":      int(epi[10] * 60) if epi[10] else None,
                    "ExternalIds":   None,
                    "Deeplinks": {
                        "Web":       epi[12] +'/' + str(epi[2]),
                        "Android":   None,
                        "iOS":       None,
                    },
                    "Synopsis":      epi[6].replace("â", "").replace(
                                    "âs", "'s").replace('\'', "'").replace(
                                    ' â\x80\x9cwokeâ\x80\x9d', '').replace(
                                    'Ã©', 'e').replace('\r\n', '') if epi[6] else None,
                    "Image":         [epi[11]] if epi[11] else None,
                    "Rating":        epi[7],
                    "Provider":      None,
                    "Genres":        None,
                    "Cast":          [epi[4]] if epi[4] else None,
                    "Directors":     None,
                    "Availability":  None,
                    "Download":      None,
                    "IsOriginal":    None,
                    "IsAdult":       None,
                    "IsBranded":     None,
                    "Packages":      packages,
                    "Country":       None,
                    "Timestamp":     datetime.now().isoformat(),
                    "CreatedAt":     self._created_at,
                }

                Datamanager._checkDBandAppend(
                    self, payload_epi, list_db_episodes, payloads)

            Datamanager._insertIntoDB(
                self, payloads, self.titanScrapingEpisodios)
