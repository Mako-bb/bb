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
        self._config                = config()['ott_sites'][ott_site_uid]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self._platform_code         = self._config['countries'][ott_site_country]   
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()
        self.skippedEpis            = 0
        self.skippedTitles          = 0

        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing=True)
        
        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''

            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
                    
            self._scraping()

        
    def _scraping(self, testing=False):

        link_movies = 'https://www.bouncetv.com/movies/'

        link_series = 'https://www.bouncetv.com/shows/?show-type=streaming'

        self.get_links(link_movies, 'movies')
        #self.get_links(link_series, 'series')

        self.sesion.close()

        #Upload(self._platform_code, self._created_at, testing = testing)

    def selenium_options(self):
        '''Aca se setea nuestor selenium'''

        webdriver.ChromeOptions()

        chrome_options = Options()
        #chrome_options.add_argument("--headless") #Primera vez debe estar disabled para ver velocidad scroll.
        chrome_options.add_argument("--incognito")
        driver = webdriver.Chrome(r"C:\Users\tadeo\OneDrive\Escritorio\Curso Data Analyst\Python 0\chromedriver.exe", 
                                                options=chrome_options) #PATH de chromedriver de C7U
        return driver
        

    def get_links(self, url, _type=None):
        '''Esta función sirve para hacer la conexión a las paginas principales mediante el driver, 
        generar un scroll hasta el final (asi se cargan todas)
        y extraer los links de cada pelicula unica para luego loopear y scrapear'''

        
        driver = self.selenium_options()

        driver.get(url)
        time.sleep(20)

        #Aca necesitamos generar el scroll para que aparezca todo el contenido
        iter=1
        while True:
                scroll_height = driver.execute_script("return document.documentElement.scrollHeight") #Obtenemos el size del documento
                height = 250*iter
                driver.execute_script("window.scrollTo(0, " + str(height) + ");") #Scroll hasta el final
                if height > scroll_height:
                    print('End of page')
                    break
                time.sleep(1)
                iter+=1
            
            
        body = driver.execute_script("return document.body")
        html = body.get_attribute('innerHTML')
            
        soup = BeautifulSoup(html, "html.parser")

        
        if _type == 'movies':
            list_url_movies = []

            for div in soup.find_all('div', class_='row posterRow'):
                for link in div.find_all('a'):
                    list_url_movies.append('https://www.bouncetv.com' + link['href'])

            list_final = set(list_url_movies)
            
            self.get_info(list_final, 'movies')
        
        elif _type == 'series':
            list_url_series = []

            for link in soup.find_all('div', class_='showTagline'):
                a = link.find('a')
                list_url_series.append('https://www.bouncetv.com' + a['href'])
            
            list_final = set(list_url_series)

            self.get_info(list_final, 'series')

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
                    driver.get(url)
                    time.sleep(20)

                    body = driver.execute_script("return document.body")
                    html = body.get_attribute('innerHTML')
                    
                    soup = BeautifulSoup(html, "html.parser")

                    try: 
                        _id = [int(s) for s in url.split('/') if s.isdigit()] #Tomamos numerosId de url 
                        cast_genres = soup.find_all('div', class_='singleMovieText') #Cast y genres vienen juntos
                        div_of_image = soup.find('div', class_='smMoviePoster col-lg-6 col-md-6 col-sm-12')
                        data = soup.find('div', class_='singleMovieInfo').get_text().replace('\xa0', '').split('|')

                        info_movies.append([int(_id[0]),
                                            soup.find('h4', class_='singleMovieTitle').get_text(),
                                            data[0].strip() if len(data) == 0 else 'None',
                                            data[1].strip() if len(data) == 1 else 'None',
                                            data[2].strip() if len(data) == 2 else 'None',
                                            soup.find('div', class_='singleMovieDescription').get_text(),
                                            div_of_image.find('img')['src'] if div_of_image else None,
                                            cast_genres[0].get_text().replace('\xa0', '').replace('Starring: ', ''),
                                            cast_genres[1].get_text().replace('\xa0', '').replace('/', '')])
                    except KeyError as e:
                        print(f'Error no se encuentra campo {e} de {url}')
                        choice = str(input('Desea seguir colectanto información? (y/n): ')).strip().lower()
                        if choice == 'y':
                            pass
                        elif choice == 'n':
                            break
                        else:
                            print('Seleccion opción correcta (y/n)')
                except Exception as e:
                    print(f'Error {e}')
                    pass
            
            print(info_movies)
        
        elif _type == 'series':
            info_series = []

            for url in list_urls:
                try:
                    driver.get(url)
                    time.sleep(20)

                    body = driver.execute_script("return document.body")
                    html = body.get_attribute('innerHTML')
                    
                    soup = BeautifulSoup(html, "html.parser")

                    try:
                        info_series.append([url,
                                            soup.find('div', id='aboutContainer'),
                                            soup.find_all('div', class_='col-md-7 col-lg-7 col-sm-12 castContentDiv'),
                                            soup.find_all('div', class_='seasonMenu')])
                    except KeyError as e:
                        print(f'Error no se encuentra campo {e} al extraer datos de {url}')
                        choice = str(input('Desea seguir colectanto información? (y/n): ')).strip().lower()

                        if choice == 'y':
                            pass
                        elif choice == 'n':
                            break
                        else:
                            print('Seleccion opción correcta (y/n)')

                except Exception as e:
                    print(f'error {e}')
                    pass

            print(info_series)  
        

        else:
            print('No se ha especificado el argumento movies o series a get_info()')



    def get_response(self, url): #Sin uso todavia

        '''Esta función intenta hacer la conexión mediante request simple. 
        Si el request es imposible, arrojara error e intentara por el driver.'''
        
        r = self.sesion.get(url)
        
        if r.status_code == 200:
            self.get_links(r)

        elif r.status_code == 301:
            print(f'Error {r.status_code}, el servidor esta tratando de redirigirte hacia otra URL. Quizas haya cambiado de dominio.')
            
        elif r.status_code == 400:
            print(f'Error {r.status_code}, request erroneo, por favor verificar sintaxis y url escrita.')
            
        elif r.status_code == 401:
            print(f'Error {r.status_code}, autenticación incorrecta, verifica credenciales o token.')
            
        elif r.status_code == 403:
            print(f'Error {r.status_code}, no tienes permiso para ver el/los recurso/s.')
            
        elif r.status_code == 404:
            print(f'Error {r.status_code}, recurso no encontrado.')

        elif r.status_code == 503:
            print(f'Error {r.status_code}, el servidor no pudo procesar la petición.')

        else:
            print(f'Error {r.status_code}.')