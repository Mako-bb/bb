import re
import json
import time
import requests
import hashlib
import platform   
from common                                         import config
from datetime                                       import datetime
from handle.mongo                                   import mongo
from updates.upload                                 import Upload
from handle.datamanager                             import Datamanager
from handle.replace                                 import _replace
from handle.payload                                 import Payload
from selenium                                       import webdriver
from selenium.webdriver.firefox.options             import Options
from selenium.webdriver.common.keys                 import Keys
from selenium.webdriver.common.action_chains        import ActionChains
from pyvirtualdisplay                               import Display
# from handle.datamanager                             import RequestsUtils

class MultimediaGo():
    """
    DSmartGo es una ott de Polonia.

    DATOS IMPORTANTES:
    - VPN: Si (PL).
    - ¿Usa Selenium?: Si (Unicamente para hacer login-logout).
    - ¿Tiene API?: Si.
    - ¿Usa BS4?: Si.
    - ¿Se relaciona con scripts TP? No.
    - ¿Instanacia otro archivo de la carpeta "platforms"?: No.
    - ¿Cuanto demoró la ultima vez? 8 min (2021-04-19)
    - ¿Cuanto contenidos trajo la ultima vez? 982 titanScraping, 114 titanScrapingEpisodes (2021-04-19)

    OTROS COMENTARIOS:
    Se realiza login e ingresa un pin de control parental para poder obtener las cookies 
    necesarias para acceder a las apis de todos los contenidos. 
    Se creó una cuenta gratuita (user y pass en config.yaml). En caso de que caduque habria que crear otra cuenta
    Al finalizar el script o si hay un problema de ejecucion se realiza un logout 
    ya que la platagorma permite unicamente 4 sesiones abiertas por cuenta

    Revisada el 07-06-2021, toma peliculas como series por que en la parte de categorias dice que es serial,
    se saco la comparacion esa 

    """
    
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
        # self.req_utils              = RequestsUtils()
        self.currency               = config()['currency'][ott_site_country]
        option = Options()
        option.add_argument('--headless')
        self.browser                = webdriver.Firefox(options=option)
        if platform.system() == 'Linux':
            Display(visible=0, size=(1366, 768)).start()

        # urls
        self.login                  = self._config['urls']['login']
        self.contenidos             = self._config['urls']['contenidos']
        self.api_contenido          = self._config['urls']['api_contenido']
        
        # user-password
        self.user                   = self._config['account']['user']
        self.password               = self._config['account']['password']

        self.regex_serie = re.compile(r'S\d+E\d+')
        self.regex_epi = re.compile(r'odc\.?\s?\d+', re.IGNORECASE)
        
        if type == 'scraping':
            try:
                self._scraping()
            except:
                self._log_in_out(logout=True)
                raise
        
        if type == 'return':
            '''
            Retorna a la Ultima Fecha
            '''

            params = {"PlatformCode" : self._platform_code}
            lastItem = self.mongo.lastCretedAt(self.titanScraping, params)
            if lastItem.count() > 0:
                for lastContent in lastItem:
                    self._created_at = lastContent['CreatedAt']
            try:
                self._scraping()
            except:
                self._log_in_out(logout=True)
                raise
        if type == 'testing':
            try:
                self._scraping(testing = True)
            except:
                self._log_in_out(logout=True)
                raise
    
    def _scraping(self, testing = False):

        cookies = self._log_in_out()
        self.headers = {'Cookie': cookies}

        listaSeriesyPelis = []
        listaSeriesyPelisDB = Datamanager._getListDB(self,self.titanScraping)
            
        listaEpi = []
        listaEpiDB = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        
        categorias = ['kids', 'list']
        
        ids = set()
        series = {}

        # obtener todos los ids
        for categoria in categorias:
            page = 1
            while True:
                url = self.contenidos.format(categoria = categoria, page = page)
                soup = Datamanager._getSoup(self, url)
                contenidos = soup.find_all('div',{'class':'col-xs-2'})
                if not contenidos:
                    break
                
                for contenido in contenidos:
                    id_ = contenido.find('div',{'class':'product-item'})['data-id']
                    ids.add(id_)
                
                page += 1
        
        print(f'{len(ids)} contenidos obtenidos')
        # print('Realizando request async!')
        list_urls = [self.api_contenido.format(id_ = id_) for id_ in ids]
        # list_responses = self.req_utils.async_requests(list_urls, headers = self.headers)
        # dict_responses = {res.url: res for res in list_responses if res}
        

        for url in list_urls:
            
            # data = dict_responses[url].json() if url in dict_responses.keys() else self._obtener_json(url)
            data = self._obtener_json(url)
            if not data:
                continue
            payload, is_epi = self._obtener_payload(data)
            if is_epi:
                print(f"\x1b[1;33;40m {payload['Id']}: {payload['Title']} es episodio\x1b[0m")
                if payload['ParentTitle'] in series.keys():
                    series[payload['ParentTitle']].append(payload)
                else:
                    series[payload['ParentTitle']] = [payload]
            else:
                Datamanager._checkDBandAppend(self, payload, listaSeriesyPelisDB, listaSeriesyPelis)
            
            if len(listaSeriesyPelis) == 20:
                Datamanager._insertIntoDB(self, listaSeriesyPelis, self.titanScraping)
        Datamanager._insertIntoDB(self, listaSeriesyPelis, self.titanScraping)

        # unifico episodios sueltos

        for serie in series:
            directors = set()
            cast = set()
            genres = set()
            images = set()
            packages = []
            epis = series[serie]
            for epi in epis:
                if epi['Directors']:
                    for director in epi['Directors']:
                        directors.add(director)
                if epi['Cast']:
                    for actor in epi['Cast']:
                        cast.add(actor)
                if epi['Packages']:
                    for package in epi['Packages']:
                        package = {'Type':package['Type']}
                    if packages == []:
                        packages.append(package)
                    elif package['Type'] not in [p['Type'] for p in packages]:
                        packages.append(package)
                if epi['Genres']:
                    for genre in epi['Genres']:
                        genres.add(genre)
                if epi['Image']:
                    for image in epi['Image']:
                        images.add(image)

            # obtengo el episodio de numero mas bajo (temporada y episodio)
            # sera el deeplink de la serie

            epis = [(epi['Season'], epi['Episode'], epi['Deeplinks']['Web']) for epi in epis]
            epis.sort()
            deeplink = epis[0][-1]
            id_ = hashlib.md5(deeplink.encode('UTF-8')).hexdigest()
            
            # update de payloads de epis
            for epi in series[serie]:
                epi['ParentId'] = id_
                Datamanager._checkDBandAppend(self, epi, listaEpiDB, listaEpi, isEpi=True)
            
            images = list(images) if images != set() else None
            genres = list(genres) if genres != set() else None
            cast = list(cast) if cast != set() else None
            directors = list(directors) if directors != set() else None

            payload = Payload(platform_code=self._platform_code, id_ = id_, title = serie, clean_title=_replace(serie), 
                                deeplink_web=deeplink, image=images, genres=genres, cast=cast, directors=directors,
                                packages= packages, createdAt=self._created_at).payload_serie()
            
            Datamanager._checkDBandAppend(self, payload, listaSeriesyPelisDB, listaSeriesyPelis)
        
        Datamanager._insertIntoDB(self, listaSeriesyPelis, self.titanScraping)
        Datamanager._insertIntoDB(self, listaEpi, self.titanScrapingEpisodios)

        self._log_in_out(logout=True)
        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)

    def _obtener_payload(self, data):

        id_ = str(data['id'])
        title = data['title']
        is_active = data['active']
        if not is_active:
            return

        rating = str(data['rating']) if 'rating' in data.keys() and data['rating'] != 0 else None
        is_hd = any('HD' in cat['name'] for cat in data['categories'])
        packages = self._obtener_packages(data['schedules'], is_hd)
        duration = data['duration']
        year = data['year']
        synopsis = self._clean_text(data['description'])
        cast = list(set([self._clean_text(actor['name'], is_name=True) for actor in data['actors'] if actor['name'] != '...']))
        cast = cast if cast != [] else None
        directors = list(set([self._clean_text(director['name'], is_name=True) for director in data['directors'] if director['name'] != '...']))
        directors = directors if directors != [] else None
        images = [img['mainUrl'] for img in data['covers']['pc']]
        images = images if images != [] else None
        genres = [self._clean_text(genre['name']) for genre in data['genres']]
        genres = genres if genres != [] else None
        countries = [self._clean_text(country['name']) for country in data['countries']]
        countries = countries if countries != [] else None
        deeplink = data['shareUrl']
        download = data['offlineAvailable']
        availability = data['schedules']['till']
        
        is_epi = False
        search_epi = self.regex_serie.search(title)
        if search_epi:
            is_epi = True
            season_num, epi_num = re.findall(r'\d+', search_epi.group())
            season_num = int(season_num)
            epi_num = int(epi_num)
            parent_title = self.regex_serie.sub('', title).strip()       
        elif any('serie' in cat['name'].lower() for cat in data['categories']) or any('serie' in genre.lower() for genre in genres):
            is_epi = True
            season_num = None
            epi_num = self.regex_epi.search(title)
            if epi_num:
                epi_num = int(re.findall(r'\d+', epi_num.group())[0])
                parent_title = self.regex_epi.sub('', title).strip()
            elif 'sprawa' in title: 
                parent_title = title.split('sprawa')[0].strip().strip(':').strip()
            else:
                is_epi = False
        
        payload = Payload(platform_code=self._platform_code, id_=id_, title=title, clean_title=_replace(title), 
                year=year, duration=duration, deeplink_web=deeplink, synopsis=synopsis, image=images, 
                rating=rating, genres=genres, cast=cast, directors=directors, availability=availability, 
                download=download, packages=packages, createdAt=self._created_at)
        if is_epi:
            payload.episode = epi_num
            payload.season = season_num
            payload.parent_title = parent_title
            payload = payload.payload_episode()
        else:
            payload = payload.payload_movie()
        
        return payload, is_epi

    def _obtener_packages(self, schedules, is_hd):

        price = schedules['price']
        price = price / 100 if price and price != 0 else None

        if price:
            packages = [{
                'Type'      : 'transaction-vod',
                'RentPrice' : price,
                'Definition': 'HD' if is_hd else 'SD',
                'Currency'  : self.currency
            }]
        else:
            packages = [{'Type' : 'subscription-vod'}]
        
        return packages

    def _obtener_json(self, url, max_retries = 5):
        retry = 0
        data = None
        while retry < max_retries:
            response = self.sesion.get(url, headers = self.headers)
            print(response.status_code, url)
            data = response.json() if response else None
            if data:
                break
            retry += 1
            print(f'Retrying {retry}/{max_retries}...')
            time.sleep(retry)
        
        return data
                
    def _clean_text(self, text, is_name=False):
        """
        Metodo para limpiar tags html. 
        Tambien si es un nombre separado por coma (Apellido, Nombre) lo ordena y elimina la coma
        Args:
            text [str]: texto a limpiar
            is_name [bool]: si es nombre se ordena en caso de que tenga una coma
        Returns:
            clean_text [str]: texto limpio
        """
        
        tags = ['\xa0','\n']
        clean_text = text
        for tag in tags:
            clean_text = clean_text.replace(tag,'').strip()
        
        if is_name:
            if ',' in clean_text:
                clean_text = clean_text.split(',')[::-1]
                clean_text = ' '.join(clean_text).strip()
        
        return clean_text

    
    def _log_in_out(self, logout=False):
        """
        Metodo para realizar login y logout utilizando selenium
        Es necesario realizar log-out ya que la web admite 4 sesiones con un mismo usuario
        El browser tiene que permanecer abierto hasta que se termine de correr el script
        """
        
        browser = self.browser
        if logout:
            print('\nRealizando logout\n')
            browser.get('https://www.multimediago.pl/subscriber/logout')
            browser.quit()
            return
        
        print('\nRealizando login\n')
        url = self.login
        browser.get(url)

        user = browser.find_element_by_xpath('//input[@id="client_number"]')
        password = browser.find_element_by_xpath('//input[@id="client_pass"]')

        user.send_keys(self.user)
        password.send_keys(self.password)

        submit = browser.find_element_by_xpath('//button[@class="btn btnBase"]')
        ActionChains(browser).move_to_element(submit).click().perform()

        while browser.current_url != 'https://www.multimediago.pl/':
            time.sleep(0.1)
        
        time.sleep(1)
        parental = browser.find_element_by_xpath('//li[@class="mmgo-parental-nav"]')
        ActionChains(browser).move_to_element(parental).click().perform()

        time.sleep(3)

        botones = browser.find_elements_by_xpath('//a[@class="btn btn-primary"]')
        boton = [boton for boton in botones if boton.text == 'Potwierdzam'][0]

        ActionChains(browser).move_to_element(boton).click().perform()

        time.sleep(3)

        input_pin = browser.find_element_by_xpath('//input[@name="pin"]')
        input_pin.send_keys('123456')

        botones = browser.find_elements_by_xpath('//a[@class="btn btn-primary"]')
        boton = [boton for boton in botones if boton.text == 'Wyślij'][0]

        ActionChains(browser).move_to_element(boton).click().perform()

        time.sleep(3)

        cookies_dict = browser.get_cookies()
        
        cookies = ''
        for cookie in cookies_dict:
            cookies += cookie['name'] + '=' + cookie['value'] + '; '
        
        if cookies != '':
            print('Login realizado correctamente\n')

        return cookies
