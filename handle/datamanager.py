# -*- coding: utf-8 -*-
import json
import time
import logging
import requests
import hashlib
import platform
import threading
from requests.adapters      import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pprint                 import pformat, pprint
from concurrent.futures     import ThreadPoolExecutor, as_completed
from pyvirtualdisplay       import Display
from common                 import config
from bs4                    import BeautifulSoup
from datetime               import datetime
from handle.mongo           import mongo
from updates.upload         import Upload
from selenium               import webdriver
from datetime               import datetime, date
from handle.replace         import _replace
from selenium.webdriver import ActionChains



# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)

class Datamanager():
    '''
    Este script es un set de metodos que yo uso en todas mis plataformas (Juanma) si tienen alguna duda sobre esto me pueden preguntar.
    Para usarlo solamente se tiene que importar con:
        - from handle.datamanager import Datamanager
    '''
    def __init__(self):
        self.mongo                  = mongo()
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode']
        self.sesion                 = requests.session()

    def _getListDB(self, DB):
        '''
        Hace una query al mongo local con el codigo de plataforma y devuelve una lista de contenidos de esa plataforma
        '''
        if DB == self.titanScraping:
            listDB = self.mongo.db[DB].find({'PlatformCode': self._platform_code, 'CreatedAt': self._created_at}, projection={'_id': 0, 'Id': 1, 'Title': 1})
        else:
            listDB = self.mongo.db[DB].find({'PlatformCode': self._platform_code, 'CreatedAt': self._created_at}, projection={'_id': 0, 'Id': 1, 'Title': 1, 'ParentId' : 1})
        listDB = list(listDB)

        return listDB

    def _checkDBandAppend(self, payload, listDB, listPayload, currentItem=0, totalItems=0, isEpi=False, hasDuplicates=False):
        '''
        - Recibe:
            - El payload del contenido a insertar
            - La lista del mongo local, que se deberia haber sacado antes con _getListDB()
            - La lista de payloads del script
            - Si se tiene la cantidad de contenidos que tiene la plataforma de antemano, usar currentItem y totalItems (opcional)
            - isEpi determina si el payload es de contenido o de episodios
            - hasDuplicates es una opcion extra que se puede agregar si la plataforma esta dando error de duplicados en los episodios. Hace que los IDs se chequeen directamente
            y no como lo hace por defecto, que mira duplicados solo dentro del ParentId.

        Luego de chequear si ya existe el contenido en la lista de mongo, usa el metodo noInserta() o inserta()
        '''
        if isEpi:
            if hasDuplicates: #Esta opcion hace que solo chequee IDs sin mirar el parent, para evitar
                              #duplicacion de IDs entre distintos ParentId
                if any((payload['Id'] == d['Id']) for d in listDB):
                    Datamanager.noInserta(self,payload,listDB,listPayload,currentItem,totalItems,isEpi)
                else:
                    Datamanager.insertar(self,payload,listDB,listPayload,currentItem,totalItems,isEpi)
            else:
                if any((payload['Id'] == d['Id'] and payload['ParentId'] == d['ParentId']) for d in listDB):
                    Datamanager.noInserta(self,payload,listDB,listPayload,currentItem,totalItems,isEpi)
                else:
                    Datamanager.insertar(self,payload,listDB,listPayload,currentItem,totalItems,isEpi)
        else:
            if any(payload['Id'] == d['Id'] for d in listDB):
                Datamanager.noInserta(self,payload,listDB,listPayload,currentItem,totalItems,isEpi)
            else:
                Datamanager.insertar(self,payload,listDB,listPayload,currentItem,totalItems,isEpi)


    def noInserta(self, payload, listDB, listPayload, currentItem=0, totalItems=0, isEpi=False):
        '''
        Este metodo se usa solo en _checkDBandAppend() NO usar en scripts!!!
        Solamente es lo que determina que se imprime en el caso que no se inserte un contenido
        '''
        if isEpi:
            print("\x1b[1;31;40m     EXISTE EPISODIO \x1b[0m {}x{} --> {} : {} : {}".format(payload['Season'],payload['Episode'],payload['ParentId'],payload['Id'],payload['Title']))
        else:
            if currentItem == 0 and totalItems == 0:
                if payload['Type'] == 'movie':
                    print("\x1b[1;31;40m EXISTE PELICULA \x1b[0m {} : {}".format(payload['Id'],payload['CleanTitle']))
                else:
                    print()
                    print("\x1b[1;31;40m EXISTE SERIE \x1b[0m {} : {}".format(payload['Id'],payload['CleanTitle']))
            else:
                if payload['Type'] == 'movie':
                    print("\x1b[1;31;40m {}/{} EXISTE PELICULA \x1b[0m {} : {}".format(currentItem,totalItems,payload['Id'],payload['CleanTitle']))
                else:
                    print()
                    print("\x1b[1;31;40m {}/{} EXISTE SERIE \x1b[0m {} : {}".format(currentItem,totalItems,payload['Id'],payload['CleanTitle']))

    def insertar(self, payload, listDB, listPayload, currentItem=0, totalItems=0, isEpi=False):
        '''
        Este metodo se usa solo en _checkDBandAppend() NO usar en scripts!!!
        Checkea que el payload este bien con _dataChecker(), si esta bien lo inserta y si no, lo salta y aumenta el contador skippedTitles o skippedEpis
        '''
        noInsertar = Datamanager._dataChecker(self,payload,isEpi)
        if noInsertar == False:
            listPayload.append(payload)
            listDB.append(payload)
            if isEpi:
                print("\x1b[1;32;40m     INSERT EPISODIO \x1b[0m {}x{} --> {} : {} : {}".format(payload['Season'],payload['Episode'],payload['ParentId'],payload['Id'],payload['Title']))
            else:
                if currentItem == 0 and totalItems == 0:
                    if payload['Type'] == 'movie':
                        print("\x1b[1;32;40m INSERT PELICULA \x1b[0m {} : {}".format(payload['Id'],payload['CleanTitle']))
                    else:
                        print()
                        print("\x1b[1;32;40m INSERT SERIE \x1b[0m {} : {}".format(payload['Id'],payload['CleanTitle']))
                else:
                    if payload['Type'] == 'movie':
                        print("\x1b[1;32;40m {}/{} INSERT PELICULA \x1b[0m {} : {}".format(currentItem,totalItems,payload['Id'],payload['CleanTitle']))
                    else:
                        print()
                        print("\x1b[1;32;40m {}/{} INSERT SERIE \x1b[0m {} : {}".format(currentItem,totalItems,payload['Id'],payload['CleanTitle']))
        else:
            if isEpi:
                self.skippedEpis += 1
            else:
                self.skippedTitles += 1

    def _dataChecker(self, payload, isEpi):
        '''
        NO usar este metodo en scripts!!!
        Checkea la data de los payloads y la deja en None si esta mal y no es necesario o retorna noInsertar = True si hay que saltarlo.
        '''
        noInsertar = False
        if payload['Year'] != None:
            if isinstance(payload['Year'], int):
                if payload['Year'] > int(time.strftime("%Y")) or payload['Year'] < 1870:
                    payload['Year'] = None
                    print("\x1b[1;31;40m !!!AÑO INCORRECTO REEMPLAZADO POR NONE!!! \x1b[0m")
            else:
                try:
                    payload['Year'] = int(payload['Year'])
                    if payload['Year'] > int(time.strftime("%Y")) or payload['Year'] < 1870:
                        payload['Year'] = None
                        print("\x1b[1;31;40m !!!AÑO INCORRECTO REEMPLAZADO POR NONE!!! \x1b[0m")
                except:
                    payload['Year'] = None
                    print("\x1b[1;31;40m !!!AÑO TIPO INCORRECTO REEMPLAZADO POR NONE!!! \x1b[0m")

        if payload['Title'] == None or payload['Title'] == '' or payload['Title'] == "":
            noInsertar = True
            print("\x1b[1;31;40m !!!TITULO VACIO!!! Skipping... \x1b[0m")

        if payload['Packages'] == None or payload['Packages'] == '' or payload['Packages'] == "":
            noInsertar = True
            print("\x1b[1;31;40m !!!PACKAGES VACIOS!!! Skipping... \x1b[0m")

        if payload['Synopsis'] == "":
            payload['Synopsis'] = None

        if payload['Rating'] == "":
            payload['Rating'] = None

        if payload['Duration'] == 0 or payload['Duration'] == "":
            payload['Duration'] = None

        if payload['Availability'] == "":
            payload['Availability'] = None

        if isEpi == False:
            try:
                hola = payload['CleanTitle']

                if payload['CleanTitle'] == "":
                    noInsertar = True
                    print("\x1b[1;31;40m !!!CLEANTITLE NO TIENE NADA!!! Skipping... \x1b[0m")
            except:
                noInsertar = True
                print("\x1b[1;31;40m !!!CLEANTITLE NO EXISTE!!! Skipping... \x1b[0m")
        else:
            if payload['Episode'] == 0 or payload['Episode'] == "":
                payload['Episode'] = None

            if payload['Season'] == "":
                payload['Season'] = None

        return noInsertar

    def _checkDBContentID(self, ID, listDB, currentItem=0, totalItems=0):
        isPresent = False
        if any(ID == d['Id'] for d in listDB):
            if currentItem == 0 and totalItems == 0:
                print("\x1b[1;31;40m EXISTE \x1b[0m {}".format(ID))
            else:
                print("\x1b[1;31;40m {}/{} EXISTE \x1b[0m {}".format(currentItem,totalItems,ID))
            isPresent = True
        else:
            isPresent = False

        return isPresent

    def _checkDBContentTitle(self, Title, listDB, currentItem=0, totalItems=0):
        isPresent = False
        if any(Title == d['Title'] for d in listDB):
            if currentItem == 0 and totalItems == 0:
                print("EXISTE {}".format(Title))
            else:
                print("{}/{} EXISTE {}".format(currentItem,totalItems,Title))
            isPresent = True
        else:
            isPresent = False

        return isPresent

    def _checkIfKeyExists(data, key):
        existe = False
        try:
            hola = data[key]
            existe = True
        except:
            existe = False

        return existe

    def _insertIntoDB(self, listPayload, DB):
        '''
        Recibe el DB y la lista de payloads correspondiente para insertarlo en el mongo local.
        '''
        if len(listPayload) != 0:
            self.mongo.insertMany(DB, listPayload)
            if DB == self.titanScraping:
                print("\x1b[1;33;40m INSERTADAS {} PELICULAS/SERIES \x1b[0m".format(len(listPayload)))
                print("\x1b[1;33;40m SKIPPED {} PELICULAS/SERIES \x1b[0m".format(self.skippedTitles))
                listPayload.clear()
            elif DB == self.titanScrapingEpisodios:
                print("\x1b[1;33;40m INSERTADOS {} EPISODIOS \x1b[0m".format(len(listPayload)))
                print("\x1b[1;33;40m SKIPPED {} EPISODIOS \x1b[0m".format(self.skippedEpis))
                listPayload.clear()
            else:
                print("\x1b[1;33;40m INSERTADAS {} ENTRADAS \x1b[0m".format(len(listPayload)))
                listPayload.clear()

    def _getSoup(self, URL, headers={}, showURL=True, timeOut=0):
        '''
        Devuelve el beautifulsoup del URL solicitado, si es necesario se le puede pasar headers.
        '''
        if showURL == True:
            print("\x1b[1;33;40m INTENTANDO PAGINA ----> \x1b[0m"+URL)
        time.sleep(timeOut)
        content = self.sesion.get(URL,headers=headers)
        soup = BeautifulSoup(content.text, features="html.parser")
        return soup

    def _getJSON(self, URL, headers=None, data=None, json=None, showURL=True, usePOST=False, timeOut=0, params=None):
        '''
        Devuelve un JSON con el contenido de la URL solicitada, se le pueden pasar headers o payloads si es necesario
        usePOST determina si se va a usar POST para la request, por defecto False.
        '''
        tryNumber = 0
        while tryNumber <= 10:
            if tryNumber == 10:
                print("\x1b[1;37;41m Too many tries... Give up\x1b[0m")
                jsonData = None
                break
            try:
                if tryNumber > 0 and timeOut > 0:
                    print("\x1b[1;33;40m Esperando para intentar de nuevo... ({} seg) ----> \x1b[0m{}".format(timeOut,URL))
                time.sleep(timeOut)

                if usePOST:
                    content = self.sesion.post(URL,headers=headers,data=data,json=json)
                else:
                    content = self.sesion.get(URL,headers=headers, params=params)

                if showURL == True:
                    print("\x1b[1;33;40m STATUS {} URL: \x1b[0m{}".format(content.status_code,URL))

                jsonData = content.json()
                tryNumber = 11

            except requests.exceptions.ConnectionError as e:
                print(repr(e))
                print("\x1b[1;37;41m",URL,"\x1b[0m")
                print("\x1b[1;37;41m La conexion fallo, reintentando... (Intento #{})\x1b[0m".format(tryNumber))

                tryNumber += 1
                timeOut = tryNumber * 10 #aumenta el timeOut a medida que mas tries hace, maximo 1:40 mins
            except Exception as e:
                print(repr(e))
                # print(content.text)
                print("\x1b[1;37;41m",URL,"\x1b[0m")
                print("\x1b[1;37;41m El JSON fallo, reintentando... (Intento #{})\x1b[0m".format(tryNumber))
                tryNumber += 1
        return jsonData

    def _getSoupSelenium(self,URL,waitTime=0,showURL=True):
        '''
        Devuelve un beautifulsoup usando selenium
        '''
        os = platform.system()
        if showURL == True:
            print("\x1b[1;33;40m INTENTANDO PAGINA ----> \x1b[0m"+URL)
        # if os == 'Linux':
        #     display    = Display(visible=0, size=(1366, 768)).start()

        # driver = webdriver.Firefox()
        self.driver.get(URL)
        time.sleep(waitTime)
        soup = BeautifulSoup(self.driver.page_source, features="html.parser")
        # driver.close()
        return soup

    def Upload_Other_Countries(self, last_platform_code, _created_at,
                               titanScraping, titanScrapingEpisodes,
                               other_countries, testing=False,
                               specific=None, filter_=None):
        """Método que modifica el PlatformCode, por los países que le
        indiquemos y luego guarda en Misato(si testing=False) los mismos
        payloads y episodios, pero con platformCode distinto.

        Args:
            titanScraping (str): Indica nombre de la BBDD.
            titanScrapingEpisodes (str): Indica nombre de la BBDD.
            other_countries (dict): Diccionario en donde indican los
            PlatformCodes a ingresar. TOMAR DEL CONFIG.YAML .
            testing (bool, optional): Indica si está en modo testing.
            Defaults to False.
            specific (list, optional): Lista que indica países específicos
            a scrapear del diccionario other_countries. Defaults to None.
            filter_ (list, optional): Lista que indica países a no tener
            en cuenta del diccionario other_countries. Defaults to None.
        """
        platform_code_inserted = last_platform_code
        for country in other_countries:
            query = {
                "PlatformCode": last_platform_code,
                "CreatedAt": _created_at,
                }
            platform_code = other_countries[country]

            if platform_code == platform_code_inserted:
                continue
            #######################################
            # Si hay países específicos o filtros #
            #######################################
            if specific and filter_:
                print("\nSolo puede haber un SPECIFIC o un FILTER\n")
                break
            if specific:
                if not country in specific:
                    continue
            if filter_:
                if country in filter_:
                    continue

            print(f"\n UPLOAD - Actualizando a -> {platform_code}\n")
            new_value = {"$set":{"PlatformCode": platform_code}}
            self.mongo.gralUpdate(titanScraping,
                                    query,
                                    new_value)

            new_value = {"$set":{"PlatformCode": platform_code}}
            self.mongo.gralUpdate(titanScrapingEpisodes,
                                    query,
                                    new_value)
            print(f"\n {platform_code} -> Actualizado payload y episodios\n")
            Upload(platform_code, _created_at, testing=testing)
            last_platform_code = platform_code

    def _clickAndGetSoupSelenium(self,URL,botton,waitTime=0,showURL=True):
        '''
        Primero hace un click y despues devuelve un BeatufilSoup de la pagina cargada por el click. El botton
        tiene que ser el nombre de la clase de la pagina html (<XXX class="Este seria el botton parametro">)
        '''
        os = platform.system()
        if showURL == True:
            print("\x1b[1;33;40m INTENTANDO PAGINA ----> \x1b[0m"+URL)
        # if os == 'Linux':
        #     display    = Display(visible=0, size=(1366, 768)).start()

        # driver = webdriver.Firefox()
        self.driver.get(URL)
        time.sleep(waitTime)
        try:
            click = self.driver.find_element_by_class_name("page-overlay_close")
            ActionChains(self.driver).move_to_element(click).click().perform()
            time.sleep(waitTime)
        finally:
            click = self.driver.find_elements_by_class_name(botton)[0]
            ActionChains(self.driver).move_to_element(click)
            time.sleep(waitTime)
            click.click()
            time.sleep(waitTime)

            soup = BeautifulSoup(self.driver.page_source, features="html.parser")
            # self.driver.close()
            return soup

    def threadedFunction(func, func_args, workers=1, join=True, return_threads=False):

        """
        easy setup for executing a function on multiple threads.

        workers = <int> quantity of threads to instantiate. hard cap is at 3

        func = <function> the function to be called

        func_args = <tuple> a tuple of arguments to be passed to the function.
        #for multiple arguments: (arg, arg, arg......)
        #for a single argument, a comma must be added after the argument so the program takes it as a tuple: (arg,)

        join = <bool> whether or not to wait for all threads to finish before main thread continues running

        return_threads = <bool> with this on, the function will only instantiate the threads and return them in a dict.
        set this to True if you only want the threads to be created and want more control over them.
        """

        workers = min(workers, 3)
        threads = dict()

        print(f"creating {workers} threads...")

        for num in range(workers):
            threads[num] = threading.Thread(target=func, args=func_args)

        if return_threads:
            return threads
        else:
            for key in threads:
                threads[key].start()

            if join:
                for key in threads:
                    threads[key].join()

            del threads


class RequestsUtils():
    """Clase utilizada para realizar la mayoría de las peticiones normales y/o peticiones asíncronas

    Example:
        >>> from handle.datamanager import RequestsUtils
        >>> requests_utils = RequestsUtils()
        >>> res = requests_utils.obtain_response("url")

    Notas:
    - Cuando el script principal finaliza la instancia creada de esta clase en el script llama automáticamente a su destructor para cerrar la conexión.
    - Se puede cerrar manualmente la conexión si se desea de cualquiera de las sig. maneras:
        >>> req_utils.close_session()
        >>> del req_utils
    """

    def __init__(self, enable_logging=True, disable_warnings=False, set_value_ssl=False, create_session=True):
        logging.basicConfig(level=logging.DEBUG)
        self.requests_logger = logging.getLogger('requests')
        if not enable_logging:
            self.requests_logger.setLevel(logging.CRITICAL)
        if create_session:
            self.session = self._create_new_session(disable_warnings, set_value_ssl)

    # Soluciona problemas de SSL en algunas plataformas al realizar requests
    def __increase_default_value_ssl(self):
        requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += 'HIGH:!DH:!aNULL'
        try:
            requests.packages.urllib3.contrib.pyopenssl.DEFAULT_SSL_CIPHER_LIST += 'HIGH:!DH:!aNULL'
        except AttributeError:
            pass

    def _create_new_session(self, disable_warnings=False, set_value_ssl=False):
        if disable_warnings:
            requests.packages.urllib3.disable_warnings()
        if set_value_ssl:
            self.__increase_default_value_ssl()
        session = requests.Session()
        retries = Retry(total=20, backoff_factor=1, status_forcelist=list(range(500, 505)))
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def update_session(self, headers):
        self.session.headers.update(headers)

    def reset_session(self, headers):
        self.session.close()
        self.session = self._create_new_session()

    def close_session(self):
        self.session.close()

    def obtain_response(self, url, **kwargs):
        requests_timeout = 15
        method  = kwargs.get("method", "GET")
        headers = kwargs.get("headers", None)
        data    = kwargs.get("data", None)
        params  = kwargs.get("params", None)
        verify  = kwargs.get("verify", None)
        while True:
            try:
                timeout = requests_timeout if method == "GET" else None
                response = self.session.request(method, url, headers=headers, data=data, params=params, timeout=timeout, verify=verify)
                return response
            except requests.exceptions.ConnectionError:
                self.requests_logger.warning("Connection Error, Retrying")
                time.sleep(requests_timeout)
                continue
            except requests.exceptions.RequestException:
                self.requests_logger.warning(f'Waiting... {url}')
                time.sleep(requests_timeout)
                continue

    def async_requests(self, list_urls, max_workers=3, **kwargs):
        """Utilizado para realizar peticiones de forma asíncrona.\n
        Por defecto se realizan de a 3 peticiones asíncronas por vez.

        Args:
        - list_urls :class:`list`: Lista de URLs :class:`str`
        - max_workers :class:`int`: Cantidad máxima de Threads que se crearán

        Returns:
        - list_responses :class:`list`: Lista de objetos de tipo :class:`Response <Response>`

        Basic usage:
            >>> from handle.datamanager import RequestsUtils
            >>> req_utils = RequestsUtils()
            >>> list_responses = req_utils.async_requests(["url1", "url2", "url3"])
        """
        list_responses = []
        len_urls = len(list_urls)
        # ["url1","url2","url3","url4","url5","url6"] --> [["url1","url2","url3"], ["url4","url5","url6"]]
        list_urls = [list(list_urls[i:i+max_workers]) for i in range(0, len_urls, max_workers)]
        for sublist_urls in list_urls:
            list_threads = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for url in sublist_urls:
                    self.requests_logger.info(f'ASYNC request: {url}')
                    list_threads.append(executor.submit(self.obtain_response, url, **kwargs))
                for task in as_completed(list_threads):
                    list_responses.append(task.result())
            del list_threads

        return list_responses


    def instantiateSeleniumWithProxy(country_code, headless=False):

        """
        Creates a Chrome extension for handling Authenticated Proxies in the /chrome_extensions folder
        and instances a ChromeDriver connected to Oxylab's proxies

        """

        import os, zipfile
        from selenium.webdriver.chrome.options import Options

        def setupChromeExtension(country_code):

            oxylabs_proxies = config()["oxylabs"]

            PROXY_HOST = oxylabs_proxies["countries"][country_code]["host"]  # rotating proxy or host
            PROXY_PORT = oxylabs_proxies["countries"][country_code]["port"] # port
            PROXY_USER = oxylabs_proxies["auth"]["username"] # username
            PROXY_PASS = oxylabs_proxies["auth"]["password"] # password

            manifest_json = """
            {
                "version": "1.0.0",
                "manifest_version": 2,
                "name": "Chrome Proxy",
                "permissions": [
                    "proxy",
                    "tabs",
                    "unlimitedStorage",
                    "storage",
                    "<all_urls>",
                    "webRequest",
                    "webRequestBlocking"
                ],
                "background": {
                    "scripts": ["background.js"]
                },
                "minimum_chrome_version":"22.0.0"
            }
            """

            background_js = """
            var config = {
                    mode: "fixed_servers",
                    rules: {
                    singleProxy: {
                        scheme: "http",
                        host: "%s",
                        port: parseInt(%s)
                    },
                    bypassList: ["localhost"]
                    }
                };

            chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

            function callbackFn(details) {
                return {
                    authCredentials: {
                        username: "%s",
                        password: "%s"
                    }
                };
            }

            chrome.webRequest.onAuthRequired.addListener(
                        callbackFn,
                        {urls: ["<all_urls>"]},
                        ['blocking']
            );
            """ % (PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS)

            return manifest_json, background_js


        country_code = country_code.upper()
        manifest_json, background_js = setupChromeExtension(country_code)

        if not os.path.exists("chrome_extensions"):
            os.makedirs("chrome_extensions")
            print("Directory 'chrome_extensions' Created ")

        options = Options()
        pluginfile = f'chrome_extensions/proxy_auth_plugin_{country_code}.zip'

        with zipfile.ZipFile(pluginfile, 'w') as zp:
            zp.writestr("manifest.json", manifest_json)
            zp.writestr("background.js", background_js)
        options.add_extension(pluginfile)

        if headless:
            options.add_argument('--headless')

        driver = webdriver.Chrome(options=options)

        return driver


    def createSessionWithProxy(country_code):

        def getProxies(country_code):

            oxylabs_proxies = config()["oxylabs"]

            username = oxylabs_proxies["auth"]["username"]
            password = oxylabs_proxies["auth"]["password"]
            port = oxylabs_proxies["countries"][country_code]["port"]
            host = oxylabs_proxies["countries"][country_code]["host"]

            proxy = f"{username}:{password}@{host}:{port}"

            proxies = {
                "http":"http://" + proxy,
                "https":"https://" + proxy
            }
            return proxies

        session = requests.sessions.Session()
        session.proxies.update(getProxies(country_code))

        return session

    def __del__(self):
        self.requests_logger.info(f'Closing connection.')
        self.close_session()
