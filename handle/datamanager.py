# -*- coding: utf-8 -*-
import json
import time
import requests
import hashlib
import platform
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyvirtualdisplay       import Display
from common                 import config
from bs4                    import BeautifulSoup
from datetime               import datetime
from handle.mongo           import mongo
from updates.upload         import Upload
from selenium               import webdriver
from selenium.webdriver import ActionChains


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

    def _checkDBandAppend(self, payload, listDB, listPayload, currentItem=0, totalItems=0, isEpi=False):
        '''
        - Recibe:
            - El payload del contenido a insertar
            - La lista del mongo local, que se deberia haber sacado antes con _getListDB()
            - La lista de payloads del script
            - Si se tiene la cantidad de contenidos que tiene la plataforma de antemano, usar currentItem y totalItems (opcional)
            - isEpi determina si el payload es de contenido o de episodios

        Luego de chequear si ya existe el contenido en la lista de mongo, usa el metodo noInserta() o inserta()
        '''
        if isEpi:
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

    def _getSoup(self, URL, headers={}, showURL=True, timeOut=0, parser = 'html.parser'):
        '''
        Devuelve el beautifulsoup del URL solicitado, si es necesario se le puede pasar headers.
        '''
        if showURL == True:
            print("\x1b[1;33;40m INTENTANDO PAGINA ----> \x1b[0m"+URL)
        time.sleep(timeOut)
        content = self.sesion.get(URL,headers=headers)
        soup = BeautifulSoup(content.text, features=parser)
        return soup

    def _getJSON(self, URL, headers=None, data=None, json=None, showURL=True, usePOST=False, timeOut=0):
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
                    content = self.sesion.get(URL,headers=headers)

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
                print(content.text)
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

    def obtain_response(url, **kwargs):
        requests_timeout = 5
        method  = kwargs.get("method", "GET")
        headers = kwargs.get("headers", None)
        data    = kwargs.get("data", None)
        while True:
            try:
                timeout = requests_timeout if method == "GET" else None
                response = requests.request(method, url, headers=headers, data=data, timeout=timeout)
                return response
            except requests.exceptions.ConnectionError:
                print("Connection Error, Retrying")
                time.sleep(requests_timeout)
                continue
            except requests.exceptions.RequestException:
                print('Waiting...', url)
                time.sleep(requests_timeout)
                continue

    def async_requests(list_urls, max_workers=3, headers=None):
        """Utilizado para realizar peticiones de forma asíncrona.\n 
        Por defecto se realizan de a 3 peticiones asíncronas por vez.
        
            Args:
            - list_urls (list) - Lista de URLS(str)
            - max_workers (int) - Cantidad máxima de Threads que se crearán
            - headers (dict) - headers para algunas peticiones
        
            Returns:
            - list_responses (list): Lista de responses

            Example:
            from handle.datamanager import DataManager as utils
            list_responses = utils.async_requests(["url1", "url2", "url3"]) \n
        """

        list_responses = []
        len_urls = len(list_urls)
        # ["url1","url2","url3","url4","url5","url6"] --> [["url1","url2","url3"], ["url4","url5","url6"]]
        list_urls = [list(list_urls[i:i+max_workers]) for i in range(0, len_urls, max_workers)]
        for sublist_urls in list_urls:
            list_threads = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for url in sublist_urls:
                    # time.sleep(0.1)
                    print("ASYNC request:", url)
                    list_threads.append(executor.submit(Datamanager.obtain_response, url, headers=headers))
                for task in as_completed(list_threads):
                    list_responses.append(task.result())
            del list_threads
        
        return list_responses

    def _clickAndGetSoupSelenium(self,URL,botton,waitTime=0,showURL=True):
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

