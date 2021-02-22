# -*- coding: utf-8 -*-
import time
import requests
import hashlib   
import pymongo 
import re
import json
import platform
from handle.replace                     import _replace
from common                             import config
from datetime                           import datetime
from handle.mongo                       import mongo
from slugify                            import slugify
from handle.datamanager                 import Datamanager
from updates.upload                     import Upload
from selenium                           import webdriver
from selenium.webdriver.support.wait    import WebDriverWait
from selenium.webdriver.common.by       import By
from selenium.webdriver.support         import expected_conditions as EC
from selenium.webdriver.firefox.options import Options

class SundanceTvTest():
    '''
        Scraping de la plataforma SundanceTv, la misma se puede scrapear por API. Cuenta con un caso particular en el que se tiene que utilizar Selenium
        para obtener el link "/shows/nombre-de-serie/episodes--XXXX" (mediante un apartado de "Episodios" cliqueable) que facilita el acceso al .json 
        de esa serie especifica, en donde se encuentran todas las temporadas que tiene la serie y sus links. Dichos links van a servir para scrapear 
        todos los episodios, temporada por temporada.

        DATOS IMPORTANTES: 
            - ¿Necesita VPN? -> NO.
            - ¿HTML, API, SELENIUM? -> SELENIUM y API
            - Cantidad de contenidos (ultima revisión 22/02/2021): 53 contenidos | 420 episodios
            - Tiempo de ejecucion: 5 minutos aproximadamente
    '''
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        #self._start_url             = self._config['start_url']
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.skippedTitles          = 0
        self.skippedEpis            = 0
        self.titanScrapingEpisodios = self.titanScrapingEpisodes
        
        self.currentSession = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

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
        
        if type == 'scraping':
            self._scraping()

        if type == 'testing':
            self._scraping(testing = True)

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

    def _scraping(self, testing = False):

        payloads = []
        payloads_episodes = []

        scraped = Datamanager._getListDB(self, self.titanScraping)
        scraped_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodes)

        start_time = time.time()

        ###############
        ## PELICULAS ##
        ###############
        request = self.currentSession.get('https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/sundance/url/movies?device=web')
        print(request.status_code, request.url)

        data = request.json()

        titulos = data['data']['children'][4]['children']

        for titulo in titulos:

            info = titulo['properties']['cardData']

            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            str(info['meta']['nid']),
                'Title':         info['text']['title'],
                'OriginalTitle': None,
                'CleanTitle':    _replace(info['text']['title']),
                'Type':          "movie",
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       'https://www.sundancetv.com{}'.format(info['meta']['permalink']),
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      info['text']['description'],
                'Image':         None,
                'Rating':        None,
                'Provider':      None,
                'Genres':        [info['meta']['genre']],
                'Cast':          None,
                'Directors':     None,
                'Availability':  None,
                'Download':      None,
                'IsOriginal':    None,
                'IsAdult':       None,
                'Packages':      [{'Type': 'tv-everywhere'}],
                'Country':       None,
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }

            Datamanager._checkDBandAppend(self, payload, scraped, payloads)

        ###############
        ### SERIES ####
        ###############
        request = self.currentSession.get('https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/sundance/url/shows?device=web')
        print(request.status_code, request.url)

        data = request.json()

        titulos = data['data']['children'][5]['children']

        opts = Options()
        opts.add_argument('--headless')

        # Para obtener la API que contiene las temporadas es necesario obtener el link de los Episodios de una serie, es por eso que tengo que utilizar Selenium.
        browser = webdriver.Firefox(options=opts)

        # Esta lista va a contener las referencias de cada contenido, para que luego sea facil de acceder en la parte de episodios.
        content_references = []

        for titulo in titulos:

            info = titulo['properties']['cardData']

            content_link = 'https://www.sundancetv.com{}'.format(info['meta']['permalink'])

            # Creo una lista para almacenar los links de las temporadas de cada serie, para luego acceder a las APIs que tienen data de los episodios.
            seasons_links = []
            # Esta lista guarda los payloads de cada temporada
            seasons_payload = []

            try:
                browser.get(content_link)
                # Obtengo el link la seccion de Episodios.
                episodes = WebDriverWait(browser, 5).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[3]/div[1]/a'))
                )

                episodes_link = episodes.get_attribute('href').replace('https://www.sundancetv.com', '')
                print(episodes_link)

                # Valido que el contenido cuenta con un apartado de episodios.
                if 'episodes' in episodes_link:
                    request = self.currentSession.get('https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/sundance/url{}?device=web'.format(episodes_link))

                    data = request.json()

                    seasons_data = data['data']['children'][3]['properties']['dropdownItems']

                    for season in seasons_data:
                        
                        season_properties = season['properties']

                        season_payload = {
                                'Id':        season_properties['nid'],
                                'Synopsis':  None,
                                'Title':     season_properties['title'],
                                'Deeplink':  'https://www.sundancetv.com{}'.format(season_properties['permalink']),
                                'Number':    None,
                                'Year':      None,
                                'Image':     None,
                                'Directors': None,
                                'Cast':      None,
                                }
                        
                        seasons_links.append(season_payload['Deeplink'])
                        seasons_payload.append(season_payload)
            
            except:
                print("Este contenido no cuenta con un apartado de Episodios")

            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            str(info['meta']['nid']),
                "Seasons":       seasons_payload if seasons_payload else None,
                'Title':         info['text']['title'],
                'OriginalTitle': None,
                'CleanTitle':    _replace(info['text']['title']),
                'Type':          "serie",
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       content_link,
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
                'Synopsis':      info['text']['description'],
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
                'Packages':      [{'Type': 'tv-everywhere'}],
                'Country':       None,
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }

            Datamanager._checkDBandAppend(self, payload, scraped, payloads)

            # Este diccionario va a guardar los ids, titulos y links a temporadas de cada contenido.
            references = {
                'Id': payload['Id'],
                'Title': payload['Title'],
                'SeasonsLinks': seasons_links
            }
            content_references.append(references)
        
        browser.close()

        ###################
        #### EPISODIOS ####
        ###################
        for reference in content_references:
            
            # Valido que la referencia del contenido cuente con links a temporadas.
            if reference['SeasonsLinks']:

                for link in reference['SeasonsLinks']:

                    modified_link = link.replace('https://www.sundancetv.com', '')
                    request = self.currentSession.get('https://content-delivery-gw.svc.ds.amcn.com/api/v2/content/amcn/sundance/url{}?device=web'.format(modified_link))

                    data = request.json()

                    episodes_data = data['data']['children'][4]['children']

                    for episode in episodes_data:

                        info = episode['properties']['cardData']

                        payload_episodes = {
                            'PlatformCode':  self._platform_code,
                            'Id':            str(info['meta']['nid']), 
                            'ParentId':      reference['Id'],
                            'ParentTitle':   reference['Title'],
                            'Episode':       int(info['text']['seasonEpisodeNumber'].split(',')[1].replace('E', '')) if info['text'].get('seasonEpisodeNumber') else None, 
                            'Season':        int(info['text']['seasonEpisodeNumber'].split(',')[0].replace('S', '')) if info['text'].get('seasonEpisodeNumber') else None, 
                            'Title':         info['text']['title'],
                            'OriginalTitle':  None, 
                            'Year':          None, 
                            'Duration':      None,
                            'ExternalIds':   None,
                            'Deeplinks': {
                                'Web':       'https://www.sundancetv.com{}'.format(info['meta']['permalink']),
                                'Android':   None,
                                'iOS':       None,
                                },
                            'Synopsis':      info['text']['description'],
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
                            'Packages':      [{'Type': 'tv-everywhere'}],
                            'Country':       None,
                            'Timestamp':     datetime.now().isoformat(),
                            'CreatedAt':     self._created_at
                            }

                        Datamanager._checkDBandAppend(self, payload_episodes, scraped_episodes, payloads_episodes, isEpi=True)
            
        Datamanager._insertIntoDB(self, payloads, self.titanScraping)
        Datamanager._insertIntoDB(self, payloads_episodes, self.titanScrapingEpisodes)

        self.currentSession.close()


        Upload(self._platform_code, self._created_at, testing=testing)
        print("--- {} seconds ---".format(time.time() - start_time))