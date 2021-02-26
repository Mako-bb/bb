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
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains

class Bet():
    '''
        Scraping de la plataforma Bet, la misma cuenta únicamente con series como contenido scrapeable 
        (además tiene videos musicales o notas con artistas). El scraping comienza en la sección de shows 
        (apartado All shows A - Z) y hay que filtrar los contenidos validando que sean shows con episodios
        de los que se puedan obtener datos.

        DATOS IMPORTANTES: 
            - ¿Necesita VPN? -> SI (PureVPN USA)
            - ¿HTML, API, SELENIUM? -> SELENIUM y BS4
            - Cantidad de contenidos (ultima revisión 24/02/2021): 44 series | 510 episodios
            - Tiempo de ejecucion: 37 minutos aproximadamente (depende de conexión a internet)
    '''
    def __init__(self, ott_site_uid, ott_site_country, type):
        self._config                = config()['ott_sites'][ott_site_uid]
        self._platform_code         = self._config['countries'][ott_site_country]
        self._created_at            = time.strftime("%Y-%m-%d")
        self.mongo                  = mongo()
        self.titanPreScraping       = config()['mongo']['collections']['prescraping']
        self.titanScraping          = config()['mongo']['collections']['scraping']
        self.titanScrapingEpisodes  = config()['mongo']['collections']['episode']
        self.skippedTitles          = 0
        self.skippedEpis            = 0
        self.titanScrapingEpisodios = self.titanScrapingEpisodes
        
        self.sesion = requests.session()
        self.headers  = {"Accept":"application/json",
                         "Content-Type":"application/json; charset=utf-8"}

        self.driver = webdriver.Firefox()
        self._start_url = self._config['start_url']
        self.main_page_see_more_xpath = self._config['queries']['main_page_see_more_xpath']
        self.episodes_see_more_css_selector = self._config['queries']['episodes_see_more_css_selector']

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

        payloads = []
        payloads_episodes = []

        scraped = Datamanager._getListDB(self, self.titanScraping)
        scraped_episodes = Datamanager._getListDB(self, self.titanScrapingEpisodes)

        start_time = time.time()

        ###############
        ### TITULOS ###
        ###############
        self.driver.maximize_window()
        self.driver.get(self._start_url)

        # Para cargar todos los contenidos hay que cliquear continuamente en el botón "SEE MORE" ubicado al final de la página.
        self.main_page_see_more_button(self.driver)

        # Traigo el html actualizado luego de los clicks para pasarlo por un bsoup
        page_source = self.driver.page_source
        updated_soup = BeautifulSoup(page_source, 'lxml')

        # Busco todos los links de los contenidos que aparecen en la pagina principal luego de cargar todos los "SEE MORE"
        titles_links = updated_soup.find_all('a', {"href" : re.compile("https://www.bet.com/shows")})

        for title in titles_links:
            href = title.get("href")
            
            content_soup = Datamanager._getSoup(self, href)

            # Corroboro que el contenido tenga episodios sobre los cuales se pueda iterar y obtener información, si el contenido actual no tiene episodios lo salteo y paso al siguiente.
            if content_soup.find('h3', {"class": "filter__title"}) is None or content_soup.find('h3', {"class": "filter__title"}).text != "episodes":
                continue
            else:
                content_title = content_soup.findAll('span', {"class": "title__title"})[0].text

                _id = hashlib.md5(content_title.encode('utf-8')).hexdigest()

                content_updated_soup = Datamanager._clickAndGetSoupSelenium(
                    self, href, 'filter__dropdown-container__optionsWrapper', 5, showURL=False)
                seasons = content_updated_soup.findAll('a', {"class": "filter__dropdown-container__option default open"})
                
                seasons_data = []

                # Para cada temporada del contenido voy a obtener el link, con ese dato puedo armar el html soup y asi completar
                # tanto el payload de la temporada para agregar al contenido como los datos de los episodios de la misma. 
                for season in seasons:
                    season_link = "https://www.bet.com" + season.get('href')
                    
                    # Filtro el "Season ..." para quedarme con el numero de temporada
                    season_number = season.text.replace("Season", "").strip()

                    season_payload = {
                        "Id":        _id + season_number, 
                        "Synopsis":  None, 
                        "Title":     None,
                        "Deeplink":  season_link,
                        "Number":    int(season_number),
                        "Year":      None,
                        "Image":     None,
                        "Directors": None,
                        "Cast":      None
                        }
                    
                    # Agrego el payload a la lista con datos de las temporadas del contenido actual
                    seasons_data.append(season_payload)

                    # Si el navegador se cuelga por x motivo en esta instancia no se obtienen los 
                    # episodios de la temporada actual. TODO: AVISAR EL ERROR 
                    try:    
                        self.driver.get(season_link)
                    except:
                        print("No se pudo cargar la página")
                        continue

                    # Luego de traer el link de la temporada, me fijo si tiene un boton "See More" que carga mas episodios.
                    # Si lo tiene llamo a la función que los cliquea y devuelve un soup actualizado. Caso contrario
                    # obtengo el soup sin modificaciones.
                    try:
                        if self.driver.find_element_by_css_selector(self.episodes_see_more_css_selector.format(11)) is not None:
                            season_page = self.content_soup_after_see_more_button(self.driver)
                    except:
                        season_page = Datamanager._getSoup(self, season_link)

                    # Dentro del container de episodios traigo todos los links de los mismos, para iterarlos y sacar informacion
                    episodes_container = season_page.find(
                        'section', {"class": re.compile("filter-video__container filtered")})
                    episodes = episodes_container.findAll(
                        'div', {"class": re.compile("js-item filter-video__item")})

                    for episode in episodes:

                        epi_title = episode.find('h3').text
                        # Para el id tomo el id del padre y le concateno el hash del titulo del episodio y el nro de season 
                        # (hay algunos episodios de la misma serie que tienen el mismo nombre en distintas temporadas)
                        epi_id = _id + hashlib.md5(epi_title.encode('utf-8')).hexdigest() + season_number
                        epi_image = episode.find('img').get("srcset")
                        epi_info = episode.findAll('p')
                        epi_link = episode.find('a').get("href")

                        epi_number = epi_info[0].text

                        # Debido al formato que tiene la página, algunas series cuentan con temporadas 
                        # correspondientes al año en el que se estrenaron (mas que series son especiales)
                        # En ese caso tampoco tienen nro de episodios por lo que quedan en 0.
                        epi_number_filtered = epi_number.replace("S{} EP".format(int(season_number)), "") if "EP" in epi_number else None
                        # Luego del filtro anterior me quedo con "{nro_episodio}...." por lo que si hay otra cadena con caracteres o números
                        # despues de lo filtrado lo paso por un ultimo filtro que obtiene el primer numero entero positivo
                        clean_epi_number = int(re.findall(r'\d+', epi_number_filtered)[0]) if epi_number_filtered is not None else None

                        epi_synopsis = epi_info[1].text if len(epi_info) > 1 else None

                        payload_episodes = {
                                'PlatformCode':  self._platform_code,
                                'Id':            epi_id, 
                                'ParentId':      _id,
                                'ParentTitle':   content_title,
                                'Episode':       clean_epi_number,
                                'Season':        int(season_number) if int(season_number) < 100 else 0,
                                'Title':         epi_title,
                                'OriginalTitle': None, 
                                'Year':          None, 
                                'Duration':      None,
                                'ExternalIds':   None,
                                'Deeplinks': {
                                    'Web':       epi_link,
                                    'Android':   None,
                                    'iOS':       None,
                                    },
                                'Synopsis':      epi_synopsis,
                                'Image':         ['www.bet.com' + epi_image],
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

            payload = {
                'PlatformCode':  self._platform_code,
                'Id':            _id,
                "Seasons":       seasons_data,
                'Title':         content_title,
                'OriginalTitle': None,
                'CleanTitle':    _replace(content_title),
                'Type':          "serie",
                'Year':          None,
                'Duration':      None,
                'Deeplinks': {
                    'Web':       href,
                    'Android':   None,
                    'iOS':       None,
                },
                'Playback':      None,
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
                'Packages':      [{'Type': 'tv-everywhere'}],
                'Country':       None,
                'Timestamp':     datetime.now().isoformat(),
                'CreatedAt':     self._created_at
            }
            
            Datamanager._checkDBandAppend(self, payload, scraped, payloads)

        Datamanager._insertIntoDB(self, payloads, self.titanScraping)
        Datamanager._insertIntoDB(self, payloads_episodes, self.titanScrapingEpisodes)

        self.driver.close()
        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)
        print("--- {} seconds ---".format(time.time() - start_time))

    def main_page_see_more_button(self, browser):
        '''
           Precondición: Solo se puede llamar si el browser tiene la página principal cargada.
           
           Va cliqueando los "See More" que aparecen en la página principal (la que tiene todos los shows ordenados de la A a la Z) con el objetivo
           de que carguen todos los contenidos. Una vez que carga todos y no aparecen mas botones finaliza la ejecución. 
        '''
        index = 1
        time.sleep(10)

        while True:
            see_more_xpath = self.main_page_see_more_xpath.format(index)
            time.sleep(3)
            try:
                browser.execute_script("arguments[0].click();", browser.find_element_by_xpath(see_more_xpath))
                index += 1
            except:
                break

    def content_soup_after_see_more_button(self, browser):
        '''
            PRECONDICIÓN: Solo se puede llamar si el browser tiene algun contenido 
                          (de tipo show, es indistinto en que temporada) cargado.

            Va cliqueando los "See More" (en el caso de haber varios) que aparecen en 
            la sección de los capitulos hasta que no aparecen mas botones.

            RETURN: Un BSoup actualizado con todos los episodios cargados.
        '''
        index = 11
        time.sleep(10)

        while True:
            see_more_css_selector = self.episodes_see_more_css_selector.format(index)
            time.sleep(3)
            try:
                browser.execute_script("arguments[0].click();", browser.find_element_by_css_selector(see_more_css_selector))
                index += 10
            except:
                break

        # Obtengo nuevamente el html actualizado
        updated_season_page = browser.page_source
        updated_season_soup = BeautifulSoup(updated_season_page, 'lxml')

        return updated_season_soup
