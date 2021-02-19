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

class BetTest():
    '''
        Scraping de la plataforma Bet, la misma cuenta únicamente con series como contenido scrapeable (además tiene videos musicales o notas con artistas). El scraping comienza en 
        la sección de shows (apartado All shows A - Z) y hay que filtrar los contenidos validando que sean shows con episodios de los que se puedan obtener datos.

        IMPORTANTE: Se necesita VPN.
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
        
        self.sesion = requests.session()
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

        ###############
        ### TITULOS ###
        ###############
        url = "https://www.bet.com/shows.html"
        browser = webdriver.Firefox()
        browser.maximize_window()
        browser.get(url)

        # Para cargar todos los contenidos hay que cliquear continuamente en el botón "SEE MORE" ubicado al final de la página.
        self.main_page_see_more_button(browser)

        # Traigo el html actualizado luego de los clicks para pasarlo por un bsoup
        page_source = browser.page_source
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

                # Scrolleo 500 pixels para que se pueda cliquear sobre el selector de temporadas y se pueda usar bsoup para sacar los links.
                browser.get(href)
                browser.execute_script("scrollTo(0, 500);")
                season_selector = browser.find_element_by_class_name('filter__dropdown-container__optionsWrapper')
                season_selector.click()

                # Obtengo el html actualizado con el selector de temporadas "abierto"
                title_page_source = browser.page_source

                title_updated_soup = BeautifulSoup(title_page_source, 'lxml')
                seasons = title_updated_soup.findAll('a', {"class": "filter__dropdown-container__option default open"})
                
                seasons_data = []

                # Para cada temporada del contenido voy a obtener el link, con ese dato puedo armar el html soup y asi completar
                # tanto el payload de la temporada para agregar al contenido como los datos de los episodios de la misma. 
                for season in seasons:
                    season_link = season.get('href')
                    
                    # Filtro el "Season ..." para quedarme con el numero de temporada
                    season_number = season.text.replace("Season", "")

                    season_payload = {
                        "Id":        _id + season_number.strip(), 
                        "Synopsis":  None, 
                        "Title":     None,
                        "Deeplink":  "https://www.bet.com" + season_link,
                        "Number":    int(season_number),
                        "Year":      None,
                        "Image":     None,
                        "Directors": None,
                        "Cast":      None
                        }
                    
                    # Agrego el payload a la lista con datos de las temporadas del contenido actual
                    seasons_data.append(season_payload)

                    season_url = season_payload['Deeplink']
                    browser.get(season_url)

                    # Luego de traer el link de la temporada, llamo a la funcion que cliquea todos los "SEE MORE" que haya
                    # en el container de episodios
                    self.content_page_see_more_button(browser)

                    # Obtengo nuevamente el html actualizado
                    updated_season_page = browser.page_source
                    updated_season_soup = BeautifulSoup(updated_season_page, 'lxml')

                    # Dentro del container de episodios traigo todos los links de los mismos, para iterarlos y sacar informacion
                    episodes_container = updated_season_soup.find('section', {"class": re.compile("filter-video__container filtered")})
                    episodes = episodes_container.findAll('a')

                    for episode in episodes:
                        # A veces el ultimo tag 'a' es un #, por eso valido que no lo sea para garantizar que estoy sobre un link
                        # de episodio
                        if episode.get("href") != "#":
                            href_epi = episode.get("href")

                            epi_soup = Datamanager._getSoup(self, href_epi)

                            # Valido que el episodio tenga un titulo localizable, de no ser así se lo saltea. 
                            if epi_soup.find('h2', {"class": "hero__sidebar__title"}) is not None:
                                epi_title = epi_soup.find('h2', {"class": "hero__sidebar__title"}).text
                            elif epi_soup.find('h1', {"class": "hero__title"}) is not None:
                                epi_title = epi_soup.find('h1', {"class": "hero__title"}).text
                            else:
                                continue

                            # Si se ubico el titulo es porque se está mostrando la pagina del episodio, por lo tanto puedo sacar el nro de episodio:
                            epi_number = epi_soup.find('span', {"class": "hero__sidebar__episode"}).text if epi_soup.find(
                                'span', {"class": "hero__sidebar__episode"}) is not None else None

                            # Como el tag.text trae "S.. EP..." lo paso por un filtro
                            clean_epi_number = epi_number.split("|")[0].replace("S{} EP".format(season_number.strip()), "") if epi_number is not None else None

                            # Genero el id del episodio usando el id de la serie junto con la codificacion del titulo del episodio
                            epi_id = _id + hashlib.md5(epi_title.encode('utf-8')).hexdigest()

                            payload_episodes = {
                                    'PlatformCode':  self._platform_code,
                                    'Id':            epi_id, 
                                    'ParentId':      _id,
                                    'ParentTitle':   content_title,
                                    'Episode':       int(clean_epi_number),
                                    'Season':        int(season_number),
                                    'Title':         epi_title,
                                    'OriginalTitle': None, 
                                    'Year':          None, 
                                    'Duration':      None,
                                    'ExternalIds':   None,
                                    'Deeplinks': {
                                        'Web':       href_epi,
                                        'Android':   None,
                                        'iOS':       None,
                                        },
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

        browser.close()
        self.sesion.close()

        Upload(self._platform_code, self._created_at, testing=testing)

    def main_page_see_more_button(self, browser):
        '''
           Precondición: Solo se puede llamar si el browser tiene la página principal cargada.
           
           Va cliqueando los "See More" que aparecen en la página principal (la que tiene todos los shows ordenados de la A a la Z) con el objetivo
           de que carguen todos los contenidos. Una vez que carga todos y no aparecen mas botones finaliza la ejecución. 
        '''
        index = 1
        time.sleep(10)

        while True:
            see_more_xpath = '/html/body/div[3]/div[1]/div[5]/div/div[2]/section[3]/div/div[{}]/a'.format(index)
                            #/html/body/div[2]/div[1]/div[5]/div/div[2]/section[3]/div/div[1]/a
            time.sleep(3)
            try:
                browser.execute_script("arguments[0].click();", browser.find_element_by_xpath(see_more_xpath))
                index += 1
            except:
                break

    def content_page_see_more_button(self, browser):
        '''
           Precondición: Solo se puede llamar si el browser tiene algun contenido (de tipo show, es indistinto en que temporada) cargado.

           Va cliqueando los "See More" (en el caso de haber varios) que aparecen en la sección de los capitulos hasta que no aparecen mas botones.
        '''
        index = 11
        time.sleep(5)

        while True:
            see_more_xpath = 'a.js-loadMoreButton:nth-child({})'.format(index)
            time.sleep(5)
            try:
                browser.execute_script("arguments[0].click();", browser.find_element_by_css_selector(see_more_xpath))
                index += 10
            except:
                break    
