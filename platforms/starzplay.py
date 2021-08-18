# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace

class StarzPlay():

    """
    - Status: EN PROGRESO
    - VPN: NO
    - Método: (Si la plataforma se scrapea con Requests, BS4, Selenium o alguna mezcla)
    - Runtime: 0:00:00.494192

    """

    def __init__(self, ott_platforms, ott_site_country, ott_operation):
        self.test = ott_operation in ("testing", "return") #
        config_ = config()['ott_sites'][ott_platforms] # Obligatorio
        self.country = ott_site_country # Opcional, puede ser útil dependiendo de la lógica del script.
        self._created_at = time.strftime('%Y-%m-%d')
        self._platform_code = config_['countries'][ott_site_country]
        self.mongo = mongo()
        self.sesion                 = requests.session() # Requerido si se va a usar Datamanager
        self.titanPreScraping       = config()['mongo']['collections']['prescraping'] # Opcional
        self.titanScraping          = config()['mongo']['collections']['scraping'] # Obligatorio. También lo usa Datamanager
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode'] # Obligatorio. También lo usa Datamanager
        self.skippedTitles          = 0 # Requerido si se va a usar Datamanager
        self.skippedEpis            = 0 # Requerido si se va a usar Datamanager
        self.url                    = config_['url']
        #### OBLIGATORIO si se usa Selenium para que pueda correr en los servers
        try:
            if platform.system() == 'Linux':
                Display(visible=0, size=(1366, 768)).start()
        except Exception:
            pass
        ####

        """
        La operación 'return' la usamos en caso que se nos corte el script a mitad de camino cuando
        testeamos, sea por un error de conexión u otra cosa. Nos crea una lista de ids ya insertados en
        nuestro Mongo local, la cual podemos usar para saltar los contenidos scrapeados y volver rápidamente
        a donde había cortado el script.
        """
        if ott_operation == 'return':
            return_params = {'PlatformCode' : self._platform_code}
            last_item = self.mongo.lastCretedAt('titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_ids = [pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_ids = list()

        if ott_operation in ('testing', 'scraping'):
            self.scraping()



    def scroll_page(self, driver):
        SCROLL_PAUSE_TIME = 0.5
        last_height = driver.execute_script("return document.body.scrollHeight")
        
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height     


    def get_urls_films(self, driver):
        urls_films_temp = []
        container_urls = driver.find_elements_by_class_name('content-link')
        
        for urls in container_urls:
            urls_films_temp.append(urls.find_element_by_tag_name('a').get_attribute('href'))

        return urls_films_temp    



    def get_data(self, driver, urls_films):

        #PAYLOAD DE LA SALADA
        """
        payload_peliculas = {
                    "PlatformCode":  self._platform_code,
                    "Title":         movie['properties']['cardData']['text']['title'],
                    "CleanTitle":    _replace(movie['properties']['cardData']['text']['title']),
                    "OriginalTitle": None,
                    "Type":          "movie",
                    "Year":          None,
                    "Duration":      None,

                    "Id":            str(movie['properties']['cardData']['meta']['nid']),
                    "Deeplinks": {

                        "Web":       deeplink.replace('/tve?',''),
                        "Android":   None,
                        "iOS":       None,
                    },
                    "Synopsis":      movie['properties']['cardData']['text']['description'],
                    "Image":         [movie['properties']['cardData']['images']],
                    "Rating":        None,  # Important!
                    "Provider":      None,
                    "Genres":        [movie['properties']['cardData']['meta']['genre']],  # Important!
                    "Cast":          None,
                    "Directors":     None,  # Important!
                    "Availability":  None,  # Important!
                    "Download":      None,
                    "IsOriginal":    None,  # Important!
                    "IsAdult":       None,  # Important!
                    "IsBranded":     None,  # Important!
                    # Obligatorio
                    "Packages":      [{'Type': 'tv-everywhere'}],
                    "Country":       None,
                    "Timestamp":     datetime.now().isoformat(),  # Obligatorio
                    "CreatedAt":     self._created_at,  # Obligatorio
                }
        """

        for film in urls_films:
            time.sleep(3)
            driver.get(film)
    
    
    
    def scraping(self):

        options = webdriver.ChromeOptions()
        options.add_argument('--start-maximized')
        options.add_argument('--disable-extensions')
        
        path = 'C:\chromedriver'
        driver = webdriver.Chrome(path,chrome_options=options)
        driver.get(self.url)
       
        time.sleep(2)


        #Clickeo la seccion buscar del nav
        header_browse_link = driver.find_element_by_class_name('header-browse-link')
        header_browse_link.click()
        header_nav_link = header_browse_link.find_element_by_class_name('header-nav-link')
        header_nav_link.find_element_by_tag_name('a').click()
        
        time.sleep(2)
        #Busco el link que contiene todos los films
        categories = driver.find_element_by_class_name('categories-blocks-wrapper')
        categories.find_element_by_class_name('starz-link').click()

        
        self.scroll_page(driver)
        
        urls_films = self.get_urls_films(driver)
        
        self.get_data(driver, urls_films)