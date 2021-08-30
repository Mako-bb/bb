# -*- coding: utf-8 -*-
import json
from typing import Counter
from pymongo.client_options import _parse_ssl_options
import requests # Si el script usa requests/api o requests/bs4
import time
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload

class VrtFacu():

    """
    - Status: EN PROGRESO
    - VPN: NO
    - Método: API
    - Runtime: < 30sec

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
        self.URL                    = config_['url']
        self.payloads               = []
        self.payloads_episodes      = []
        self.ids_scrapeados         = Datamanager._getListDB(self,self.titanScraping)
        self.ids_scrapeados_episodios = Datamanager._getListDB(self,self.titanScrapingEpisodios)
        self.id = 0
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
    

    def scraping(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--start-maximized')
        options.add_argument('--disable-extensions')

        path = 'C:/chromedriver.exe'
        browser = webdriver.Chrome(path, chrome_options=options)
        browser.get('https://www.vrt.be/vrtnu/a-z/#searchtype=programs')
        time.sleep(5)
        
        series = self.get_series(browser)
        movies = self.get_movies(browser)
        
        for movie in movies:
            #Entro a la url consigo la imagen de portada y consigo la url donde hay mas datos de la pelicula
            self.get_movie_content(movie, browser)
            time.sleep(5)
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        
        # for serie in series:
        #     self.get_serie_content(serie)



    def get_hrefs(self, browser):
        hrefs = []
        section = browser.find_element_by_css_selector('nui-list--content[role="list"]')
        lists = section.find_elements_by_css_selector('li>ul>li:not([class*="hidden-by-search-filtering"])')
        for li in lists:
            href = li.find_element_by_tag_name('nui-tile').get_attribute('href')
            hrefs.append(self.URL+href)
        return hrefs


    def get_series(self, browser):
        browser.find_element_by_css_selector('input[value="series"]').click()
        #Tiempo de espera para que se actualice el filtro
        time.sleep(5)
        browser.find_element_by_css_selector('input[value="series"]').click()
        #Vuelvo a clickear para sacar el filtro de series
        time.sleep(5)
        return self.get_hrefs(browser)
     
    
    def get_movies(self, browser):
        #Hago un execute script para poder conseguir los elemenos 
        #dentro de un Shadow-root
        tag = 'nui-input--group[name="categories"]'
        btn_show_more = browser.execute_script(f"return document.querySelector('{tag}').shadowRoot.querySelector('div.showmore')")
        btn_show_more.click()
        browser.find_element_by_css_selector('nui-input--group[name="categories"] label[fetched="films"]').click()
        #Tiempo de espera para que se actualice el filtro
        time.sleep(5)
        return self.get_hrefs(browser)

    
    def get_image(self, browser):
        try:
            return browser.find_element_by_css_selector('#parsys_pageHeader').get_attribute('src')
        except Exception:
            return browser.find_element_by_css_selector('#parsys_pageheader').get_attribute('src')

    
    def get_movie_content(self, movie, browser):
        browser.get(movie)
        time.sleep(3)
        #Utilizo el refresh por si salta algun pop up
        #Me di cuenta que al refrescar se quita
        browser.refresh()
        time.sleep(3)
        #Saco la imagen que hay en el banner antes de acceder a la pelicula
        image = self.get_image(browser)
        browser.execute_script("return document.querySelector('vrtnu-tile').shadowRoot.querySelector('.media')").click()
        #Tiempo para que cargue la pelicula
        time.sleep(5)
        self.id+=1
        payload = self.get_payload(browser, image)
        payload_movie = payload.payload_movie()
        Datamanager._checkDBandAppend(self,payload_movie,self.ids_scrapeados,self.payloads)
        

    def get_serie_content(self):
        pass


    def get_payload(self,browser, image):

            payload = Payload()
            
            
            payload.platform_code = self._platform_code
            #payload.id = self.get_id(browser)#NO FUNCION / BUSCAR FORMA DE ENCONTRAR ID
            payload.id = self.id
            payload.title = self.get_title(browser)
            payload.original_title = payload.title
            payload.clean_title = _replace(payload.title)
            payload.deeplink_web = browser.current_url
        
            payload.year = self.get_year(browser)
            
            payload.duration = self.get_duration(browser)
            payload.synopsis = self.get_synopsis(browser)
            payload.rating = self.get_rating(browser)
            payload.genres = self.get_genres(browser)
            payload.image = [image]
            # payload.cast = self.get_cast(browser)
            # payload.directors = self.get_directors(browser)
            # payload.availability = self.get_availability(browser)
            payload.packages = self.get_packages()
            # #Agregados
            # payload.download = self.get_download(browser)
            # payload.is_original = self.get_is_original(browser)
            # payload.is_adult = self.get_is_adult(browser)
            # payload.crew = self.get_crew(browser)
            payload.createdAt = self._created_at

            return payload

    
    def get_id(self, browser):
        return browser.find_element_by_xpath('/html/body/div[4]').get_attribute('id')
        
        
    def get_title(self, browser):
        return browser.execute_script("return document.querySelector('vrtnu-video-information h2').textContent")
    

    def get_year(self, browser):
        div = browser.find_elements_by_class_name("vrtnu-text--default")
        return div[0].text

    
    def get_duration(self, browser):
        div = browser.find_elements_by_class_name("vrtnu-text--default")
        return div[1].text


    def get_synopsis(self, browser):
        try:
            return browser.find_element_by_css_selector('vrtnu-video-information .cmp-text p').text
        except Exception:
            print(f'No se encontro sinopsis para : {browser.current_url}')

   
    def get_rating(self, browser):
        try:
            icon_rating = browser.find_element_by_tag_name('vrtnu-icon').get_attribute('alt')     
            icon_rating = icon_rating.split(' ')
            return icon_rating[1]
        except Exception:
            print(f'No se encontro rating para : {browser.current_url}')
            return None
    
    
    def get_genres(self, browser):
        try:
            div = browser.find_elements_by_class_name("vrtnu-text--highlighted")
            return [div[1].text]
        except Exception:
            print(f'no contiene genero : {browser.current_url}')
            None


    def get_packages(self):
        """  Se hardcodeo el package porque no se encontró el dato. """
        return [{"Type":"subscription-vod"}]

