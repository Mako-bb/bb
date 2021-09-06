# -*- coding: utf-8 -*-
import json
from typing import Counter
from pymongo.client_options import _parse_ssl_options
import requests # Si el script usa requests/api o requests/bs4
import time
from bs4                import BeautifulSoup
from requests.models import ContentDecodingError # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload
import hashlib
import re
class VrtFacu():

    """
    - Status: EN PROGRESO
    - VPN: NO
    - Método: Selenium
    - Runtime: 0:59:50.996992

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
    
    
    def is_exist(self, browser, element):
        """[Cree este metodo para saber
        si existe un elemento en el DOM]
        
        Args:
            browser ([type]): [url actual]
            element ([string]): [el valor del css_selector]

        Returns:
            [bool]: [existe o no existe]
        """
        try:
            browser.find_element_by_css_selector(element)
            return True
        except Exception:
            return False
 
    
    def is_trailer(self,browser, type_):
        if type_ == 'movie':
            if 'trailer' in browser.current_url:
                return True
        
        if type_ == 'serie':
            div_cant_seasons = browser.find_elements_by_css_selector('vrtnu-meta[slot="meta"] span')
            for span in div_cant_seasons:
                if '0 Seizoenen' in span.text:
                    return True
                if 'audiodescriptie' in browser.current_url:
                    return True
        
        if type_ == 'episode':
            if 'making-of' in browser:
                return True
            if 'extra-s' in browser:
                return True
            if 'samenvattingen' in browser:
                return True
            if 'extra' in browser:
                return True
            if 'bloopers' in browser:
                return True
            if 'trailer' in browser:
                return True
        return False
    
    
    def is_repeated(self, movie):
        if 'audiodescriptie' in movie:
            return True
        return False


    def scraping(self):

        path = 'C:/chromedriver.exe'
        browser = webdriver.Chrome(path)
        browser.get('https://www.vrt.be/vrtnu/a-z/#searchtype=programs')
        time.sleep(5)
        
        series = self.get_series(browser)
        movies = self.get_movies(browser)
        
        for movie in movies:
            if self.is_repeated(movie) == False:
                self.get_movie_content(movie, browser)
            
        for serie in series:
            browser.get(serie)
            type_ = 'serie'
            is_trailer = self.is_trailer(browser, type_)
            if is_trailer == False :
                self.get_serie_content(browser)
        
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        Upload(self._platform_code, self._created_at, testing=True, has_episodes=bool(self.payloads_episodes))
    
    
    def get_series(self, browser):
        browser.find_element_by_css_selector('input[value="series"]').click()
        #Tiempo de espera para que se actualice el filtro
        time.sleep(10)
        return self.get_hrefs(browser)
          

    def get_movies(self, browser):
        browser.find_element_by_css_selector('input[value="series"]').click()
        #Vuelvo a clickear para sacar el filtro de series
        time.sleep(5)
        
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

        if self.is_exist(browser,'#parsys_pageHeader'):
            return browser.find_element_by_css_selector('#parsys_pageHeader').get_attribute('src')
        else:
            return browser.find_element_by_css_selector('#parsys_pageheader').get_attribute('src')
    
    
    def get_synopsis(self, browser):
        if self.is_exist(browser, 'div[slot="short-description"]'):
            return browser.find_element_by_css_selector('div[slot="short-description"]').text
        return None
    
    
    def get_hrefs(self, browser):
        hrefs = []
        section = browser.find_element_by_css_selector('nui-list--content[role="list"]')
        lists = section.find_elements_by_css_selector('li>ul>li:not([class*="hidden-by-search-filtering"])')
        for li in lists:
            href = li.find_element_by_tag_name('nui-tile').get_attribute('href')
            hrefs.append(self.URL+href)
        return hrefs
    
    
    def get_episodes_hrefs(self, browser):
        episodes_deeplinks= []
        
        if self.is_exist(browser, '#parsys_container_episodes-list'):
            section = browser.find_element_by_css_selector('#parsys_container_episodes-list')
            episodes = section.find_elements_by_css_selector('vrtnu-tile')
        else:
            episodes = browser.find_elements_by_css_selector('vrtnu-tile')
        
        for episode in episodes:
            deeplinks = episode.get_attribute('link')
            episodes_deeplinks.append(deeplinks)
        
        return episodes_deeplinks
    
    
    def get_episode_content(self,content, parent_id, parent_title, rating, episode):
        payload = self.build_payload_episode(content, parent_id, parent_title, rating, episode)
        if payload != '':
            payload_episode = payload.payload_episode()
            Datamanager._checkDBandAppend(self,payload_episode,self.ids_scrapeados_episodios,self.payloads_episodes, isEpi=True)

    
    def get_movie_content(self, movie, browser):
        browser.get(movie)
        time.sleep(1)
        #Utilizo el refresh por si salta algun pop up
        #Me di cuenta que al refrescar se quita
        browser.refresh()
        time.sleep(3)
        #Saco la imagen que hay en el banner antes de acceder a la pelicula
        image = self.get_image(browser)
        #Saco la synopsis por si no esta dentro de la pelicula sucede en varios casos
        synopsis = self.get_synopsis(browser)
        browser.execute_script("return document.querySelector('vrtnu-tile').shadowRoot.querySelector('.media')").click()
        #Tiempo para que cargue la pelicula
        time.sleep(5)
        type_ = 'movie'
        is_trailer = self.is_trailer(browser, type_)
        if is_trailer == False:
            payload = self.build_payload_movie(browser, image, synopsis)
            payload_movie = payload.payload_movie()
            Datamanager._checkDBandAppend(self,payload_movie,self.ids_scrapeados,self.payloads)
         

    def get_serie_content(self,browser):
        
        time.sleep(8)
        image = self.get_image(browser)
        payload = self.build_payload_serie(browser, image)
        payload_serie = payload.payload_serie()
        Datamanager._checkDBandAppend(self,payload_serie,self.ids_scrapeados,self.payloads)   
        
    
    def build_payload_movie(self,browser, image, synopsis):
        
        def get_id():
            deeplinks = browser.current_url
            return hashlib.md5(deeplinks.encode('utf-8')).hexdigest()
        
        
        def get_title():
            return browser.execute_script("return document.querySelector('vrtnu-video-information h2').textContent")
    

        def get_year():
            div = browser.find_elements_by_class_name("vrtnu-text--default")
            year = div[0].text
            return int(year)

    
        def get_duration():
            div = browser.find_elements_by_class_name("vrtnu-text--default")
            return int(div[1].text.replace('min',''))

   
        def get_rating():
            if self.is_exist(browser,'vrtnu-icon'):
                icon_rating = browser.find_element_by_tag_name('vrtnu-icon').get_attribute('alt') 
                if icon_rating:    
                    icon_rating = icon_rating.split(' ')
                    if icon_rating[1] !='programma' and icon_rating[1] !='Placement':
                        return icon_rating[1]
            return None
    
    
        def get_genres():
            if self.is_exist(browser, '.vrtnu-text--highlighted'):
                div = browser.find_elements_by_class_name("vrtnu-text--highlighted")
                for genre in div:
                    if genre.text !='Films':
                        return [genre.text]
            return None


        def get_packages():
            """  Se hardcodeo el package porque no se encontró el dato. """
            return [{"Type":"subscription-vod"}]

        
        payload = Payload()

        payload.platform_code = self._platform_code
        payload.id = get_id()
        payload.title = get_title()
        payload.clean_title = _replace(payload.title)
        payload.deeplink_web = browser.current_url
        payload.year = get_year()
        payload.duration = get_duration()
        payload.synopsis = synopsis
        payload.rating = get_rating()
        payload.genres = get_genres()
        payload.image = [image]
        # payload.availability = get_availability(browser)
        payload.packages = get_packages()
        payload.createdAt = self._created_at

        return payload


    def build_payload_serie(self, browser, image):
        
        def get_title():
            return browser.find_element_by_css_selector('h1[slot="title"]').text

        
        def get_synopsis():
            if self.is_exist(browser,'div[slot="short-description"] > p'):
                return browser.find_element_by_css_selector('div[slot="short-description"] > p').text
            else:
                return browser.find_element_by_css_selector('div[slot="short-description"]').text
        
        
        def get_seasons():
            seasons = 0
            try:
                select = browser.find_element_by_css_selector('#parsys_container_banner_navigation select')
                options = select.find_elements_by_css_selector('option')
                for option in options:
                    if 'Seizoen' in option.text:
                        seasons+=1     
                return seasons
            except Exception:
                return 1

        
        def get_id():
            deeplinks = browser.current_url
            return hashlib.md5(deeplinks.encode('utf-8')).hexdigest()

        
        def get_packages():

            """  Se hardcodeo el package porque no se encontró el dato. """
            return [{"Type":"subscription-vod"}]


        def get_rating():
            try:
                icon_rating = browser.find_element_by_tag_name('vrtnu-icon').get_attribute('name')     
                if 'plus' in icon_rating:
                    return icon_rating.replace('plus','+')
            except Exception:
                print(f'No se encontro rating para : {browser.current_url}')
                return None
        

        payload = Payload()    
        payload.platform_code = self._platform_code
        payload.id = get_id()
        payload.title = get_title()
        payload.clean_title = _replace(payload.title)
        payload.deeplink_web = browser.current_url
        payload.image = [image]
        payload.rating = get_rating()
        payload.seasons = get_seasons()
        payload.synopsis = get_synopsis()
        payload.packages = get_packages()
        payload.rating = get_rating()
        payload.createdAt = self._created_at
        
        
        if self.is_exist(browser,'#parsys_container_banner_navigation'):
            self.recorrer_seasons(browser)
        
        seasons = browser.find_elements_by_css_selector('vrtnu-list[content="episode"]')
        n=0
        
        for season in seasons:
            episodes = self.get_episodes_hrefs(season)
            if self.is_exist(browser,'#parsys_container_banner_navigation'):
                self.change_season(browser,n)
                n+=1
            
            for episode in episodes:
                type_ = 'episode'
                is_trailer = self.is_trailer(episode,type_)
                if is_trailer== False:
                    selector = (f'vrtnu-tile[link="{episode}"]')
                    content = browser.execute_script(f"return document.querySelector('{selector}')")
                    self.get_episode_content(content, payload.id, payload.title, payload.rating, episode) 
      
        return payload
    
    
    def recorrer_seasons(self,browser):
        """
        Este metodo lo utilizo para clickear todas las seasons
        y asi agregarle el atributo [content="episode"]  
        a la etiqueta <vrtnu-list> y poder conseguir todas las seasons
        """
        select = browser.find_element_by_css_selector('#parsys_container_banner_navigation select')
        options = select.find_elements_by_css_selector('option')
        for option in options:
            if 'Seizoen' in option.text:
                browser.find_element_by_css_selector('#parsys_container_banner_navigation select').click()
                time.sleep(2)
                option.click()
                time.sleep(8)
    
    
    def change_season(self, browser, n):
        select = browser.find_element_by_css_selector('#parsys_container_banner_navigation select')
        options = select.find_elements_by_css_selector('option')
        if len(options) >n:  
            if 'Seizoen' in options[n].text:
                browser.find_element_by_css_selector('#parsys_container_banner_navigation select').click()
                time.sleep(2)
                options[n].click()
                time.sleep(8)
                          

    def build_payload_episode(self, content, parent_id, parent_title, rating, deeplinks):
        
        def get_id():
            return hashlib.md5(deeplinks.encode('utf-8')).hexdigest()
        

        def get_title():
            title = content.find_element_by_css_selector('h3[slot="title"]').text
            return title
        
        
        def get_episode():
            episode = content.find_elements_by_css_selector('span.is-hidden-below-tablet')
            episode = episode[-1].text.split(' ')
            try:
                if '/' in episode[-1]:
                    epi = get_with_deeplink('episode')
                    return int(epi)
                return int(episode[-1])
            except Exception:
                print('No es un episodio valido')
                return ''
        
        
        def get_with_deeplink(type_):
            """
            Este metodo es para obtener el numero de season y episodio por el deeplink
            ya que existen 4 capitulos que vienen con una fecha ejs 31/08 01/09
            """
            url = deeplinks
            url = url.split('/')
            url = url[-2].split('-')
            url = url[1].split('a')
            if type_ == 'episode':
                return url[-1]
            if type_ == 'season':
                return url[0].replace('s','')
        
        
        def get_season():
            season = content.find_elements_by_css_selector('span.is-hidden-below-tablet')
            season = season[0].text.split(' ')
            try:
                if season[-1] == 'Vlaams':
                    return int(season[0])
                if '/' in season[-1]:
                    seas = get_with_deeplink('season')
                    return int(seas)
                if len(season[-1]) >= 3:
                    return None
                return int(season[-1])
            except Exception:
                print('No es una season valida')
                return ''
        
        
        def get_duration():
            min = content.find_element_by_css_selector('vrtnu-label[slot="media-meta"]').text
            return int(min.replace('min',''))
        
              
        def get_synopsis():
            if self.is_exist(content,'div[slot="description"]'):
                return content.find_element_by_css_selector('div[slot="description"]').text
            return None


        def get_image():
            image = content.find_element_by_css_selector('vrtnu-image[slot="image"]').get_attribute('src')
            return [image]
        
        
        def get_packages():
            """  Se hardcodeo el package porque no se encontró el dato. """
            return [{"Type":"subscription-vod"}]
        
        
        payload = Payload()
        payload.platform_code = self._platform_code
        payload.parent_id = parent_id
        payload.parent_title = parent_title
        payload.id = get_id()
        payload.title = get_title()
        payload.episode = get_episode()
        if payload.episode == '':
            return ''
        payload.season = get_season()
        if payload.season == '':
            return ''
       
        
        payload.rating = rating
        payload.duration = get_duration()
        payload.synopsis = get_synopsis()
        payload.image = get_image()
        payload.deeplink_web = self.URL + deeplinks
        payload.packages = get_packages()
        payload.createdAt = self._created_at

        return payload
        
        