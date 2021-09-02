# -*- coding: utf-8 -*-
import requests  # Si el script usa requests/api o requests/bs4
import hashlib
import time
from bs4 import BeautifulSoup  # Si el script usa bs4
from selenium import webdriver  # Si el script usa selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# Opcional si el script usa Datamanager
from handle.datamanager import Datamanager
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from handle.replace import _replace
from handle.payload import Payload


class VRTNuFede():

    """
    - Status: EN PROCESO
    - VPN: NO
    - Método:
    - Runtime:
    """

    def __init__(self, ott_platforms, ott_site_country, ott_operation):
        self.test = ott_operation in ("testing", "return")
        config_ = config()['ott_sites'][ott_platforms]  # Obligatorio
        # Opcional, puede ser útil dependiendo de la lógica del script.
        self.country = ott_site_country
        self._created_at = time.strftime('%Y-%m-%d')
        self._platform_code = config_['countries'][ott_site_country]
        self.mongo = mongo()
        self.sesion = requests.session()  # Requerido si se va a usar Datamanager
        self.titanPreScraping = config(
        )['mongo']['collections']['prescraping']  # Opcional
        # Obligatorio. También lo usa Datamanager
        self.titanScraping = config()['mongo']['collections']['scraping']
        # Obligatorio. También lo usa Datamanager
        self.titanScrapingEpisodios = config(
        )['mongo']['collections']['episode']
        self.skippedTitles = 0  # Requerido si se va a usar Datamanager
        self.skippedEpis = 0  # Requerido si se va a usar Datamanager
        self._url_movies = config_['url_movies']
        self._url_series = config_['url_series']
        self._base_url = config_['base_url']
        self.payloads = []
        self.payloads_episodes = []
        self.ids_scrapeados = Datamanager._getListDB(self, self.titanScraping)
        self.ids_episcrap = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)
        self.driver = webdriver.Chrome("./drivers/chromedriver.exe")

        # OBLIGATORIO si se usa Selenium para que pueda correr en los servers
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
            return_params = {'PlatformCode': self._platform_code}
            last_item = self.mongo.lastCretedAt(
                'titanPreScraping', return_params)
            if last_item.count() > 0:
                for last_content in last_item:
                    self._created_at = last_content['CreatedAt']
            self.prescraped_ids = [
                pay["Id"] for pay in Datamanager._getListDB(self, self.titanPreScraping)]

            self.scraping()
        else:
            self.prescraped_ids = list()

        if ott_operation in ('testing', 'scraping'):
            self.scraping()

    def hashing(self, var):
        return hashlib.md5(var.encode("utf-8")).hexdigest()
        

    def get_payload(self, content_metadata, is_episode=None):
            """Método para crear el payload. Se reutiliza tanto para
            titanScraping, como para titanScrapingEpisodes.
            Args:
                content_metadata (dict): Indica la metadata del contenido.
                is_episode (bool, optional): Indica si hay que crear un payload
                que es un episodio. Defaults to False.
            Returns:
                Payload: Retorna el payload.
            """
            payload = Payload()
            payload.platform_code = self._platform_code
            payload.id = self.get_id(content_metadata)
            payload.title = self.get_title(content_metadata)
            payload.original_title = self.get_original_title(content_metadata)
            payload.clean_title = self.get_clean_title(content_metadata)
            payload.deeplink_web = self.get_deeplinks(content_metadata)
            if is_episode:
                payload.parent_title = None
                payload.parent_id = None
                payload.season = self.get_season(content_metadata)
                payload.episode = self.get_episode(content_metadata)
    
            payload.year = self.get_year(content_metadata)
            payload.duration = self.get_duration(content_metadata)
            payload.synopsis = self.get_synopsis(content_metadata)
            payload.image = self.get_images(content_metadata)
            payload.packages = self.get_packages()
            payload.createdAt = self._created_at
            return payload

    def get_clean_title(self, content_metadata):
        pass
    

    def get_movies(self):
        self.driver.get(self._url_movies)
        movies = self.driver.find_elements_by_tag_name("nui-tile")
        movie_links = []
        for item in movies:
            if item.is_displayed():
                movie_links.append(item.get_attribute("href"))
        for movie_link in movie_links:
            url = f'https://www.vrt.be{movie_link}'
            self.driver.get(url)
            i_l = []
            metadata = {
                "id":None,
                "deeplink":None,
                "title":None,
                "desc":None,
                "year":None,
                "genre":None,
                "duration":None,
                "images":None
            }

            try:
                if EC.element_to_be_clickable((By.ID, "qlf-close-button")):
                    self.driver.find_element_by_id("qlf-close-button").click()
            except:
                pass
            try:
                if EC.presence_of_element_located((By.CLASS_NAME, "has-ellipsis")):
                    metadata["desc"] = self.driver.find_element_by_class_name("has-ellipsis").text
                else:
                    metadata["desc"] = self.driver.find_element_by_xpath("/html/body/main/div/div[1]/vrtnu-page-header/div[1]/p").text
            except:
                pass
            time.sleep(0.5)
            metadata["title"] = self.driver.find_element_by_tag_name("h1").text
            i_l.append(self.driver.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
            metadata["year"] = self.driver.find_element_by_tag_name("h2").text
            self.driver.get(self._base_url + self.driver.find_element_by_tag_name("vrtnu-tile").get_attribute("link"))
            time.sleep(0.5)
            metadata["deeplink"] = self.driver.current_url
            metadata["id"] = self.hashing(self.driver.current_url)
            metadata["duration"] = str(self.driver.find_elements_by_class_name("vrtnu-text--default")[1].text).split(" ")[0]
            metadata["images"] = i_l
            payload = self.get_payload(metadata)
            payload_movie = payload.payload_movie()
            Datamanager._checkDBandAppend(self, payload_movie, self.ids_scrapeados, self.payloads)
            
            
    def get_id(self, content_metadata):
        return content_metadata.get('id')
    
    def get_original_title(self, content_metadata):
        pass
    
    def get_clean_title(self, content_metadata):
        return _replace(content_metadata.get("title"))
    
    def get_year(self, content_metadata):
        return content_metadata.get("year") or None
    
    def get_duration(self, content_metadata):
        return content_metadata.get("duration") or None
    
    def get_deeplinks(self, content_metadata):
        return content_metadata.get("deeplink") or None
    
    def get_synopsis(self, content_metadata):
        return content_metadata.get("desc") or None
        
    def get_title(self, content_metadata):
        print(content_metadata.get("title"))
        return content_metadata.get('title')
    
    def get_images(self, content_metadata):
        return content_metadata.get('images')
    
    def get_series(self):
        self.driver.get(self._url_series)
        series = self.driver.find_elements_by_tag_name("nui-tile")
        serie_links = []
        for item in series:
            if item.is_displayed():
                serie_links.append(item.get_attribute("href"))
        for serie_link in serie_links:
            url = f'https://www.vrt.be{serie_link}'
            self.driver.get(url)
            i_l = []
            metadata = {
                "id":None,
                "deeplink":None,
                "title":None,
                "desc":None,
                "year":None,
                "genre":None,
                "duration":None,
                "images":None,
                "seasons":None
            }
            try:
                if EC.element_to_be_clickable((By.ID, "qlf-close-button")):
                    self.driver.find_element_by_id("qlf-close-button").click()
            except:
                pass
            time.sleep(0.5)
            link_episodes = []
            if self.driver.find_elements_by_tag_name('select'):
                tempos = self.driver.find_elements_by_tag_name('option')
                n_seasons = len(tempos)
                for tempo in tempos:
                    tempo.click()
                    episodes = self.driver.find_elements_by_tag_name('vrtnu-tile')
                    for link_epi in episodes:
                        link_episodes.append(self._base_url + link_epi.get_attribute("link"))
                    
                    
            metadata["title"] = self.driver.find_element_by_tag_name("h1").text
            i_l.append(self.driver.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
            metadata["year"] = self.driver.find_element_by_tag_name("h2").text
            self.driver.get(self._base_url + self.driver.find_element_by_tag_name("vrtnu-tile").get_attribute("link"))
            time.sleep(0.5)
            metadata["deeplink"] = self.driver.current_url
            metadata["id"] = self.hashing(self.driver.current_url)
            metadata["duration"] = str(self.driver.find_elements_by_class_name("vrtnu-text--default")[1].text).split(" ")[0]
            metadata["images"] = i_l
            payload = self.get_payload(metadata)
            payload_serie = payload.payload_serie()
            Datamanager._checkDBandAppend(self, payload_serie, self.ids_scrapeados, self.payloads)
            
        pass
    
    def get_packages(self):
        return [{'Type':'free-vod'}]
    
    def scraping(self):
        #self.get_movies()
        self.get_series()
        #Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        #Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        
        Upload(self._platform_code,self._created_at,testing=True)
