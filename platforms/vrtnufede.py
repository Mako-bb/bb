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
                payload.parent_title = self.get_parent_title(content_metadata)
                payload.parent_id = self.get_parent_id(content_metadata)
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
        return _replace(content_metadata.get("title"))
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
            if "audiodescriptie" in movie_link:
                continue
            if "trailer" in movie_link:
                continue
            self.driver.get(url)
            i_l = []
            #Se puede usar el payload, use este formato porque me era más comodo seguir qué estaba extrayendo
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
                metadata["genre"] = str(self.driver.find_element_by_css_selector("div[slot='category']").text).replace("Films", "").replace(" / ", ",").split(",")
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
        return _replace(content_metadata.get("title")) or None
    
    def get_year(self, content_metadata):
        return content_metadata.get("year") or None
    
    def get_duration(self, content_metadata):
        return content_metadata.get("duration") or None
    
    def get_deeplinks(self, content_metadata):
        return content_metadata.get("deeplink") or None
    
    def get_synopsis(self, content_metadata):
        return content_metadata.get("desc") or None
        
    def get_title(self, content_metadata):
        return content_metadata.get('title') or None
    
    def get_images(self, content_metadata):
        return content_metadata.get('images') or None
    
    def get_season(self, content_metadata):
        return content_metadata.get('season') or None
    
    def get_episode(self, content_metadata):
        return content_metadata.get("number") or None
    
    def get_series(self):
        self.driver.get(self._url_series)
        series = self.driver.find_elements_by_tag_name("nui-tile")
        serie_links = []
        for item in series:
            if item.is_displayed():
                serie_links.append(item.get_attribute("href"))
        for serie_link in serie_links:
            if "audiodescriptie" in serie_link:
                continue
            url = f'https://www.vrt.be{serie_link}'
            self.driver.get(url)
            time.sleep(10)
            if "Making of" in self.driver.find_element_by_tag_name("h2").text:
                continue
            
            i_l = []
            #Se puede usar el payload, use este formato porque me era más comodo seguir qué estaba extrayendo
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
            time.sleep(3)
            seasons = []
            metadata["id"] = self.hashing(self.driver.current_url)
            metadata["title"] = self.driver.find_element_by_tag_name("h1").text
            metadata["genre"] = str(self.driver.find_element_by_css_selector("div[slot='category']").text).replace("Series", "").replace(" / ", ",").split(",")
            if self.driver.find_elements_by_tag_name('select'):
                tempos = self.driver.find_elements_by_tag_name('option')
                n_seasons = len(tempos)
                epi_act = 0
                for tempo in tempos:
                    self.driver.find_element_by_tag_name("select")
                    if "Seizoen" in tempo.text:
                        tempo.click()
                        time.sleep(2)
                        episodes = self.driver.find_elements_by_tag_name('vrtnu-tile')
                        if epi_act:
                                del episodes[0:epi_act]
                        season = {
                            "title":None,
                            "number":None,
                            "episodes":None
                        }
                        try:
                            split_title = str(tempo.text).split(" (")
                            season["title"]= split_title[0]
                            season["number"] = int(split_title[0].replace("Seizoen ", ""))
                            season["episodes"] = int(split_title[1].replace(" afleveringen)", ""))
                            seasons.append(season)
                        except:
                            pass
                        parent_id = metadata["id"]
                        parent_title = metadata["title"]
                        self.get_episodes(episodes, parent_id=parent_id, parent_title=parent_title)
                        epi_act = epi_act + len(episodes)
            else:
                episodes = self.driver.find_elements_by_tag_name('vrtnu-tile')
                season = {
                            "title":None,
                            "number":None,
                            "episodes":None
                        }
                try:
                    split_title = str(self.driver.find_element_by_tag_name("h2").text).split(" (")
                    season["title"]= split_title[0]
                    season["number"] = int(split_title[0].replace("Seizoen ", ""))
                    season["episodes"] = len(episodes)
                    seasons.append(season)
                except:
                    pass
                parent_id = metadata["id"]
                parent_title = metadata["title"]
                self.get_episodes(episodes, parent_id=parent_id, parent_title=parent_title)
            metadata["seasons"] = seasons
            i_l.append(self.driver.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
            metadata["year"] = self.driver.find_element_by_tag_name("h2").text
            time.sleep(0.5)
            metadata["deeplink"] = self.driver.current_url
            #metadata["duration"] = str(self.driver.find_elements_by_class_name("vrtnu-text--default")[1].text).split(" ")[0]
            metadata["images"] = i_l
            payload = self.get_payload(metadata)
            payload_serie = payload.payload_serie()
            Datamanager._checkDBandAppend(self, payload_serie, self.ids_scrapeados, self.payloads)
    
    def get_episodes(self, epis, parent_id=None, parent_title=None):
        for epi in epis:
            time.sleep(3)
            if epi.find_element_by_tag_name("h3").text == "":
                continue
            #Se puede usar el payload, use este formato porque me era más comodo seguir qué estaba extrayendo
            metadata = {
                "id":None,
                "title":None,
                "duration":None,
                "number":None,
                "images":None,
                "season":None,
                "desc":None,
                "deeplink":None,
                "parent_title":parent_title,
                "parent_id":parent_id
            }
            metadata["parent_title"] = parent_title
            try:
                metadata["desc"] = epi.find_element_by_css_selector('div[slot="description"]').text
            except:
                pass
            img = []
            img.append(epi.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
            metadata["images"] = img
            metadata["parent_id"] = parent_id
            details = str(epi.find_element_by_tag_name("vrtnu-meta").text).replace("Seizoen ", "s-").replace("Aflevering ", "a-").split("\n")
            metadata["id"] = self.hashing(self._base_url + epi.get_attribute("link"))
            print("------------------------------------------")
            print(metadata["parent_id"])
            print(parent_title)
            print("------------------------------------------")
            try:
                metadata["title"] = epi.find_element_by_tag_name("h3").text
                metadata["season"] = int(str(details[0].replace("s-", "")))
                metadata["number"] = int(str(details[1].replace("a-", "")))
                metadata["duration"] = int(str(epi.find_element_by_tag_name("vrtnu-label").text).replace(" min", ""))
            except:
                pass
            try:
                element = epi.find_element_by_tag_name("vrtnu-meta")
                print(epi.find_element_by_selector(f'#parsys_container_episodes-list_1-{metadata["number"]-1} > div').text)
            except:
                pass
            if "trailer" in epi.get_attribute("link") or "blooper" in epi.get_attribute("link"):
                continue
            metadata["deeplink"] = self._base_url + epi.get_attribute("link")
            print(metadata)
            payload = self.get_payload(metadata, is_episode=True)
            payload_episode = payload.payload_episode()
            Datamanager._checkDBandAppend(self,payload_episode,self.ids_episcrap,self.payloads_episodes,isEpi=True)
            
    def get_parent_id(self, content_metadata):
        return content_metadata.get('parent_id') or None
    
    def get_parent_title(self, content_metadata):
        return content_metadata.get('parent_title') or None
    
    def get_packages(self):
        return [{'Type':'free-vod'}]
    
    def scraping(self):
        #self.get_movies()
        self.get_series()
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        
        Upload(self._platform_code,self._created_at,testing=True)
