# -*- coding: utf-8 -*-
import requests  # Si el script usa requests/api o requests/bs4
import hashlib
import time
import re
from bs4 import BeautifulSoup  # Si el script usa bs4
from selenium import webdriver  # Si el script usa selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
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
    - Método: SELENIUM
    - Runtime: 1h 10m
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
        self.options = Options().add_argument("--headless")
        self.driver = webdriver.Chrome("./drivers/chromedriver.exe", options=self.options)
        self.regex = re.compile(r'[a-z]\d+')


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
            payload.seasons = self.get_seasons(content_metadata)
            payload.year = self.get_year(content_metadata)
            payload.genres = self.get_genres(content_metadata)
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
        payload = Payload()
        for item in movies:
            if item.is_displayed():
                movie_links.append(item.get_attribute("href"))
        for movie_link in movie_links:
            url = f'https://www.vrt.be{movie_link}'
            #Se hardcodeó el skip de esta pelicula ya que sólo trae trailer el día de la fecha 08/09/2021
            if "/a-z/grace-a-dieu/" in url:
                continue
            if "audiodescriptie" in movie_link:
                continue
            self.driver.get(url)
            time.sleep(2)
            i_l = []
            #Se puede usar el payload, use este formato porque me era más comodo seguir qué estaba extrayendo
            metadata = payload.payload_season()
            try:
                if EC.element_to_be_clickable((By.ID, "qlf-close-button")):
                    self.driver.find_element_by_id("qlf-close-button").click()
            except Exception:
                pass
            try:
                for span in self.driver.find_elements_by_css_selector("div[slot='category']"):
                    if span.text != "Films" and span.text != "/":
                        metadata["Genres"] = span.text
                    else:
                        metadata["Genres"] = None
            except Exception:
                pass
            time.sleep(1)
            if "trailer" in self.driver.current_url:
                continue
            if self.driver.find_elements_by_css_selector('div[slot="short-description"] > p'):
                metadata["Synopsis"] = self.driver.find_element_by_css_selector('div[slot="short-description"] > p').text
            else:
                metadata["Synopsis"] = self.driver.find_element_by_css_selector('div[slot="short-description"]').text
            metadata["Title"] = self.driver.find_element_by_tag_name("h1").text
            try:
                if "https://" in self.driver.find_element_by_tag_name("vrtnu-image").get_attribute("src"):
                    i_l.append(self.driver.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
                else:
                    i_l.append("https:"+self.driver.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
            except Exception:
                pass
            try:
                if "Trailer" in self.driver.find_element_by_tag_name("h2").text:
                    continue
                else:
                    metadata["Year"] = int(self.driver.find_element_by_tag_name("h2").text)
            except:
                pass
            self.driver.get(self._base_url + self.driver.find_element_by_tag_name("vrtnu-tile").get_attribute("link"))
            time.sleep(0.5)
            metadata["deeplink_web"] = url
            metadata["Id"] = self.hashing(url)
            metadata["Duration"] = int(str(self.driver.find_elements_by_class_name("vrtnu-text--default")[1].text).split(" ")[0])
            print(metadata["Duration"])
            metadata["Image"] = i_l
            payload = self.get_payload(metadata)
            metadata["Synopsis"] = ""
            payload_movie = payload.payload_movie()
            Datamanager._checkDBandAppend(self, payload_movie, self.ids_scrapeados, self.payloads)
            
            
    def get_id(self, content_metadata):
        return content_metadata.get('Id')
    
    def get_original_title(self, content_metadata):
        pass
    
    def get_clean_title(self, content_metadata):
        return _replace(content_metadata.get("Title")) or None
    
    def get_year(self, content_metadata):
        if isinstance(content_metadata.get("Year"), int):
            print(content_metadata.get("Year"))
            return content_metadata.get("Year")
        else:
            return None
    
    def get_duration(self, content_metadata):
        return content_metadata.get("Duration") or None
    
    def get_deeplinks(self, content_metadata):
        if "#nieuwsbrief" in content_metadata.get("deeplink_web"):
            return str(content_metadata.get("deeplink_web")).replace("#nieuwsbrief", "")
        return content_metadata.get("deeplink_web") or None
    
    def get_synopsis(self, content_metadata):
        if content_metadata.get("Synopsis"):
            print(content_metadata.get("Synopsis"))
        return content_metadata.get("Synopsis") or None
        
    def get_title(self, content_metadata):
        return content_metadata.get('Title') or None
    
    def get_genres(self, content_metadata):
        
        if content_metadata.get('Genres'):
            genre = content_metadata.get('Genres').replace("Series / ", "").replace("Films / ", "")
            if "/" in genre:
                print(genre.split(" / "))
                return genre.split(" / ")
            print(genre)
            return [genre]
        else:
            return None
    
    def get_images(self, content_metadata):
        return content_metadata.get('Image') or None
    
    def get_season(self, content_metadata):
        return content_metadata.get('Season') or None
    
    def get_episode(self, content_metadata):
        return content_metadata.get("Number") or None
    
    def get_series(self):
        self.driver.get(self._url_series)
        series = self.driver.find_elements_by_tag_name("nui-tile")
        serie_links = []
        
        for item in series:
            if item.is_displayed():
                serie_links.append(item.get_attribute("href"))
        for serie_link in serie_links:
            payload = Payload()
            if "audiodescriptie" in serie_link:
                continue
            
            if "Making of" in self.driver.find_element_by_tag_name("h2").text:
                continue
            url = f'https://www.vrt.be{serie_link}'
            self.driver.get(url)
            time.sleep(10)
            i_l = []
            #Se puede usar el payload, use este formato porque me era más comodo seguir qué estaba extrayendo
            metadata = payload.payload_serie()
            try:
                if EC.element_to_be_clickable((By.ID, "qlf-close-button")):
                    self.driver.find_element_by_id("qlf-close-button").click()
                    time.sleep(3)
            except Exception:
                pass
            metadata["Id"] = self.hashing(self.driver.current_url)
            metadata["Title"] = self.driver.find_element_by_tag_name("h1").text
            if self.driver.find_elements_by_css_selector('div[slot="short-description"] > p'):
                metadata["Synopsis"] = self.driver.find_element_by_css_selector('div[slot="short-description"] > p').text
            else:
                metadata["Synopsis"] = self.driver.find_element_by_css_selector('div[slot="short-description"]').text
            for span in self.driver.find_elements_by_css_selector("div[slot='category']"):
                    if span.text != "Series" and span.text != "/":
                        metadata["Genres"] = span.text
                    else:
                        metadata["Genres"] = None
            seasons = []
            if self.driver.find_elements_by_tag_name('select'):
                tempos = self.driver.find_elements_by_tag_name('option')
                epi_act = 0
                for tempo in tempos:
                    season = payload.payload_season()
                    self.driver.find_element_by_tag_name("select")
                    
                    if "Seizoen" in tempo.text or "Vlaams" in tempo.text:
                        tempo.click()
                        
                        episodes = self.driver.find_elements_by_tag_name('vrtnu-tile')
                        if epi_act:
                                del episodes[0:epi_act]
                        time.sleep(2)
                        split_title = str(tempo.text).split(" (")
                        try:
                            season["Id"] = self.hashing(self.driver.current_url+split_title[0])
                            season["Title"]= split_title[0]
                            if "Seizoen" in tempo.text:
                                season["Number"] = int(split_title[0].replace("Seizoen ", ""))
                            else:
                                season["Number"] = int(split_title[0].replace("Vlaams ", ""))
                            season["Episodes"] = len(episodes)
                        except Exception:
                            pass
                        seasons.append(season)
                        epi_act = epi_act + len(episodes)
                        parent_id = metadata["Id"]
                        parent_title = metadata["Title"]
                        self.get_episodes(episodes, parent_id=parent_id, parent_title=parent_title)
                    else:
                        tempo.click()
                        time.sleep(2)
                        episodes = self.driver.find_elements_by_tag_name('vrtnu-tile')
                        if epi_act:
                                del episodes[0:epi_act]
                        epi_act = epi_act + len(episodes)
            else:
                episodes = self.driver.find_elements_by_tag_name('vrtnu-tile')
                season = payload.payload_season()
                if len(episodes) == 1:
                    try:
                        season["Title"] = self.driver.find_element_by_tag_name("h2").text
                        season["Id"] = self.hashing(self.driver.current_url+split_title[0])
                        season["Number"] = 1
                        season["Episodes"] = len(episodes)
                        metadata["Seasons"] = [season]
                    except Exception:
                        pass;
                else:
                    try:
                        split_title = str(self.driver.find_element_by_tag_name("h2").text).split(" (")
                        season["Id"] = self.hashing(self.driver.current_url+split_title[0])
                        season["Title"]= split_title[0]
                        if "compilatie" in split_title[0]:
                            season["Number"] = int(split_title[0].replace(" compilatie", ""))
                        else:    
                            season["Number"] = int(split_title[0].replace("Seizoen ", ""))
                        season["Episodes"] = len(episodes)
                        metadata["Seasons"] = [season]
                    except Exception:
                        pass
                parent_id = metadata["Id"]
                parent_title = metadata["Title"]
                self.get_episodes(episodes, parent_id=parent_id, parent_title=parent_title)
            try:
                if "https://" in self.driver.find_element_by_tag_name("vrtnu-image").get_attribute("src"):
                    i_l.append(self.driver.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
                else:
                    i_l.append("https:" + self.driver.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
            except Exception:
                pass
            try:
                metadata["Year"] = int(self.driver.find_element_by_tag_name("h2").text)
            except Exception:
                print("No se encontró AÑO")
            time.sleep(0.5)
            if len(episodes) == 0:
                continue
            metadata["deeplink_web"] = self.driver.current_url
            #metadata["duration"] = str(self.driver.find_elements_by_class_name("vrtnu-text--default")[1].text).split(" ")[0]
            metadata["Image"] = i_l
            if seasons:
                metadata["Seasons"] = seasons
            if metadata["Seasons"]:
                payload = self.get_payload(metadata)

                payload_serie = payload.payload_serie()
                Datamanager._checkDBandAppend(self, payload_serie, self.ids_scrapeados, self.payloads)
    
    def get_episodes(self, epis, parent_id=None, parent_title=None):
        payload = Payload()
        cap_num = 0
        for epi in epis:
            if epi.find_element_by_tag_name("h3").text == "":
                continue
            
            if "Making of" in epi.find_element_by_tag_name("h3").text:
                continue
            #Se puede usar el payload, use este formato porque me era más comodo seguir qué estaba extrayendo
            metadata = payload.payload_episode()
            metadata["ParentTitle"] = parent_title
            try:
                metadata["Synopsis"] = epi.find_element_by_css_selector('div[slot="description"]').text
            except Exception:
                pass
            img = []
            try:
                if "https://" in epi.find_element_by_tag_name("vrtnu-image").get_attribute("src"):
                    img.append(epi.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
                else:
                    img.append("https:" + epi.find_element_by_tag_name("vrtnu-image").get_attribute("src"))
            except Exception:
                pass
            metadata["Image"] = img
            metadata["ParentId"] = parent_id
            metadata["Id"] = self.hashing(self._base_url + epi.get_attribute("link"))
            try:
                metadata["Title"] = epi.find_element_by_tag_name("h3").text
                find = self.regex.findall(epi.get_attribute("link"))
                cap_num += 1
                if len(find) == 2:
                    metadata["Season"] = int(str(find[0].replace("s", "")))
                    metadata["Number"] = int(str(find[1].replace("a", "")))
                elif len(find) == 1:
                    metadata["Season"] = 1
                    metadata["Number"] = int(str(find[0].replace("a", "")))
                else:
                    metadata["Season"] = 1
                    metadata["Number"] = cap_num
                metadata["Duration"] = int(str(epi.find_element_by_tag_name("vrtnu-label").text).replace(" min", ""))
            except Exception:
                pass
            try:
                element = epi.find_element_by_tag_name("vrtnu-meta")
            except Exception:
                pass
            if "trailer" in epi.get_attribute("link") or "blooper" in epi.get_attribute("link") or "extra" in epi.get_attribute("link"):
                continue
            if metadata.get("Number"):
                metadata["deeplink_web"] = self._base_url + epi.get_attribute("link")
                payload = self.get_payload(metadata, is_episode=True)
                payload_episode = payload.payload_episode()
                Datamanager._checkDBandAppend(self,payload_episode,self.ids_episcrap,self.payloads_episodes,isEpi=True)
            
    def get_parent_id(self, content_metadata):
        return content_metadata.get('ParentId') or None
    
    def get_seasons(self, content_metadata):
        print(content_metadata.get("Seasons"))
        if content_metadata.get("Seasons"):
            return content_metadata.get('Seasons')
        else:
            return None
    
    def get_parent_title(self, content_metadata):
        return content_metadata.get('ParentTitle') or None
    
    def get_packages(self):
        return [{'Type':'free-vod'}]
    
    def scraping(self):
        #self.get_movies()
        self.get_series()
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        
        Upload(self._platform_code,self._created_at,testing=True)
