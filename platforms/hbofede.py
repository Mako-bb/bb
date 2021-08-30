# -*- coding: utf-8 -*-
import requests  # Si el script usa requests/api o requests/bs4
import time
import json
import random as rand
import string
from bs4 import BeautifulSoup  # Si el script usa bs4
from selenium import webdriver  # Si el script usa selenium
# Opcional si el script usa Datamanager
from handle.datamanager import Datamanager
from common import config
from handle.mongo import mongo
from updates.upload import Upload
from handle.replace import _replace
from handle.payload import Payload


class HBOFede():

    """
    - Status: EN PROCESO
    - VPN: NO
    - Método: Mixto; Requests y BeautifulSoup
    - Runtime: ?
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
        self._url = config_['url']
        self.payloads = []
        self.payloads_episodes = []
        self.ids_scrapeados = Datamanager._getListDB(self, self.titanScraping)
        self.ids_episcrap = Datamanager._getListDB(
            self, self.titanScrapingEpisodios)

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

    def get_movies(self):
        """Obtiene todas las peliculas
        """
        res = requests.get(self._url
                           + "/movies/catalog")
        soup = BeautifulSoup(
            res.text, "lxml")
        noscript = soup.find("noscript")
        d = json.loads(noscript["data-state"])
        for movie in d["bands"][1]["data"]["entries"]:
            # Chequeamos si existe link
            if("moviePageUrl" in movie):
                # Chequeamos si tiene Streaming ID, porque de no tenerlo.. no se puede ver. Skipeamos
                if("streamingId" in movie):
                    res = requests.get(self._url + movie["moviePageUrl"])
                    soup = BeautifulSoup(res.text, features="html.parser")
                    movie_data = soup.find(
                        "div", {"class": "components/MainContent--mainContent"})
                    if movie_data.find("div", {"class": "modules/Text--text"}) != None:
                        # Si el contenido no está vacio, procedemos a ingresar los datos
                        payload = self.get_payload(movie)
                        payload_movie = payload.payload_movie()
                        Datamanager._checkDBandAppend(
                            self, payload_movie, self.ids_scrapeados, self.payloads)

    def get_series(self):
        """Obtiene todas las series
        """
        res = requests.get(self._url
                           + "/series/all-series")
        soup = BeautifulSoup(
            res.text, "lxml")
        noscript = soup.find("noscript")
        d = json.loads(noscript["data-state"])
        for serie in d["bands"][1]["data"]["entries"]:
            # Chequeamos si existe link
            if("cta" in serie):
                # Chequeamos si tiene Streaming ID, porque de no tenerlo.. no se puede ver. Skipeamos
                if("streamingId" in serie):
                    res = requests.get(self._url + serie["cta"]["href"])
                    soup = BeautifulSoup(res.text, features="html.parser")
                    serie_data = soup.find(
                        "div", {"class": "components/MainContent--mainContent"})
                    if serie_data.find("div", {"class": "components/RichText--richText"}) != None:
                        # Si el contenido no está vacio, procedemos a ingresar los datos
                        payload = self.get_payload(serie, is_serie=True)
                        payload_serie = payload.payload_serie()
                        Datamanager._checkDBandAppend(
                            self, payload_serie, self.ids_scrapeados, self.payloads)

    def get_payload(self, content_metadata, is_episode=None, is_serie=None, parent_id=None, parent_title=None):
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
            # Indica si el payload a completar es un episodio:
            if is_episode:
                self.is_episode = True
            else:
                self.is_episode = False
            payload.platform_code = self._platform_code
            payload.id = self.get_id(content_metadata)
            payload.title = self.get_title(content_metadata)
            payload.original_title = self.get_original_title(content_metadata)
            payload.clean_title = self.get_clean_title(content_metadata)
            payload.deeplink_web = self.get_deeplinks(content_metadata)
            if self.is_episode:
                payload.parent_title = parent_title
                payload.parent_id = parent_id
                payload.season = self.get_season(content_metadata)
                payload.episode = self.get_episode(content_metadata)
            if is_serie:
                payload.seasons = self.get_seasons(content_metadata)
            payload.year = self.get_year(content_metadata)
            payload.duration = self.get_duration(content_metadata, is_serie, is_episode)
            payload.synopsis = self.get_synopsis(content_metadata)
            payload.image = self.get_images(content_metadata)
            payload.rating = self.get_ratings(content_metadata)
            payload.genres = self.get_genres(content_metadata)
            payload.cast = self.get_cast(content_metadata)
            payload.crew = self.get_crew(content_metadata)
            payload.availability = self.get_availability(content_metadata, is_serie, is_episode)
            payload.is_original = self.get_is_original(content_metadata)
            payload.packages = self.get_packages(content_metadata)
            payload.createdAt = self._created_at
            return payload

    def get_id(self, content_metadata):
        """Obtiene el ID desde la metadata, de no obtenerlo.. se genera

        Args:
            content_metadata (dict): Metadata del contenido

        Returns:
            str: ID del contenido
        """
        random = ''.join([rand.choice(string.ascii_letters
                                                    + string.digits) for n in range(32)])
        return content_metadata.get("streamingId").get("id")
        pass

    def get_title(self, content_metadata):
        """Obtiene el título desde la metadata del contenido

        Args:
            content_metadata (dict): Metadata del contenido

        Returns:
            str: Devuelve el titulo
        """
        return content_metadata.get("title") or content_metadata.get("data").get("episode").get("title") or None
        pass

    def get_clean_title(self, content_metadata):
        return _replace(content_metadata.get("title")) or None

    def get_original_title(self, content_metadata):
        title = str(content_metadata["title"]).strip()
        index = None
        if "(" in title:
            index = title.index("(")
            title = title[index:len(title)-1]
        return title

    def get_year(self, content_metadata):
        #Como el releaseDate devuelve 10 digitos, se hace un substring de los primeros 4 para obtener el año
        return str(content_metadata.get("releaseDate"))[0:4] or None

    def get_duration(self, content_metadata, is_serie, is_episode):
        """Obtiene la duración y de estar en horas, la cambia a minutos

        Args:
            content_metadata (dict): Metadata del contenido
            is_serie (bool): Indica si es serie
            is_episode (bool): Indica si es un episodio

        Returns:
            [type]: [description]
        """
        format_time = 0
        mins = 0
        if is_serie or is_episode:
            return format_time
        else:
            if "HR" in content_metadata["duration"]:
                hr = content_metadata["duration"].split(" ")[0]
                format_time = int(hr)*60
            if "MIN" in content_metadata["duration"]:
                if 'HR' in content_metadata["duration"]:
                    mins = int(content_metadata["duration"].split(' ')[2])
                else:
                    mins = int(content_metadata["duration"].split(' ')[0])
            format_time = format_time + int(mins)
        return format_time

    def get_deeplinks(self, content_metadata):
        """Obtiene el link del contenido desde la metadata

        Args:
            content_metadata (dict): Metadata del contenido

        Returns:
            str: Link del contenido
        """
        return content_metadata.get("cta").get("href") or content_metadata.get("data").get("episode").get("cta").get("href") or None
    
    
    
    
    def get_synopsis(self, content_metadata):
        """Obtiene la synopsis desde la metadata del contenido

        Args:
            content_metadata (dict): Metadata del contenido

        Returns:
            str: Synopsis del contenido
        """
        return content_metadata.get("synopsis") or content_metadata.get("data").get("episode").get("summary") or None
    
    
    
    
    def get_images(self, content_metadata):
        """Obtiene las imagenes desde la metadata del contenido

        Args:
            content_metadata (dict): Metadata del contenido

        Returns:
            list: Lista de imagenes
        """
        images = []
        if "thumbnail" in content_metadata:
            for image in content_metadata["thumbnail"]["images"]:
                images.append(image["src"])
        elif "cards" in content_metadata:
            for image in content_metadata["cards"][0]["image"]["images"]:
                images.append(image["src"])
        return images
    
    
    
    
    def get_ratings(self, content_metadata):
        #Obtiene el rating desde el content_metadata
        return content_metadata.get("rating") or None
        
        
        
        
        
    def get_genres(self, content_metadata):
        #Obtiene el genero desde el content_metadata
        return content_metadata.get("genres") or None
        
        
        
        
    def get_cast(self, content_metadata):
        """Obtiene el cast desde la metadata del contenido

        Args:
            content_metadata (dict): Metadata del contenido

        Returns:
            list: Cast
        """
        cast = []
        try:
            res = requests.get(self._url + content_metadata["cta"]["href"] + "/cast-and-crew")
            soup = BeautifulSoup(res.text, "lxml")
            d = soup.find("noscript")
            d = json.loads(d["data-state"])
            #Buscamos la propiedad groups, ya que si no la tiene, no tiene cast
            if "groups" in d["bands"][1]["data"]:
                for member in d["bands"][1]["data"]["groups"][0]["members"]:
                    cast.append(member["byline"])
        except:
            pass
        return cast
    
    def get_crew(self, content_metadata):
        crew = []
        person = {
            "Role":None,
            "Name":None
        }
        res = requests.get(self._url + content_metadata["cta"]["href"] + "/cast-and-crew")
        soup = BeautifulSoup(res.text, "lxml")
        d = soup.find("noscript")
        d = json.loads(d["data-state"])
        #Chequeamos si tiene más de 2 indices, de ser así, podemos acceder
        if len(d["bands"]) > 2:
            #Acá accedemos al segundo para preguntar por la propiedad "Crew"
                if "Crew" in d["bands"][2]["band"]:
                    for member in d["bands"][2]["data"]["groups"][0]["categories"][0]["members"]:
                        person = {
                            "Role":None,
                            "Name":None
                            }
                        #Preguntamos si existe contenido en los atributos
                        if member["role"] != None or member["name"] != None:
                            person["Role"] = str(member.get("role")).replace("/",",").strip()
                            person["Name"] = member.get("name").strip()
                            crew.append(person)
        elif "cta" in d["bands"][4]["data"]:
            res = requests.get(self._url + d["cta"]["href"] + "/cast-and-crew")
            soup = BeautifulSoup(res.text, features="lxml")
            d = soup.find("noscript")
            d = d.json()
            if len(d["bands"]) > 2:
                if "Crew" in d["bands"][2]["band"]:
                    for member in d["bands"][2]["data"]["groups"][0]["categories"][0]["members"]:
                        person = {
                            "Role":None,
                            "Name":None
                            }
                        person["Role"] = member["role"].replace("/",",").strip()
                        person["Name"] = member["name"].strip()
                        crew.append(person)
        elif "cta" in d["bands"][5]["data"]:
            res = requests.get(self._url + d["cta"]["href"] + "/cast-and-crew")
            soup = BeautifulSoup(res.text, features="lxml")
            d = soup.find("noscript")
            d = d.json()
            if len(d["bands"]) > 2:
                if "Crew" in d["bands"][2]["band"]:
                    for member in d["bands"][2]["data"]["groups"][0]["categories"][0]["members"]:
                        person = {
                            "Role":None,
                            "Name":None
                            }
                        person["Role"] = member["role"].replace("/",",").strip()
                        person["Name"] = member["name"].strip()
                        crew.append(person)
        return crew
        
    
    
    
    
    def get_availability(self, content_metadata, is_serie, is_episode):
        if is_serie or is_episode:
            end = None
            pass
        else:
            end = str(content_metadata["availability"][0]["end"])
            end = end[0:10]
        return end
    
    
    
    
    def get_is_original(self, content_metadata):
        return content_metadata.get("streamingId").get("hboFilm") or None
    
    
    def get_packages(self, content_metadata):
        return [{'Type':'subscription-vod'}]
    
    
    
    
    def get_episode(self, content_metadata):
        return int(str(content_metadata.get("data").get("episode").get("groups").get(1).get("groupingValue")).replace("0","")) or None
    
    
    def get_episodes(self, content_metadata, parent_id, parent_title):
        for episode in content_metadata:
            if episode["band"] == "SerialEpisode":
                payload = self.get_payload(episode,parent_id=parent_id, parent_title=parent_title, is_episode=True)
                payload_episode = payload.payload_episode()
                Datamanager._checkDBandAppend(self,payload_episode,self.ids_episcrap,self.payloads_episodes,isEpi=True)
                
                
                
    def get_season(self, content_metadata):
        return content_metadata.get("data").get("episode").get("groups").get(1).get("groupingValue") or None
      
    
    
    
    def get_seasons(self, content_metadata):
        payload = Payload()
        seasons = []
        res = requests.get(self._url + content_metadata["cta"]["href"])
        soup = BeautifulSoup(res.text, "lxml")
        nosc = soup.find("noscript")
        d = json.loads(nosc["data-state"])
        try:
            if "seasonCards" in d["bands"][5]["data"]:
                for season in d["bands"][5]["data"]["seasonCards"]:
                    payload_season = payload.payload_season()
                    res = requests.get("https://www.hbo.com" + season["href"])
                    soup = BeautifulSoup(res.text, "lxml")
                    nosc = soup.find("noscript")
                    d = json.loads(nosc["data-state"])
                    count = None
                    random = ''.join([rand.choice(string.ascii_letters
                                                    + string.digits) for n in range(32)])
                    payload_season["Id"] = random
                    payload_season["Title"] = d["title"]
                    payload_season["Number"] = season["groupingValue"]
                    payload_season["Deeplink"] = season["href"]
                    for num_epi in d["bands"]:
                        if num_epi["band"] == "SerialEpisode":
                            count += 1
                    payload_season["Episodes"] = count
                    seasons.append(payload_season)
                    self.get_episodes(d["bands"], parent_title = d["title"], parent_id = random)
        except IndexError:
            #Si la serie no 
            pass       
    
    
    
    

    def scraping(self):
        self.get_movies()
        self.get_series()
        # Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        # Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        
