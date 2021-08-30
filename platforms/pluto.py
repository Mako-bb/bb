# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
import re
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload

class Pluto():
    
    """
    - Status: EN PROCESO
    - VPN: NO
    - Método: API
    - Runtime: 8 MINUTOS
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
        self._url                   = config_['url']
        self._headers               = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://pluto.tv/en/on-demand',
                'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsImtpZCI6IjM2MTkwOWQzLWI1NjUtNGI0ZS04YTQ5LTBhNTgzMGU4Mjk1ZSIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uSUQiOiI2NTA5MTgzZS0wNjlmLTExZWMtOGQ1ZC0wMjQyYWMxMTAwMDMiLCJjbGllbnRJUCI6IjE4Ni4yMi4yMzguMTEiLCJjaXR5IjoiTG9tYXMgZGUgWmFtb3JhIiwicG9zdGFsQ29kZSI6IjE4MzIiLCJjb3VudHJ5IjoiQVIiLCJkbWEiOjAsImFjdGl2ZVJlZ2lvbiI6IlZFIiwiZGV2aWNlTGF0IjotMzQuNzY2MSwiZGV2aWNlTG9uIjotNTguMzk1NywicHJlZmVycmVkTGFuZ3VhZ2UiOiJlcyIsImRldmljZVR5cGUiOiJ3ZWIiLCJkZXZpY2VWZXJzaW9uIjoiOTIuMC45MDIiLCJkZXZpY2VNYWtlIjoiZWRnZS1jaHJvbWl1bSIsImRldmljZU1vZGVsIjoid2ViIiwiYXBwTmFtZSI6IndlYiIsImFwcFZlcnNpb24iOiI1LjEwMy4wLTE5YzEwNWFkMzJjNzY5OGY5MjcxOTY1MzUwMWI2ZTQ4NmY2ODdmOWIiLCJjbGllbnRJRCI6ImQ5NzA5MTg2LTcyZDYtNDhkZC1hZDliLTA0YWI3MzEwM2FjNSIsImNtQXVkaWVuY2VJRCI6IiIsImlzQ2xpZW50RE5UIjpmYWxzZSwidXNlcklEIjoiIiwibG9nTGV2ZWwiOiJERUZBVUxUIiwidGltZVpvbmUiOiJBbWVyaWNhL0FyZ2VudGluYS9CdWVub3NfQWlyZXMiLCJzZXJ2ZXJTaWRlQWRzIjp0cnVlLCJlMmVCZWFjb25zIjpmYWxzZSwiZmVhdHVyZXMiOnt9LCJhdWQiOiIqLnBsdXRvLnR2IiwiZXhwIjoxNjMwMDMzMDIwLCJqdGkiOiI0NTMwZjU1NS1jYTI1LTQ4MTYtODgxNy0xNjNkNjQzODZhNWUiLCJpYXQiOjE2MzAwMDQyMjAsImlzcyI6ImJvb3QucGx1dG8udHYiLCJzdWIiOiJwcmk6djE6cGx1dG86ZGV2aWNlczpWRTpaRGszTURreE9EWXROekprTmkwME9HUmtMV0ZrT1dJdE1EUmhZamN6TVRBellXTTEifQ.zjLx9tCKbt94S9wzCrTZqBJSKUgD6ThZl92LkQC8Kdw',
                'Origin': 'https://pluto.tv',
                'Connection': 'keep-alive',
                'TE': 'Trailers'
                }
        self.payloads = []
        self.payloads_episodes = []
        self.ids_scrapeados = Datamanager._getListDB(self,self.titanScraping)
        self.ids_episcrap = Datamanager._getListDB(self,self.titanScrapingEpisodios)

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

    def get_categories(self):
        """Obtiene todas las categorias"""
        """
        Returns:
            cat_: list
        """
        res = requests.get(self._url + "categories?includeCategoryFields=iconSvg", headers=self._headers)
        cat_ = res.json()
        return cat_['categories']

    def get_movies(self, categories):
        """Obtiene todas las peliculas

        Args:
            categories (list): lista de categorias
        """
        cat_ = categories
        for category in cat_:
            res = requests.get(self._url + "categories/" + category['_id'] + '/items', headers=self._headers)
            res = res.json()
            for movie in res['items']:
                if movie['type'] == "movie":
                    payload = self.get_payload(movie)
                    payload_movie = payload.payload_movie()
                    Datamanager._checkDBandAppend(self,payload_movie,self.ids_scrapeados,self.payloads)

    def get_series(self, categories):
        """Obtiene todas las series/" +

        Args:
            categories (list): lista de categorias
        """
        cat_ = categories
        for category in cat_:
            res = requests.get(self._url + "categories/" + category['_id'] + '/items', headers=self._headers)
            res = res.json()
            for serie in res['items']:
                if serie['type'] == "series":
                    payload = self.get_payload(serie, is_serie=True)
                    payload_serie = payload.payload_serie()
                    Datamanager._checkDBandAppend(self,payload_serie,self.ids_scrapeados,self.payloads)



    def get_payload(self,content_metadata, parent_id=None, parent_name=None,is_episode=None, is_serie=None):
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
            payload.clean_title = self.get_clean_title(content_metadata)
            payload.deeplink_web = self.get_deeplinks(content_metadata)
            if is_episode:
                payload.parent_title = parent_name
                payload.parent_id = parent_id
                payload.season = self.get_season(content_metadata)
                payload.episode = self.get_episode(content_metadata)
            if is_serie:
                payload.seasons = self.get_seasons(content_metadata)
            payload.year = self.get_year(content_metadata)
            payload.duration = self.get_duration(content_metadata)
            payload.synopsis = self.get_synopsis(content_metadata)
            payload.image = self.get_images(content_metadata)
            payload.rating = self.get_ratings(content_metadata)
            payload.genres = self.get_genres(content_metadata)
            payload.packages = self.get_packages(content_metadata)
            payload.createdAt = self._created_at
            return payload

    def get_id(self, content_metadata):
        # Obtiene el ID desde el content_metadata
        return content_metadata.get("_id") or None
    
    def get_title(self, content_metadata):
        # Obtiene el titulo desde el content_metadata
        return content_metadata.get("name") or None
    
    def get_clean_title(self, content_metadata):
        # Remplaza el título del content_metadata para generar uno limpio
        return _replace(content_metadata.get("name")) or None
    
    def get_year(self, content_metadata):
        # Obtiene el año desde el content_metadata["slug"], si matchean 4 digitos entre dos no caracteres lo devuelve
        regex = re.compile(r'\W(\d{4})\W')
        if regex.search(content_metadata.get("slug")):
            year = regex.search(content_metadata.get("slug")).group(1)
        else:
            year = None
        return year
    def get_duration(self, content_metadata):
        # Obtiene la duracion desde el content_metadata y hace el calculo para pasarlo a minutos
        if content_metadata['type'] == "series":
            duration = None
        else:
            duration = content_metadata['duration'] // 1000
            duration = duration // 60
        return duration

    def get_deeplinks(self, content_metadata):
        deeplink = None
        if content_metadata['type'] == "movie":
           deeplink = "https://pluto.tv/es/on-demand/movies/" + content_metadata["slug"]
        else:
           deeplink = "https://pluto.tv/es/on-demand/series/" + content_metadata["slug"]
        return deeplink
    
    def get_synopsis(self, content_metadata):
        return content_metadata.get("description") or None
    def get_images(self, content_metadata):
        images = []
        for image in content_metadata.get("covers"):
            images.append(image['url'])
        return images
    def get_ratings(self, content_metadata):
        rating = content_metadata['rating']
        return content_metadata.get("rating")
    def get_genres(self, content_metadata):
        genres = None
        if "genre" in content_metadata:
            genres = content_metadata['genre'].replace(" & ", ",")
            genres = genres.replace(" Y ", ",")
            genres = genres.replace("/", ",").split(",")
        return genres
    def get_packages(self, content_metadata):
        return [{'Type':'free-vod'}]
    def get_episode(self, content_metadata):
        return content_metadata.get('number') or None
    
    
    def get_episodes(self, content_metadata):
        parent_name = content_metadata['name']
        parent_id = content_metadata['_id']
        for i in content_metadata['seasons']:
            for episode in i['episodes']:
                payload = self.get_payload(episode,parent_id=parent_id, parent_name=parent_name, is_episode=True)
                payload_episode = payload.payload_episode()
                Datamanager._checkDBandAppend(self,payload_episode,self.ids_episcrap,self.payloads_episodes,isEpi=True)

    def get_season(self, content_metadata):
        return content_metadata.get("season") or None
    def get_seasons(self, content_metadata):
        seasons = []
        payload = Payload()
        res = requests.get(self._url + "series/" + content_metadata['_id'] + "/seasons", headers=self._headers)
        res = res.json()
        slug = res['slug']
        self.get_episodes(res)
        
        for season in res['seasons']:
            payload_season = payload.payload_season()
            payload_season["Number"] = season['number']
            payload_season["Episodes"] = len(season['episodes'])
            payload_season["Deeplink"] = self._url + "series/" + slug + "/details" + "/seasons" + str(season['number'])
            seasons.append(payload_season)
        return seasons

    def scraping(self):
        cat = self.get_categories()
        self.get_movies(cat)
        self.get_series(cat)
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        
        Upload(self._platform_code,self._created_at,testing=True)