# -*- coding: utf-8 -*-
import requests # Si el script usa requests/api o requests/bs4
import time
import platform
from bs4                import BeautifulSoup # Si el script usa bs4
from selenium           import webdriver # Si el script usa selenium
from handle.datamanager import Datamanager # Opcional si el script usa Datamanager
from common             import config
from handle.mongo       import mongo
from updates.upload     import Upload
from handle.replace     import _replace
from handle.payload     import Payload
from pyvirtualdisplay   import Display
import re

class PlutoLE():

    """
    Plantilla de muestra para la definición de la clase de una plataforma nueva.

    Los imports van arriba de todo, de acuerdo a las convenciones de buenas prácticas.

    Esta es la descripción de la clase, donde se indica toda la información pertinente al script.
    Siempre debe ir, como mínimo, la siguiente información:
    - Status: Proceso
    - VPN: N/a
    - Método: Request
    - Runtime: inderteminado

    El __init__ de la clase define los atributos de la clase al instanciar un objeto de la misma.
    Los parámetros que se le pasan al momento de instanciar una clase son los que se insertan desde la terminal
    y siempre son los mismos:
    - ott_platforms: El nombre de la clase, debe coincidir con el nombre que se encuentra en el config.yaml
    - ott_site_country: El ISO code de 2 dígitos del país a scrapear. ejm: AR (Argentina), US (United States)
    - ott_operation: El tipo de operación a realizar. Cuando estamos desarrollando usamos 'testing', cuando
    se corre en el server usa 'scraping'
    Al insertar el comando en la terminal, se vería algo así:
    python main.py --o [ott_operation] --c [ott_site_country] [ott_platforms]

    Los atributos de la clase que use Datamanager siempre deben mantener el nombre, ya que Datamanager
    accede a ellos por nombre. Por ejemplo, si el script usa Datamanager, entonces self.titanScrapingEpisodios
    debe llamarse tal cual, no se le puede cambiar el nombre a self.titanScrapingEpisodes o algo así, porque
    Datamanager no lo va a reconocer y va a lanzar una excepción.
    """

    def __init__(self, ott_platforms, ott_site_country, ott_operation):
        self.test = ott_operation in ("testing", "return") #
        config_ = config()['ott_sites'][ott_platforms] # Obligatorio
        self.country = ott_site_country # Opcional, puede ser útil dependiendo de la lógica del script.
        self._created_at = time.strftime('%Y-%m-%d')
        self._platform_code = config_['countries'][ott_site_country]
        self.Token = self.getApiToken()
        self.mongo = mongo()
        self.sesion                 = requests.session() # Requerido si se va a usar Datamanager
        self.titanPreScraping       = config()['mongo']['collections']['prescraping'] # Opcional
        self.titanScraping          = config()['mongo']['collections']['scraping'] # Obligatorio. También lo usa Datamanager
        self.titanScrapingEpisodios = config()['mongo']['collections']['episode'] # Obligatorio. También lo usa Datamanager
        self.skippedTitles          = 0 # Requerido si se va a usar Datamanager
        self.skippedEpis            = 0 # Requerido si se va a usar Datamanager
        self.api_url = config_['api_url']
        self.check_id = list()

        self._headers               = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://pluto.tv/en/on-demand',
                'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsImtpZCI6IjJiYjNkYTc3LWRhMTktNGVmZC05ODJiLWVjMGI4MDNkY2JlYyIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uSUQiOiI5NjY3NzI3Mi0wZmRkLTExZWMtOGNhOS0wMjQyYWMxMTAwMDMiLCJjbGllbnRJUCI6IjE4Ni4yMi4yMzguMTEiLCJjaXR5IjoiTG9tYXMgZGUgWmFtb3JhIiwicG9zdGFsQ29kZSI6IjE4MzIiLCJjb3VudHJ5IjoiQVIiLCJkbWEiOjAsImFjdGl2ZVJlZ2lvbiI6IlZFIiwiZGV2aWNlTGF0IjotMzQuNzY2MSwiZGV2aWNlTG9uIjotNTguMzk1NywicHJlZmVycmVkTGFuZ3VhZ2UiOiJlcyIsImRldmljZVR5cGUiOiJ3ZWIiLCJkZXZpY2VWZXJzaW9uIjoiOTMuMC45NjEiLCJkZXZpY2VNYWtlIjoiZWRnZS1jaHJvbWl1bSIsImRldmljZU1vZGVsIjoid2ViIiwiYXBwTmFtZSI6IndlYiIsImFwcFZlcnNpb24iOiI1LjEwMy4xLThmYWQ5ZjU1MjNmZWY1N2RjZTA2MTc5ZGE2MDU2ODJjMDM1YWZlOTkiLCJjbGllbnRJRCI6ImQ5NzA5MTg2LTcyZDYtNDhkZC1hZDliLTA0YWI3MzEwM2FjNSIsImNtQXVkaWVuY2VJRCI6IiIsImlzQ2xpZW50RE5UIjpmYWxzZSwidXNlcklEIjoiIiwibG9nTGV2ZWwiOiJERUZBVUxUIiwidGltZVpvbmUiOiJBbWVyaWNhL0FyZ2VudGluYS9CdWVub3NfQWlyZXMiLCJzZXJ2ZXJTaWRlQWRzIjp0cnVlLCJlMmVCZWFjb25zIjpmYWxzZSwiZmVhdHVyZXMiOnt9LCJhdWQiOiIqLnBsdXRvLnR2IiwiZXhwIjoxNjMxMDQ5MjkyLCJqdGkiOiIxOThlNjViZi0wNjU5LTQ5ZjQtODY4Yy1lYWQwYWZkODAzNjAiLCJpYXQiOjE2MzEwMjA0OTIsImlzcyI6ImJvb3QucGx1dG8udHYiLCJzdWIiOiJwcmk6djE6cGx1dG86ZGV2aWNlczpWRTpaRGszTURreE9EWXROekprTmkwME9HUmtMV0ZrT1dJdE1EUmhZamN6TVRBellXTTEifQ.9R2a2xOCagQwYw0tcwxDziwx2xjkQseokeTev47bXzM',
                'Origin': 'https://pluto.tv',
                'Connection': 'keep-alive',
                'TE': 'Trailers'
                }
        self.payloads = []
        self.payloads_episodes = []
        self.ids_scrapeados = Datamanager._getListDB(self,self.titanScraping)
        self.ids_episcrap = Datamanager._getListDB(self,self.titanScrapingEpisodios)

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
        cat = self.get_cat()
        self.get_movies(cat)
        self.get_series(cat)
        Datamanager._insertIntoDB(self,self.payloads_episodes,self.titanScrapingEpisodios)
        Datamanager._insertIntoDB(self,self.payloads,self.titanScraping)
        
        Upload(self._platform_code,self._created_at,testing=True)

    def get_cat(self):
        res = requests.get(self.api_url)
        cat = res.json()
        return cat['categories']

    def get_movies(self, categorias):
        for cat in categorias:
            porCat = cat['items']
            for movies in porCat:
                if movies['type'] == "movie":
                    payload = self.get_payload_movie(movies)
                    Datamanager._checkDBandAppend(self,payload,self.ids_scrapeados,self.payloads)
                    
    def get_series(self, categorias):
        for cat in categorias:
            porCat = cat['items']
            for series in porCat:
                if series['type'] == "series":
                    payload = self.get_payload_series(series)
                    Datamanager._checkDBandAppend(self,payload,self.ids_scrapeados,self.payloads)

    def get_payload_movie(self,content_metadata=dict()):
        payload = Payload()
        payload.platform_code = self._platform_code
        payload.id = self.get_id(content_metadata)
        payload.title = self.get_title(content_metadata)
        payload.clean_title = self.get_clean_title(content_metadata)
        payload.type = self.get_type(content_metadata)
        payload.Year = self.get_year(content_metadata)
        payload.duration = self.get_duration(content_metadata)
        payload.deeplink_web = self.get_deep_link(content_metadata)
        payload.synopsis = self.get_synopsis(content_metadata)
        payload.image = self.get_image(content_metadata)
        payload.rating = self.get_rating(content_metadata)
        payload.genres = self.get_genre(content_metadata)
        payload.packages = self.get_packages(content_metadata)
        payload.createdAt = self._created_at

        return payload.payload_movie()

    def getApiToken(self):
        response = requests.request("GET", "https://boot.pluto.tv/v4/start?appName=web&appVersion=5.106.0-f3e2ac48d1dbe8189dc784777108b725b4be6be2&deviceVersion=96.0.1054&deviceModel=web&deviceMake=edge-chromium&deviceType=web&clientID=eb75c8a4-8a92-4745-8a04-4cc0322424b6&clientModelNumber=1.0.0&channelID=5dcde437229eff00091b6c30&serverSideAds=true&constraints=&clientTime=2021-11-26T16:01:26.536Z").json()
        return response["sessionToken"]

    def requestHandler(self, _id):
        querystring     = ""
        payload         = ""
        headers = {
                "authority": "service-vod.clusters.pluto.tv",
                "sec-ch-ua": "^\^",
                "Authorization": "Bearer" + self.Token
        }
        response        = ""

        querystring     = {"offset" : "1000", "page" : "1"}
        response        = requests.request("GET", "https://service-vod.clusters.pluto.tv/v4/vod/series/" + _id + "/seasons", data=payload, headers=headers, params=querystring)

        return response.json()

    def get_payload_series(self,content_metadata=dict()):
        seasons = list()
        episodes = list()
        _id = content_metadata.get("_id")
        res = self.requestHandler(_id)
        content = res
        name_parent = content.get("name")
        slug_parent = content.get("slug")
        for season in content.get("seasons", []):
            for episode in season.get("episodes", []):
                if episode.get("_id") in self.check_id:
                    continue
                payload_epi = self.build_payload_episode(episode,name_parent,_id,slug_parent)
                self.check_id.append(episode.get("_id"))
                Datamanager._checkDBandAppend(self,payload_epi,self.ids_episcrap,self.payloads_episodes,isEpi=True)

            nro_season = season.get("number")
            seasons.append(self.build_payload_season(content,nro_season))

        payload = Payload()
        payload.platform_code = self._platform_code
        payload.id = self.get_id(content_metadata)
        payload.title = self.get_title(content_metadata)
        payload.clean_title = self.get_clean_title(content_metadata)
        payload.seasons = seasons
        payload.type = self.get_type(content_metadata)
        payload.year = self.get_year(content_metadata)
        payload.duration = self.get_duration(content_metadata)
        payload.deeplink_web = self.get_deep_link(content_metadata)                 
        payload.synopsis = self.get_synopsis(content_metadata)
        payload.image = self.get_image(content_metadata)
        payload.rating = self.get_rating(content_metadata)
        payload.genres = self.get_genre(content_metadata)
        payload.packages = self.get_packages(content_metadata)
        payload.createdAt = self._created_at

        return payload.payload_serie()

    def build_payload_episode(self,episode,name_parent,id_parent,slug_parent):
        payload = Payload()
        payload.platform_code = self._platform_code
        payload.parent_id = id_parent
        payload.parent_title = name_parent
        payload.id = self.get_id(episode)
        payload.title = self.get_title(episode)
        payload.season = self.get_season(episode) 
        payload.episode = self.get_number(episode)
        payload.clean_title = self.get_clean_title(episode)
        payload.type = self.get_type(episode)
        payload.year = self.get_year(episode)
        payload.duration = self.get_duration(episode)
        payload.deeplink_web = self.get_deep_link_episode(episode,slug_parent)
        payload.synopsis = self.get_synopsis(episode)
        payload.image = self.get_image(episode)
        payload.rating = self.get_rating(episode)
        payload.genres = self.get_genre(episode)
        payload.packages = self.get_packages(episode)
        payload.createdAt = self._created_at
        return payload.payload_episode()

    def build_payload_season(self,season,nro_season):
        payload = Payload()
        payload.platform_code = self._platform_code
        payload.id = self.get_id(season)
        payload.title = self.get_title(season)
        payload.number = nro_season
        payload.clean_title = self.get_clean_title(season)
        payload.type = self.get_type(season)
        payload.deeplink_web = self.get_deep_link_season(season,nro_season)
        payload.synopsis = self.get_synopsis(season)
        payload.image = self.get_image_season(season)
        payload.rating = self.get_rating(season)
        payload.genres = self.get_genre(season)
        payload.packages = self.get_packages(season)
        payload.createdAt = self._created_at
        return payload.payload_season()

    def get_id(self,metadata):
        return metadata.get("_id") or None
    
    def get_title(self, metadata):
        return metadata.get("name") or None

    def get_clean_title(self, metadata):
        return _replace(metadata.get("name")) or None
     
    def get_type(self, metadata):
        return metadata.get("type") or None

    def get_year(self, metadata):
        regex = re.compile(r'\W(\d{4})\W')
        year = None
        if regex.search(metadata.get("slug")):
            year = regex.search(metadata.get("slug")).group(1)
        return year

    def get_duration(self, metadata):
        if metadata.get("type") == "movie":
            duration = metadata.get("allotment")
            duration = duration // 60
        elif(metadata.get("type") == "episode"):
            duration = metadata.get("allotment")
            duration = duration // 60
        else:
            duration = None
        return duration

    def get_deep_link(self, metadata):
        slug = metadata.get("slug")
        print(slug)
        if metadata.get("type") == "movie":
            deep_link = "https://pluto.tv/es/on-demand/movies/" + str(slug)
        else:
            deep_link = "https://pluto.tv/es/on-demand/series/" + str(slug)
        return deep_link
    
    def get_deep_link_episode(self, metadata,slug_parent):
        slug = metadata.get("slug")
        nro_episode = str(metadata.get("number"))
        deep_link = "https://pluto.tv/es/on-demand/series/" + str(slug_parent) + "/season/" + str(nro_episode) + "/episode/" + str(slug)
        return deep_link

    def get_deep_link_season(self,metadata,nro_season):
        slug = metadata.get("slug")
        deep_link = "https://pluto.tv/es/on-demand/series/" + str(slug) + "/details/season/" + str(nro_season)
        return deep_link

    def get_synopsis(self, metadata):
        return metadata.get("summary") or None

    def get_image(self, metadata):
        images = list()
        urls_imagenes = metadata["covers"]
        for imagenes in urls_imagenes:
            images.append(imagenes.get("url"))
        return images

    def get_image_season(self, metadata):
        imagen = metadata["featuredImage"]
        imagen = imagen.get("path")
        return imagen

    def get_rating(self, metadata):
        return metadata.get("rating") or None

    def get_genre(self, metadata):
        genres = None
        if "genre" in metadata:
            genres = metadata['genre'].replace(" & ", ",")
            genres = genres.replace(" y ", ",")
            genres = genres.replace(" Y ", ",")
            genres = genres.replace("/", ",").split(",")
        return genres

    def get_packages(self, metadata):
        return [{'Type' : 'free-vod'}]

    def get_number(self,metadata):
        return metadata.get("number") or None

    def get_season(self,metadata):
        return metadata.get("season") or None
